import json
import os
import re
import time
from pathlib import Path

import yaml
from kubernetes import client, config
from jinja2 import Environment, FileSystemLoader
from master import TaskRecord

class Kuber:
    def __init__(self, namespace: str | None = None, image: str | None = None) -> None:
        service_account_token = Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.namespace = namespace or os.getenv("KUBERNETES_NAMESPACE", "default")
        self.image = image or os.environ["WORKER_IMAGE"]
        self.image_pull_policy = os.getenv("WORKER_IMAGE_PULL_POLICY", "IfNotPresent")
        self.api_client = client.ApiClient()
        if service_account_token.exists():
            token = service_account_token.read_text(encoding="utf-8").strip()
            self.api_client.default_headers["Authorization"] = f"Bearer {token}"
        self.batch = client.BatchV1Api(self.api_client)
        self.core = client.CoreV1Api(self.api_client)
        self.template_dir = Path(__file__).resolve().parent
        
    def create_worker(self, rendered_yaml: str) -> str:
        try:
            job_body = yaml.safe_load(rendered_yaml)
            job = self.batch.create_namespaced_job(
                namespace=self.namespace,
                body=job_body,
            )
            job_name = job.metadata.name
            print(f"Created job: {job_name}")
            return job_name
        except Exception as e:
            raise RuntimeError(f"Failed to create job: {e}") from e

    def render_worker_yaml(self, task_metadata: TaskRecord, image: str = None) -> str:
        if image is None:
            image = self.image
        env = Environment(loader=FileSystemLoader(str(self.template_dir)))
        template = env.get_template("sample_job.yaml")
        worker_id = self.worker_job_name(task_metadata.worker_id or task_metadata.task_id)
        args = self.build_args(task_metadata, worker_id)

        rendered_yaml = template.render(
            worker_id=worker_id,
            image=image,
            image_pull_policy=self.image_pull_policy,
            args=args
        )
        return rendered_yaml

    def build_args(self, task_metadata: TaskRecord, worker_id: str) -> list[str]:
        task_json = json.dumps(task_metadata.payload)

        return [
            "--task-json",
            task_json,
            "--worker-id",
            worker_id,
        ]

    @staticmethod
    def sanitize_job_name(value: str) -> str:
        name = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
        name = re.sub(r"-+", "-", name)
        return name[:63].rstrip("-") or "worker-job"

    def worker_job_name(self, value: str) -> str:
        suffix = str(int(time.time() * 1000))[-10:]
        base = self.sanitize_job_name(value)
        max_base_length = 63 - len(suffix) - 1
        return f"{base[:max_base_length].rstrip('-')}-{suffix}"
    
    def wait_for_job_completion(self, job_name: str) -> bool:
        timeout_seconds = int(os.getenv("WORKER_JOB_TIMEOUT_SECONDS", "900"))
        started_at = time.monotonic()

        while True:
            job = self.batch.read_namespaced_job(job_name, self.namespace)

            if job.status.succeeded:
                return True

            conditions = job.status.conditions or []
            for condition in conditions:
                if condition.type == "Failed" and condition.status == "True":
                    return False

            if time.monotonic() - started_at > timeout_seconds:
                return False

            time.sleep(2)


    def get_job_pod_name(self, job_name: str) -> str:
        pods = self.core.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"job-name={job_name}"
        )

        if not pods.items:
            raise Exception("No pod found for job")

        return pods.items[0].metadata.name

    def get_logs(self, job_name: str) -> str:
        pod_name = self.get_job_pod_name(job_name)
        return self.core.read_namespaced_pod_log(pod_name, self.namespace)

    def check_task_success(self, job_name: str,task_metadata: TaskRecord) -> bool:
        logs = self.get_logs(job_name)
        # return task_metadata[""] in logs
        return True

    def exec(self,task_metadata: TaskRecord) -> bool:
        rendered_yaml = self.render_worker_yaml(task_metadata)
        job_name = self.create_worker(rendered_yaml)
        return self.wait_for_job_completion(job_name)

if __name__ == "__main__":
    kuber = Kuber()
    print("Kubernetes worker launcher is configured.")
