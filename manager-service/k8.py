import json
import re
import time
from pathlib import Path
from dataclasses import dataclass
import yaml
from kubernetes import client, config
from jinja2 import Environment, FileSystemLoader
from master import TaskRecord

@dataclass(slots=True)
class KubernetesTaskJob:
    task: TaskRecord
    job_name: str

class Kuber:
    def __init__(self, namespace: str = "default",image: str = "worker:latest") -> None:
        service_account_token = Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.namespace = namespace
        self.image = image
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
        worker_id = self.sanitize_job_name(task_metadata.worker_id or task_metadata.task_id)
        args = self.build_args(task_metadata, worker_id)

        rendered_yaml = template.render(
            worker_id=worker_id,
            image=image,
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
    
    def wait_for_job_completion(self, job_name: str) -> bool:
        while True:
            job = self.batch.read_namespaced_job(job_name, self.namespace)

            if job.status.succeeded:
                return True

            if job.status.failed:
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

    # def exec(self,task_metadata: TaskRecord) -> bool:
    #     rendered_yaml = self.render_worker_yaml(task_metadata)
    #     job_name = self.create_worker(rendered_yaml)
    #     return self.wait_for_job_completion(job_name)
    
    def submit_task(self, task_metadata: TaskRecord) -> KubernetesTaskJob:
        rendered_yaml = self.render_worker_yaml(task_metadata)
        job_name = self.create_worker(rendered_yaml)
        return KubernetesTaskJob(task=task_metadata, job_name=job_name)

    def wait_for_task(self, task_job: KubernetesTaskJob) -> bool:
        return self.wait_for_job_completion(task_job.job_name)

    def exec(self, task_metadata: TaskRecord) -> bool:
        task_job = self.submit_task(task_metadata)
        return self.wait_for_task(task_job)
    
    def wait_for_tasks(self, task_jobs: list[KubernetesTaskJob]) -> dict[str, bool]:
        results: dict[str, bool] = {}

        remaining = list(task_jobs)

        while remaining:
            still_running: list[KubernetesTaskJob] = []

            for task_job in remaining:
                job = self.batch.read_namespaced_job(
                    task_job.job_name,
                    self.namespace,
                )

                if job.status.succeeded:
                    results[task_job.task.task_id] = True
                elif job.status.failed:
                    results[task_job.task.task_id] = False
                else:
                    still_running.append(task_job)

            remaining = still_running

            if remaining:
                time.sleep(2)

        return results

if __name__ == "__main__":
    kuber = Kuber()
    worker_id = "worker1"
    image = "python:3.9-slim"
    command = "echo Hello from worker1"
    yaml_path = kuber.create_worker(worker_id, image, command)
    kuber.apply_worker(yaml_path)
