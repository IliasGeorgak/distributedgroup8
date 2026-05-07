import json
import tempfile
import time
from typing import Any

from kubernetes import client, config, utils
from jinja2 import Template
import os

class Kuber:
    def __init__(self, namespace: str = "default"):
        try:
            config.load_kube_config() #load local cluster configuration
        except:
            config.load_incluster_config() #same but for actual clusters
        self.namespace = namespace
        self.api_client = client.ApiClient()
        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()
        
    def apply_worker(self, path):
        utils.create_from_yaml(self.k8s_client, path)
        os.remove(path)

    def create_worker(self, rendered_yaml: str) -> str:

        try:
            objs = utils.create_from_yaml(
                self.api_client, yaml_content=rendered_yaml, namespace=self.namespace
            )
            job = objs[0]
            job_name = job.metadata.name
            print(f"Created job: {job_name}")
            return job_name
        except utils.FailToCreateError as e:
            print(f"Failed to create job: {e}")

    def render_worker_yaml(self, worker_id: str, image: str, task_metadata: dict[str, Any]) -> str:
        # Load job template
        with open("sample_job.yaml") as f:
            template = Template(f.read())

        args = self.build_args(task_metadata, worker_id)

        # Render YAML with dynamic values
        rendered_yaml = template.render(
            worker_id=worker_id,
            image=image,
            args=args
        )
        return rendered_yaml

    def build_args(self, task_metadata: dict[str, Any], worker_id: str) -> list[str]:
        task_json = json.dumps(task_metadata)

        return [
            "--task-json",
            task_json,
            "--worker-id",
            worker_id,
        ]
    
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

    def check_task_success(self, job_name: str,task_metadata: dict[str, Any]) -> bool:
        logs = self.get_logs(job_name)
        # return task_metadata[""] in logs
        return True

if __name__ == "__main__":
    kuber = Kuber()
    worker_id = "worker1"
    image = "python:3.9-slim"
    command = "echo Hello from worker1"
    yaml_path = kuber.create_worker(worker_id, image, command)
    kuber.apply_worker(yaml_path)