# Distributed MapReduce on Kubernetes

This project is a small distributed MapReduce system built around Kubernetes Jobs. It includes authentication, a UI/API gateway, a manager service, worker containers, PostgreSQL metadata storage, and MinIO object storage.

The main demo workload is word count, and the framework also supports line count and inverted index through the same map/reduce pipeline.

## Architecture

- `Auth_Service/` provides user registration, login, JWT validation, and admin user operations.
- `UI_Service/` exposes the user-facing API and forwards authenticated job requests to the manager.
- `manager-service/` accepts jobs, stores job metadata in PostgreSQL, splits input files, creates map/reduce task metadata, launches Kubernetes worker Jobs, and tracks completion.
- `worker/` downloads input from MinIO, runs map or reduce logic, uploads intermediate shuffle files and final results.
- `postgres/` contains database schema and image setup.
- `kubernetes/` contains Kubernetes manifests rendered from environment variables.
- `cli/` contains a command-line client for auth and job submission.
- `tests/` contains unit tests for mapper, reducer, storage, master state transitions, and fault-tolerance scheduling.

## Fault Tolerance

The system has application-level and Kubernetes-level recovery:

- Worker Kubernetes Jobs use `backoffLimit` so failed worker pods can be retried by Kubernetes.
- Manager retries failed map/reduce tasks up to `MANAGER_MAX_TASK_ATTEMPTS`.
- Failed task attempts are recorded in PostgreSQL using `retry_count`.
- Manager retries failed jobs up to `MANAGER_MAX_JOB_ATTEMPTS`.
- Failed jobs can be moved back to `submitted` after `MANAGER_FAILED_JOB_RETRY_DELAY_SECONDS`.
- Stale running jobs are recovered after `MANAGER_STALE_JOB_SECONDS`.
- Worker Job names are unique per attempt to avoid retry collisions with old Kubernetes Jobs.

Important environment knobs:

```env
MANAGER_MAX_TASK_ATTEMPTS=3
MANAGER_MAX_JOB_ATTEMPTS=3
MANAGER_FAILED_JOB_RETRY_DELAY_SECONDS=5
MANAGER_STALE_JOB_SECONDS=300
WORKER_JOB_TIMEOUT_SECONDS=900
```

## Requirements

- Docker
- Minikube
- kubectl
- GNU `make`
- `envsubst`
- Python 3.11+ for local tests/CLI

On Windows, run the Makefile commands from an environment that supports Unix-style shell commands, such as Git Bash, WSL, or a compatible terminal.

## Configuration

Create a local Kubernetes env file:

```bash
cp kubernetes/.env.example kubernetes/.env
```

Edit `kubernetes/.env` if you need different image names, ports, passwords, retry limits, or MinIO/Postgres settings.

For local Minikube development, the default image pull policy is:

```env
*_IMAGE_PULL_POLICY=Never
```

That tells Kubernetes to use images built inside Minikube instead of pulling them from a registry.

## Build And Deploy

Start Minikube:

```bash
make start
```

Build all images inside Minikube:

```bash
make build
```

Render and deploy Kubernetes manifests:

```bash
make deploy
```

Initialize the jobs database schema:

```bash
make db
```

Check deployed resources:

```bash
make status
```

Watch manager logs:

```bash
make logs
```

Port-forward the services:

```bash
make forward
```

Default forwarded URLs:

- Auth service: `http://localhost:8080`
- Manager service: `http://localhost:8000`
- UI service: `http://localhost:8081`

## CLI Usage

Set the UI service URL:

```bash
export MAPREDUCE_UI_SERVICE_URL=http://localhost:8081
```

Register a user:

```bash
python cli/cli.py auth register --username demo --password demo123 --email demo@example.com
```

Login:

```bash
python cli/cli.py auth login --username demo --password demo123
```

Submit a word-count job:

```bash
python cli/cli.py jobs submit \
  --input_file demo-input.txt \
  --split_count 4 \
  --r_partitions 3 \
  --operation word_count \
  --input_format auto \
  --partition_function sha256
```

Submit an inverted-index job:

```bash
python cli/cli.py jobs submit \
  --input_file doc1.txt doc2.txt \
  --split_count 4 \
  --r_partitions 3 \
  --operation inverted_index \
  --input_format auto \
  --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-100mb.log --split_count 8 --r_partitions 4 --operation status_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-100mb.log --split_count 8 --r_partitions 4 --operation endpoint_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-100mb.log --split_count 8 --r_partitions 4 --operation method_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-100mb.log --split_count 8 --r_partitions 4 --operation error_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-500mb.log --split_count 16 --r_partitions 4 --operation status_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-500mb.log --split_count 16 --r_partitions 4 --operation endpoint_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-500mb.log --split_count 16 --r_partitions 4 --operation method_count --input_format text --partition_function sha256
```

```bash
python cli/cli.py jobs submit --input_file demo-logs-500mb.log --split_count 16 --r_partitions 4 --operation error_count --input_format text --partition_function sha256
```


The inverted-index reducer emits JSON entries in this shape:

```json
[
  ["hello", ["doc1"]],
  ["world", ["doc1", "doc2"]]
]
```

Set `index_with_frequencies=true` in task parameters to emit per-document counts instead of plain document lists.

Check job status:

```bash
python cli/cli.py jobs status --job_id 1
```

Fetch job results:

```bash
python cli/cli.py jobs results --job_id 1
```

Logout:

```bash
python cli/cli.py auth logout
```

## Demo Input

The repo includes a small `demo-input.txt`. A larger 100 MiB demo file can be generated with PowerShell:

```powershell
$path = "demo-input-100mb.txt"
$line = "the quick brown fox jumps over the lazy dog map reduce distributed systems word count`n"
$bytes = [System.Text.Encoding]::UTF8.GetBytes($line)
$target = 100MB
$stream = [System.IO.File]::Open($path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write)
try {
  $written = 0L
  while ($written + $bytes.Length -le $target) {
    $stream.Write($bytes, 0, $bytes.Length)
    $written += $bytes.Length
  }
  if ($written -lt $target) {
    $stream.Write($bytes, 0, [int]($target - $written))
  }
} finally {
  $stream.Dispose()
}
```

## Running Tests

Install dependencies:

```bash
python -m pip install -r requirements.txt
python -m pip install -r manager-service/requirements.txt
```

Run tests:

```bash
python -m pytest
```

## Useful Make Targets

```bash
make start         # start Minikube
make build         # build project images inside Minikube
make render        # render Kubernetes YAML into kubernetes/.rendered
make deploy        # apply rendered manifests
make restart       # restart auth, manager, and ui deployments
make forward       # port-forward auth, manager, and ui services
make db            # load jobs DB schema into Postgres
make logs          # show manager logs
make status        # show pods, services, and jobs
make clean         # delete Kubernetes jobs and pods
make clean-render  # remove rendered manifests
```

## Troubleshooting

If worker Jobs fail immediately, check:

```bash
kubectl get jobs
kubectl get pods
kubectl logs job/<job-name>
```

If the manager cannot create worker Jobs, check that the manager service account and RBAC were applied:

```bash
kubectl describe role manager-job-runner
kubectl describe rolebinding manager-job-runner-binding
```

If a job gets stuck, the manager should recover stale running jobs after `MANAGER_STALE_JOB_SECONDS`. If a job is marked `failed`, it should be retried until `MANAGER_MAX_JOB_ATTEMPTS` is reached.

If image pulls fail in Minikube, confirm the images were built inside Minikube and the pull policy is `Never`.
