K8S_DIR ?= kubernetes
K8S_RENDERED_DIR ?= $(K8S_DIR)/.rendered

AUTH_IMAGE ?= auth
AUTH_IMAGE_PULL_POLICY ?= Never
AUTH_PORT ?= 8080
AUTH_SECRET_KEY ?= change-me

MANAGER_IMAGE ?= mngr
MANAGER_IMAGE_PULL_POLICY ?= Never
MANAGER_PORT ?= 8000
MANAGER_SERVICE_URL ?= http://manager-service:8000
MANAGER_DEFAULT_BUCKET ?= mapreduce

UI_IMAGE ?= ui
UI_IMAGE_PULL_POLICY ?= Never
UI_PORT ?= 8081

WORKER_IMAGE ?= worker:latest
WORKER_IMAGE_PULL_POLICY ?= Never

POSTGRES_IMAGE ?= db:latest
POSTGRES_IMAGE_PULL_POLICY ?= Never
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= change-me
POSTGRES_PASSWORD_BASE64 ?= change-me-base64
POSTGRES_PORT ?= 5432
POSTGRES_SERVICE_PORT ?= 5432
POSTGRES_DB ?= auth_db
POSTGRES_JOBS_DB ?= jobsdb
PGDATA ?= /var/lib/postgresql/data/pgdata

MINIO_IMAGE ?= minio/minio:latest
MINIO_ROOT_USER ?= admin
MINIO_ROOT_PASSWORD ?= change-me
MINIO_API_PORT ?= 9000
MINIO_CONSOLE_PORT ?= 9001
MINIO_BROWSER_REDIRECT_URL ?= http://localhost:9001
MINIO_SERVER_URL ?= minio-service:9000
MINIO_REGION_NAME ?= local-dev
MINIO_PROMETHEUS_AUTH_TYPE ?= public
MINIO_SECURE ?= false

MAP_TASK_JSON ?= {"task_id":"map-1","task_type":"map","input_bucket":"mapreduce","input_objects":["inputs/input.txt"],"output_bucket":"mapreduce","output_object":"results/map-1.json","parameters":{"operation":"word_count","input_format":"auto","case_sensitive":false,"r_partitions":1,"partition_function":"sha256"}}
REDUCE_TASK_JSON ?= {"task_id":"reduce-0","task_type":"reduce","input_bucket":"mapreduce","input_objects":["results/map-1-shuffle-part-0.jsonl"],"output_bucket":"mapreduce","output_object":"results/reduce-0.json","parameters":{"operation":"word_count","r_partitions":1,"reduce_partition_id":0,"partition_function":"sha256"}}

-include $(K8S_DIR)/.env
export

.PHONY: start build render deploy restart forward db logs clean clean-render

start:
	minikube start

build: start
	eval $$(minikube docker-env) && \
	docker build -t $(AUTH_IMAGE) --build-arg AUTH_PORT=$(AUTH_PORT) ./Auth_Service && \
	docker build -t $(MANAGER_IMAGE) --build-arg MANAGER_PORT=$(MANAGER_PORT) -f manager-service/Dockerfile . && \
	docker build -t $(WORKER_IMAGE) -f worker/Dockerfile . && \
	docker build -t $(UI_IMAGE) ./UI_Service && \
	docker build -t $(POSTGRES_IMAGE) ./postgres

render:
	command -v envsubst >/dev/null 2>&1 || { echo "envsubst is required to render Kubernetes manifests"; exit 1; }
	mkdir -p $(K8S_RENDERED_DIR)
	for file in $(K8S_DIR)/*.yaml; do \
		envsubst < $$file > $(K8S_RENDERED_DIR)/$$(basename $$file); \
	done

deploy: render
	kubectl apply -f $(K8S_RENDERED_DIR)/

restart:
	kubectl rollout restart deployment auth
	kubectl rollout restart deployment manager
	kubectl rollout restart deployment ui

forward:
	kubectl port-forward svc/auth-service $(AUTH_PORT):$(AUTH_PORT) & \
	kubectl port-forward svc/manager-service $(MANAGER_PORT):$(MANAGER_PORT) & \
	kubectl port-forward svc/ui-service $(UI_PORT):$(UI_PORT)

db:
	kubectl cp postgres/jobs_db.sql postgres-0:/tmp/jobs_db.sql
	kubectl exec -it postgres-0 -- psql -U $(POSTGRES_USER) -f /tmp/jobs_db.sql

logs:
	kubectl logs deployment/manager

clean:
	kubectl delete jobs --all
	kubectl delete pods --all

clean-render:
	rm -rf $(K8S_RENDERED_DIR)
