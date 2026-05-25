start:
	minikube start

build: start
	eval $$(minikube docker-env) && \
	docker build -t auth ./Auth_Service && \
	docker build -t mngr -f manager-service/Dockerfile . && \
	docker build -t worker -f worker/Dockerfile . && \
	docker build -t ui ./UI_Service

deploy:
	kubectl apply -f kubernetes/

restart:
	kubectl rollout restart deployment auth
	kubectl rollout restart deployment manager
	kubectl rollout restart deployment ui

forward:
	kubectl port-forward svc/auth-service 8080:8080 & \
	kubectl port-forward svc/manager-service 8000:8000 & \
	kubectl port-forward svc/ui-service 8081:8081

db:
	kubectl cp postgres/jobs-db.sql postgres-0:/tmp/jobs-db.sql
	kubectl exec -it postgres-0 -- psql -U postgres -f /tmp/jobs-db.sql

logs:
	kubectl logs deployment/manager

clean:
	kubectl delete jobs --all
	kubectl delete pods --all
