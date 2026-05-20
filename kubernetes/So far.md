## Minikube
#### To start testing run:
```bash
minikube start
```
#### AFTER starting the docker engine locally
## Image
#### This version uses a local docker image.  
#### To build locally , go to the relevant folder  and run:
```bash
docker build . --tag <image name>
minikube image load <image name>
```

## YAML files
#### Postgres is implemented as a stateful set using the default docker postgres image while the worker is placeholder verion using the aforementioned locally built image to test deployment and functionality. Note that all deployments as well as the postgres stateful set reference the postgres-config and postgres-secret files to define environment variables to set-up and connect to the db. While we are still using a local image in order to properly pull it you need to edit ui-deployment and manager-deployment.yaml:
```yaml
image: <image name>
imagePullPolicy: Never
```

## Running/Testing 
#### To verify the functionallity of what I have implemented so far first apply all yaml files that have to do with the stateful-set, check that the postgres pod has been succefully created and it is running(with get pods) AND THEN apply the manager deployment.
```bash
kubectl apply -f postgres-config.yaml,postgres.yaml
kubectl get pods 
kubectl apply -f manager-deployment.yaml
kubectl apply -f ui-deployment.yaml
```
## Testing 
#### To test this early cluster you will have to check each pods logs manually
```bash
kubectl get pods #To get the names
kubectl logs <pod-name>
```

## Issues to be resolved
#### 1) Providing and applying the db schema at first initialization:
####    Currently working on a simple bash script that will be built into the postgres image
#### 2) Handling scaling past one postgres pod. We need to identify the original and periodically update the copies so that data is always up-to-date while making sure that only the original can be written to at a time. Also need to implement a mechanism to make sure that when new data is addded it is propagated to all copies.
#### Proposed solutions: 
#####   -Using the built-in k8 jobs to periodically keep back-ups with a bash script to dump/restore the db using postgres tools(pg_dump/pg_restore)
#####   -Using the manager to trigger updates to all copies once new data is inserted to the original

## Additional notes
### To get a terminal to a pod use:
```bash
kubectl exec --stdin --tty <pod-name> -- /bin/bash
```
### To delete a pod:
```bash
kubectl delete pod <pod-name>
```
### To access the apps running on the cluster open a seperate terminal and run
```bash
minikube tunnel
```
### On the original terminal:
```bash
kubectl get svc
```
### For the external ips and ports



