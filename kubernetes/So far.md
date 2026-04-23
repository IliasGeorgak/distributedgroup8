## Minikube
#### To start testing run:
```bash
minikube start
```
#### AFTER starting the docker engine locally
## Image
#### This version uses a local docker image. All the scripts included in building said image can be found in Docker/test image/scripts.  
#### To build locally , go to Docker/ and run:
```bash
docker build . --tag localtest
minikube image load localtest
```

## YAML files
#### Postgres is implemented as a stateful set using the default docker postgres image while the worker is placeholder verion using the aforementioned locally built image to test deployment and functionality. Note that both the worker deployment as well as the postgres stateful set reference the postgres-config and postgres-secret files to define environment variables to set-up and connect to the db. While we are still using a local image in order to properly pull it you need to edit line 19 in manager-deployment.yaml:
```yaml
image: localtest
imagePullPolicy: Never
```

## Running/Testing 
#### To verify the functionallity of what I have implemented so far first apply all yaml files that have to do with the stateful-set, check that the postgres pod has been succefully created and it is running(with get pods) AND THEN apply the manager deployment.
```bash
kubectl apply -f postgres-config.yaml,postgres-secret.yaml,postgres-service.yaml,postgres.yaml
kubectl get pods 
kubectl apply -f manager-deployment.yaml
```
## Testing 
#### To test this early cluster you will have to check each pods logs manually
```bash
kubectl get pods #To get the names
kubectl logs <pod-name>
```

## Issues to be resolved
#### 1) Providing and applying the db schema at first initialization
#### Proposed solutions: 
#####   -Manually doing through the terminal
#####   -Using a bash script
#####   -Through the manager after a relevant check(Do the proper tables exist etc.)
#####   -Using a custom docker image here as well
#### 2) Handling scaling past one postgres pod. We need to identify the original and periodically update the copies so that data is always up-to-date while making sure that only the original can be written to at a time. Also need to implement a mechanism to make sure that when new data is addded it is propagated to all copies.
#### Proposed solutions: 
#####   -Using the built-in k8 functinalities of stateful sets to identify copies
#####   -Using a bash script to dump/restore the db using postgres tools(pg_dump/pg_restore)
#####   -Using the manager to trigger updates to all copies once new data is inserted to the original

#### 3) Forwarding all these functionalities so that our application is accessible outside the cluster
#### Proposed solutions: 
#####   -This should be straight-forward I just havent checked it out yet

## Additional notes
### To get a terminal to a pod use:
```bash
kubectl exec --stdin --tty <pod-name> -- /bin/bash
```
### To delete a pod:
```bash
kubectl delete pod <pod-name>
```




