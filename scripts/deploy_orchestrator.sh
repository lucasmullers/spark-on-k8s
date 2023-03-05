kubectl create namespace orchestrator
kubectl create secret generic airflow-ssh-secret --from-file=gitSshKey=/home/lucasmuller/.ssh/id_ed25519 -n orchestrator
helm install airflow -f kubernetes/airflow/values.yaml  kubernetes/airflow -n orchestrator --create-namespace --debug
kubectl apply -f kubernetes/airflow/config-permission.yaml