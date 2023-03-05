# spark-on-k8s

```bash
# Deploy Spark Operator to the cluster
$ kubectl create namespace processing
$ helm install spark-operator -f kubernetes/spark-operator/values.yaml kubernetes/spark-operator --namespace spark-operator --create-namespace --debug
$ kubectl apply -f kubernetes/aws-secrets.yaml -n processing

# Deploy airflow with gitSync
$ kubectl create namespace orchestrator
$ kubectl create secret generic airflow-ssh-secret --from-file=gitSshKey=/home/lucasmuller/.ssh/id_ed25519 -n orchestrator
$ helm install airflow -f kubernetes/airflow/values.yaml  kubernetes/airflow -n orchestrator --create-namespace --debug
$ kubectl apply -f kubernetes/airflow/config-permission.yaml

# Deploy Prometheus to monitor the cluster
$ kubectl create namespace monitoring
$ helm install prometheus -f kubernetes/prometheus/values.yaml  kubernetes/prometheus -n monitoring --create-namespace --debug
```

Configure S3 credentials:
https://docs.containerplatform.hpe.com/54/reference/kubernetes-applications/spark/Accessing_Data_on_Amazon_S3_Using_Spark_Operator.html
