kubectl create namespace processing
helm install spark-operator -f kubernetes/spark-operator/values.yaml kubernetes/spark-operator --namespace spark-operator --create-namespace --debug
kubectl apply -f kubernetes/spark-operator/aws-secrets.yaml -n processing