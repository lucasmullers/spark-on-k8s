kubectl create namespace monitoring
helm install prometheus -f kubernetes/prometheus/values.yaml  kubernetes/prometheus -n monitoring --create-namespace --debug