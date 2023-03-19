kubectl create namespace query
helm install dremio kubernetes/dremio_v2 -f kubernetes/dremio_v2/values.yaml -n query --debug
