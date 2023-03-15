```shell
$ docker build -t spark-k8s:v3.3.2 -f Dockerfile .
$ docker tag spark-k8s:v3.3.2 lucasmullers/spark-k8s:v3.3.2
$ docker push lucasmullers/spark-k8s:v3.3.2
```

```shell
$ docker build -t airflow-k8s:v2.4.1 -f Dockerfile .
$ docker tag airflow-k8s:v2.4.1 lucasmullers/airflow-k8s:v2.4.1
$ docker push lucasmullers/airflow-k8s:v2.4.1
```
