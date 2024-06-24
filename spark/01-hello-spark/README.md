Tại thư mục `spark`, chạy lệnh sau:

```shell
docker run -ti --name hello-spark --network=streaming-network -v ./:/spark unigap/spark:3.5 spark-submit /spark/01-hello-spark/hello_spark.py
```