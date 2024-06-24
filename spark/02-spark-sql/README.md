## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/spark-sql
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/
```

**Copy file từ host vào trong container:**

```shell
docker cp 02-spark-sql/data/sample.csv spark-spark-worker-1:/data/spark-sql
```

## 2. Chạy chương trình

```shell
docker run -ti --name spark-sql \ 
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \ 
unigap/spark:3.5 spark-submit /spark/02-spark-sql/spark_sql.py
```