## 1. Tạo thư mục và copy file vào trong spark docker

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d1
docker exec -ti spark-spark-worker-1 mkdir -p /data/dataframe-join/d2
```

**Kiểm tra:**

```shell
docker exec -ti spark-spark-worker-1 ls -la /data/dataframe-join
```

**Copy file từ host vào trong container:**

```shell
for f in 05-dataframe-join/data/d1/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d1/; done
```

```shell
for f in 05-dataframe-join/data/d2/*.json; do docker cp $f spark-spark-worker-1:/data/dataframe-join/d2/; done
```

## 2. Chạy chương trình

```shell
docker container stop dataframe-join || true &&
docker container rm dataframe-join || true &&
docker run -ti --name dataframe-join \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
unigap/spark:3.5 spark-submit /spark/05-dataframe-join/dataframe_join.py
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các quốc gia, năm, số hóa đơn, số lượng sản phẩm, tổng sô tiền của từng quốc gia và
năm đó

Dữ liệu sắp xếp theo tên quốc gia và theo năm.

Ví dụ kết quả:

| Country   | Year | num_invoices | total_quantity | invoice_value      |
|-----------|------|--------------|----------------|--------------------|
| Australia | 2010 | 4            | 454            | 1005.1000000000001 |
| Australia | 2011 | 65           | 83199          | 136072.16999999998 |
| Austria   | 2010 | 2            | 3              | 257.03999999999996 |
| Austria   | 2011 | 17           | 4824           | 9897.28            |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra top 10 khách hàng có số tiền mua hàng nhiều nhất trong năm 2010

Dữ liệu sắp xếp theo số tiền giảm dần, nếu số tiền bằng nhau thì sắp xếp theo mã khách hàng tăng dần

Ví dụ kết quả:

| CustomerID | invoice_value |
|------------|---------------|
| 18102      | 27834.61      |
| 15061      | 19950.66      |
| 16029      | 13112.52      |
