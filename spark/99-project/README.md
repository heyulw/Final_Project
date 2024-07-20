# Yêu cầu dự án

## Overview

## Bài toán

Đầu vào:

- Kafka: Cụm `Kafka` setup ở dưới local và `topic` chứa dữ liệu về hành vi người dùng trên website đã làm trong project
  của module `Kafka`
- Spark: Cụm `Spark` cài đặt dưới local trong khóa học
- Schema của dữ liệu

Đầu ra:

- Chương trình code xử lý yêu cầu của dự án
- Dữ liệu sau xử lý được lưu trong database `Postgre`

## Mô tả

Schema của dữ liệu đầu vào:

| Tên          | Kiểu dữ liệu  | Mô tả                                                   | Ví dụ                                                                                                                                                               |
|--------------|---------------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | String        | Log id                                                  | aea4b823-c5c6-485e-8b3b-6182a7c4ecce                                                                                                                                |
| api_version  | String        | Version của api                                         | 1.0                                                                                                                                                                 | 
| collection   | String        | Loại log                                                | view_product_detail                                                                                                                                                 | 
| current_url  | String        | Url của trang web mà người dùng đang vào                | https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting |
| device_id    | String        | id của thiết bị                                         | 874db849-68a6-4e99-bcac-fb6334d0ec80                                                                                                                                |
| email        | String        | Email của người dùng                                    |                                                                                                                                                                     |
| ip           | String        | Địa chỉ ip                                              | 190.163.166.122                                                                                                                                                     |
| local_time   | String        | Thời gian log được tạo. Format dạng yyyy-MM-dd HH:mm:ss | 2024-05-28 08:31:22                                                                                                                                                 |
| option       | Array<Object> | Danh sách các option của sản phẩm                       | `[{"option_id": "328026", "option_label": "diamond"}]`                                                                                                              |
| product_id   | String        | Mã id của sản phẩm                                      | 96672                                                                                                                                                               |
| referrer_url | String        | Đường dẫn web dẫn đến link `current_url`                | https://www.google.com/                                                                                                                                             |
| store_id     | String        | Mã id của cửa hàng                                      | 85                                                                                                                                                                  |
| time_stamp   | Long          | Timestamp thời điểm bản ghi log được tạo                |                                                                                                                                                                     |
| user_agent   | String        | Thông tin của browser, thiết bị                         | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1                           |

Yêu cầu:

Tính số lượt view sản phầm trong ngày hiện tại theo các chiều sau:
giờ, `domain`, `store_id`, `referrer_url`, `product_id`

- Top 10 `product_id` có lượt view cao nhất trong ngày hiện tại
- Top 10 `domain` có lượt view cao nhất trong ngày hiện tại
- Top 5 `referrer_url` có lượt view cao nhất trong ngày hiện tại
- Với `domain` có lượt view cao nhất, lấy ra danh sách các `store_id` và lượt view tương ứng, sắp xếp theo lượt view
  giảm dần
- Dữ liệu view phân bổ theo giờ của một `product_id` bất kỳ trong ngày

## Phụ lục

**Cách chạy chương trình sử dụng thư viện ngoài thông qua virtual env**

```
docker container stop test-spark || true &&
docker container rm test-spark || true &&
docker run -ti --name test-spark \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--archives pyspark_venv.tar.gz#environment \
/spark/99-project/test.py"
```

## Link tham khảo

[Python Package Management](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html)