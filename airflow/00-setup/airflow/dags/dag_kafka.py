from airflow import DAG
from airflow.utils.dates import days_ago
from Kafka_Healthcheck_Operator import KafkaHealthCheckOperator, KafkaDataFlowMonitoringOperator
from airflow.hooks.base import BaseHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
import json
import config


# Cấu hình Kafka từ Connection
KAFKA_BROKER = extra["bootstrap.servers"]
SASL_USERNAME = extra.get("sasl.username")
SASL_PASSWORD = extra.get("sasl.password")
TOPIC = "processed_data"
CONSUMER_GROUP = "test-group"

if not all([KAFKA_BROKER, SASL_USERNAME, SASL_PASSWORD]):
    raise ValueError("Missing Kafka connection details")

# Khởi tạo DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "on_failure_callback": lambda context: send_telegram_alert(context),
}

dag = DAG(
    "kafka_healthcheck_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Chạy mỗi 5 phút
    catchup=False,
)


# Hàm gửi cảnh báo lỗi qua Telegram
def send_telegram_alert(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    error_message = f"Task {task_id} trong DAG {dag_id} đã thất bại. Hãy kiểm tra log để biết chi tiết."

    telegram_hook = TelegramHook(telegram_conn_id='telegram_bot')
    telegram_hook.send_message({
        'chat_id': config.CHAT_ID,  # Thay bằng chat ID thực tế
        'text': error_message
    })


# Task kiểm tra Kafka
kafka_health_check = KafkaHealthCheckOperator(
    task_id="check_kafka_health",
    kafka_servers=KAFKA_BROKER,
    topic=TOPIC,
    consumer_group=CONSUMER_GROUP,
    sasl_username=SASL_USERNAME,
    sasl_password=SASL_PASSWORD,
    dag=dag,
)

# Task kiểm tra Data Flow
kafka_data_flow_monitoring = KafkaDataFlowMonitoringOperator(
    task_id="monitor_kafka_data_flow",
    kafka_servers=KAFKA_BROKER,
    topic=TOPIC,
    consumer_group=CONSUMER_GROUP,
    sasl_username=SASL_USERNAME,
    sasl_password=SASL_PASSWORD,
    threshold_lag=100,
    dag=dag,
)

# Xác định thứ tự chạy
kafka_health_check >> kafka_data_flow_monitoring