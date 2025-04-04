from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import config

TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
CHAT_ID = config.CHAT_ID

def get_latest_logs_timestamp():
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        query = "SELECT MAX(local_time) FROM fact_logs"
        latest_timestamp = pg_hook.get_first(query)[0]
        if latest_timestamp:
            return latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')  # Giá trị mặc định
    except Exception as e:
        raise ValueError(f"Không thể lấy timestamp từ fact_logs: {str(e)}")

def check_new_logs(**kwargs):
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        latest_timestamp = kwargs['ti'].xcom_pull(task_ids='get_latest_logs_timestamp')
        query = "SELECT COUNT(*) FROM fact_logs WHERE local_time > %s"
        result = pg_hook.get_first(query, parameters=(latest_timestamp,))[0]
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if result > 0:
            print(f"[{current_time}] fact_logs: Đã tìm thấy {result} bản ghi mới hơn {latest_timestamp}")
        else:
            print(f"[{current_time}] fact_logs: Không có dữ liệu mới hơn {latest_timestamp}")
    except Exception as e:
        raise ValueError(f"Lỗi khi kiểm tra dữ liệu mới: {str(e)}")

def send_telegram_message(**kwargs):
    ti = kwargs['ti']
    task_id = ti.task_id
    dag_id = ti.dag_id
    error_msg = f"Task {task_id} trong DAG {dag_id} đã thất bại!"
    return TelegramOperator(
        task_id='send_telegram_alert',
        telegram_token=TELEGRAM_BOT_TOKEN,
        chat_id=CHAT_ID,
        text=error_msg,
        dag=dag,
    ).execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'check_new_fact_logs',
    default_args=default_args,
    description='DAG kiểm tra dữ liệu mới trong fact_logs',
    schedule_interval=timedelta(minutes=2),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    get_logs_timestamp = PythonOperator(
        task_id='get_latest_logs_timestamp',
        python_callable=get_latest_logs_timestamp,
    )

    check_new_logs_task = PythonOperator(
        task_id='check_new_fact_logs',
        python_callable=check_new_logs,
    )

    send_telegram_alert = PythonOperator(
        task_id='send_telegram_alert',
        python_callable=send_telegram_message,
        provide_context=True,
        trigger_rule='one_failed',
    )

    get_logs_timestamp >> check_new_logs_task >> send_telegram_alert