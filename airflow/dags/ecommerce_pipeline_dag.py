from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'de',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: ingest -> bronze -> silver -> gold -> load',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. Ingest - uses Python script that downloads CSVs to MinIO via boto3
    ingest = BashOperator(
        task_id='ingest_data',
        bash_command='python3 /opt/spark_jobs/ingestion/ingest_download.py'
    )

    # 2. Bronze -> Silver (PySpark)
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='spark-submit --master local[*] /opt/spark_jobs/bronze_to_silver/bronze_to_silver_orders.py'
    )

    # 3. Silver -> Gold
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='spark-submit --master local[*] /opt/spark_jobs/silver_to_gold/silver_to_gold_model.py'
    )

    # 4. Load Gold -> Postgres Warehouse
    load_to_pg = BashOperator(
        task_id='load_to_postgres',
        bash_command='python3 /opt/spark_jobs/silver_to_gold/load_gold_to_pg.py'
    )

    # 5. Notify (simple echo, can be replaced with email/Teams)
    notify = BashOperator(
        task_id='notify_success',
        bash_command='echo "Pipeline completed successfully at $(date)"'
    )

    ingest >> bronze_to_silver >> silver_to_gold >> load_to_pg >> notify
