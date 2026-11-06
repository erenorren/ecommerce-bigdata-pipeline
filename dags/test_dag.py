from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_airflow_setup",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["percobaan"],
) as dag:
    
    mulai = EmptyOperator(task_id="mulai")
    selesai = EmptyOperator(task_id="selesai")

    mulai >> selesai