from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ecommerce_medallion_pipeline',
    default_args=default_args,
    schedule='@monthly',           # Menggantikan schedule_interval untuk Airflow 2.4+ / 3.0
    catchup=True,                  # Mengaktifkan backfill 24 bulan historis
    max_active_runs=1,             # Mencegah eksekusi paralel yang bentrok
    tags=['ecommerce', 'medallion', 'spark', 'postgres']
) as dag:

    t1_ingest_staging = BashOperator(
        task_id='ingest_raw_to_staging',
        bash_command='python /opt/airflow/src/ingestion/convert_to_parquet.py',
    )

    t2_transform_silver = BashOperator(
        task_id='clean_orders_silver',
        bash_command='python /opt/airflow/src/transform/clean_orders.py',
    )

    t3_join_gold = BashOperator(
        task_id='join_all_gold',
        bash_command='python /opt/airflow/src/transform/join_all.py',
    )

    t4_load_postgres = BashOperator(
        task_id='load_to_postgres',
        bash_command='python /opt/airflow/src/load/load_to_postgres.py',
    )

    t1_ingest_staging >> t2_transform_silver >> t3_join_gold >> t4_load_postgres