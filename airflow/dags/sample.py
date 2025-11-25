from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id='test_snowflake_connection',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_query = SnowflakeOperator(
        task_id='run_test_query',
        snowflake_conn_id='snowflake_conn', # Replace with your connection ID
        sql='SELECT 1;',
    )