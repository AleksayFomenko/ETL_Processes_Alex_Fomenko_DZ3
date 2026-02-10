from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime as dt, timedelta

default_args = {
    "owner": "Alex",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "full_load_iot",
    default_args=default_args,
    description="Полная прогрузка исторических данных",
    schedule="3 3 * * *",
    start_date=dt(2026, 2, 1),
    catchup=False,
    tags=["postgres", "IOT"]
) as dag:
    full_load_iot = SQLExecuteQueryOperator(
        task_id="full_load_iot",
        conn_id="postgres_my_data",
        autocommit=True,
        sql="""
            create schema if not exists loaded_data;

            create table if not exists 
                loaded_data.full_load as 
                table public.iot_clean 
                with no data;

            truncate table 
                loaded_data.full_load;

            insert into loaded_data.full_load select * from public.iot_clean;
          """,
    )

full_load_iot