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
    "incremental_load_iot",
    default_args=default_args,
    description="Инкрементальная загрузка данных",
    schedule="3 3 * * *",
    start_date=dt(2026, 2, 1),
    catchup=False,
    tags=["postgres", "IOT"]
) as dag:
    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema ",
        conn_id="postgres_my_data",
        autocommit=True,
        retries=0,
        sql="""
            create schema if not exists loaded_data;

            create table if not exists 
                loaded_data.incremental_load as 
                table public.iot_clean 
                with no data;

            ALTER TABLE loaded_data.incremental_load
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
        """
    )

    incremental_load_iot = SQLExecuteQueryOperator(
        task_id="incremental_load_iot",
        conn_id="postgres_my_data",
        autocommit=True,
        sql="""        
            insert into loaded_data.incremental_load 
                select p.id, p."room_id/id", p.noted_date, p.temp, p."out/in", NOW() AS updated_at FROM public.iot_clean p
                where noted_date < (CURRENT_DATE - INTERVAL '7 year 6 months 0 days') 
                    AND NOT EXISTS (
                        SELECT 1 FROM loaded_data.incremental_load l 
                        WHERE p.id=l.id
                    );
          """, #инкремент - noted_date. Эмулирую  приход новых данных из таблицы public.iot_clean через CURRENT_DATE - INTERVAL '7 year 6 months 0 days'
    )#каждый день CURRENT_DATE меняется => загружаются новые данные 
    #для добавил столбик updated_at(когда данные обновились)

init_schema >> incremental_load_iot
