from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime as dt, timedelta
from pathlib import Path
import psycopg2
import pandas as pd
import kagglehub

default_args = {
    "owner": "Alex",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "operate_iot",
    default_args=default_args,
    description="Обработка и загрузка датафрейма IOT-temp.csv",
    schedule="2 3 * * *",
    start_date=dt(2026, 2, 1),
    catchup=False,
    tags=["postgres", "IOT"]
)


def operate_IOT_temp():
    path = Path(kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices"))
    df = pd.read_csv(path / "IOT-temp.csv")

    top_hot_days= df.sort_values(by = "temp", ascending=False).head(5)
    top_cold_days = df.sort_values(by = "temp", ascending=True).head(5)
    df = df[df["out/in"] == "In"]
    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M").dt.date
    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[df["temp"].between(p05,p95)].reset_index(drop = True)
    pg_hook = PostgresHook(postgres_conn_id="postgres_my_data")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        top_hot_days.to_sql(
            "top_hot_days",
            conn,
            schema="public",
            if_exists="replace",
            index=False,
        )

        top_cold_days.to_sql(
            "top_cold_days",
            conn,
            schema="public",
            if_exists="replace",
            index=False,
        )

        df.to_sql(
            "iot_clean",
            conn,
            schema="public",
            if_exists="replace",
            index=False,
        )

operate_IOT = PythonOperator(
    task_id = "operate_and_load_IOT_data",
    python_callable =operate_IOT_temp,
    dag=dag
)

operate_IOT 