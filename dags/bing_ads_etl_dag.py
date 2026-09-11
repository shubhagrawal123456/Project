from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from bing_ads_etl.extract import extract
from bing_ads_etl.load import apply_schema, load
from bing_ads_etl.transform import transform
from bing_ads_etl.validate import validate


def _extract(**_context) -> str:
    return str(extract())


def _validate(**_context) -> str:
    return str(validate())


def _transform(**_context) -> str:
    return str(transform())


def _apply_schema(**_context) -> None:
    apply_schema()


def _load(**_context) -> dict:
    return load()


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="bing_ads_performance_etl",
    description="Parse Bing Ads multi-day reports into a MySQL star schema.",
    start_date=datetime(2026, 1, 11),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["bing-ads", "warehouse", "mvp"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=_extract)
    validate_task = PythonOperator(task_id="validate", python_callable=_validate)
    transform_task = PythonOperator(task_id="transform", python_callable=_transform)
    schema_task = PythonOperator(task_id="apply_schema", python_callable=_apply_schema)
    load_task = PythonOperator(task_id="load", python_callable=_load)

    extract_task >> validate_task >> transform_task >> schema_task >> load_task
