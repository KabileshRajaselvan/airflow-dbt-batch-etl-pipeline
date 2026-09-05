"""Batch ETL DAG: extract (parallel) -> combine+load raw -> dbt run -> dbt
test -> data quality gate -> publish marts.

Deviates from the PRD's own sketch in two documented ways (see README):
`catchup=False` (the PRD's `catchup=True` from a fixed `days_ago(1)` start
date would backfill indefinitely with no bound, which is almost never what
you actually want without also setting an explicit `end_date`), and
`load_warehouse` is replaced by `publish_marts` (dbt already loads the
warehouse; seepublish_marts.py for why a second unconditional load would be
redundant).
"""
import os
import sys
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/extraction_src")
sys.path.insert(0, "/opt/airflow/quality_src")
sys.path.insert(0, "/opt/airflow/dags")

STAGING_DIR = os.environ.get("ETL_STAGING_DIR", "/opt/airflow/staging")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
# dbt lives in its own venv (see dags/Dockerfile) - its dependency on
# sqlparse>=0.5 conflicts with Airflow 2.8.4's own constraint of
# sqlparse==0.4.4, so it can't share Airflow's Python environment.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _staging_path(run_date: str, name: str) -> str:
    day_dir = os.path.join(STAGING_DIR, run_date)
    os.makedirs(day_dir, exist_ok=True)
    return os.path.join(day_dir, f"{name}.parquet")


def extract_api_task(ds: str, **_) -> None:
    from extract_api import fetch_user_enrichment

    df = fetch_user_enrichment(ds)
    df.to_parquet(_staging_path(ds, "user_enrichment"))


def extract_db_task(ds: str, **_) -> None:
    from extract_db import fetch_orders

    df = fetch_orders(ds)
    df.to_parquet(_staging_path(ds, "orders"))


def extract_files_task(ds: str, **_) -> None:
    from extract_files import fetch_product_events

    df = fetch_product_events(ds)
    df.to_parquet(_staging_path(ds, "product_events"))


def combine_and_load_task(ds: str, **_) -> None:
    from combine import combine_and_load

    user_enrichment = pd.read_parquet(_staging_path(ds, "user_enrichment"))
    orders = pd.read_parquet(_staging_path(ds, "orders"))
    product_events = pd.read_parquet(_staging_path(ds, "product_events"))

    counts = combine_and_load(user_enrichment, orders, product_events, run_date=ds)
    print(f"combine_and_load counts for {ds}: {counts}")


def data_quality_checks_task(ds: str, **_) -> None:
    from run_quality_checks import run_all_checks

    results = run_all_checks(ds)
    print(f"data quality results for {ds}: {results}")


def publish_marts_task(**_) -> None:
    from publish_marts import publish_marts

    published = publish_marts()
    print(f"published marts: {published}")


with DAG(
    "etl_pipeline",
    default_args=default_args,
    description="Batch ETL: mock API + source Postgres + MinIO -> dbt bronze/silver/gold -> marts",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "dbt", "airflow-dbt-batch-etl-pipeline"],
) as dag:

    extract_api = PythonOperator(task_id="extract_api", python_callable=extract_api_task)
    extract_db = PythonOperator(task_id="extract_db", python_callable=extract_db_task)
    extract_files = PythonOperator(task_id="extract_files", python_callable=extract_files_task)

    combine_and_load_raw = PythonOperator(
        task_id="combine_and_load_raw", python_callable=combine_and_load_task
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks", python_callable=data_quality_checks_task
    )

    publish_marts_op = PythonOperator(task_id="publish_marts", python_callable=publish_marts_task)

    [extract_api, extract_db, extract_files] >> combine_and_load_raw
    combine_and_load_raw >> dbt_run >> dbt_test >> data_quality_checks >> publish_marts_op
