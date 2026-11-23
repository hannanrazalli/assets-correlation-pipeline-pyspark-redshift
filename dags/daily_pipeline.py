import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from cosmos import DbtTaskGroup
from pendulum import datetime

from include.utilities.check_files import has_files
from include.utilities.cosmos_config import (
    execution_config,
    profile_config,
    project_config,
    render_config,
)
from include.utilities.slack_alerts import slack_failure_alert

default_args = {
    "owner": "Hannan",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_failure_alert,
}

BUCKET = os.getenv("BUCKET_NAME")


@dag(
    dag_id="Assets_correlation_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 11, 14),
    catchup=False,
    max_active_runs=1,
)
def daily_pipeline():

    # --- STEP 1. API INGESTIONS ---
    @task(task_id="1_FX_API_to_S3")
    def run_forex(ds=None):
        from include.ingestions.forex import daily_forex

        daily_forex(ds=ds)

    @task(task_id="1_Stocks_API_to_S3")
    def run_stocks(ds=None):
        from include.ingestions.stocks import stocks

        stocks(ds=ds)

    # --- STEP 2. PYSPARK TRANSFORMATION (RENAMED, CASTED, DEDUPLICATE) ---
    pyspark_transform = GlueJobOperator(
        task_id="2_Medallion_bronze_silver",
        job_name="Glue",
        script_args={"--ds": "{{ ds }}"},
        wait_for_completion=True,
    )

    # --- STEP 3. CHECK INGESTIONS FOLDER ---
    check_forex = ShortCircuitOperator(
        task_id="3_Check_forex_has_data",
        python_callable=has_files,
        op_kwargs={"name": "forex"},
        ignore_downstream_trigger_rules=False,
    )

    check_assets = ShortCircuitOperator(
        task_id="3_Check_assets_has_data",
        python_callable=has_files,
        op_kwargs={"name": "assets"},
        ignore_downstream_trigger_rules=False,
    )

    # --- STEP 4. COPY TO REDSHIFT ---
    load_forex = S3ToRedshiftOperator(
        task_id="4_Copy_fx_to_redshift",
        s3_bucket=BUCKET,
        s3_key="pyspark/forex/"
        "year={{ logical_date.strftime('%Y') }}/"
        "month={{ logical_date.strftime('%m') }}/"
        "day={{ logical_date.strftime('%d') }}/",
        schema="int_assets_correlation",
        table="int_forex",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    load_assets = S3ToRedshiftOperator(
        task_id="4_Copy_assets_to_redshift",
        s3_bucket=BUCKET,
        s3_key="pyspark/assets/"
        "year={{ logical_date.strftime('%Y') }}/"
        "month={{ logical_date.strftime('%m') }}/"
        "day={{ logical_date.strftime('%d') }}/",
        schema="int_assets_correlation",
        table="int_assets",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    # --- STEP 5. DBT TRANSFORMATION (MARTS) ---
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "install_deps": True,
            "trigger_rule": "none_failed_min_one_success",
        },
    )

    # --- STEP 6. WORKFLOWS ---
    ingest_forex = run_forex()
    ingest_stocks = run_stocks()

    [ingest_forex, ingest_stocks] >> pyspark_transform

    pyspark_transform >> check_forex >> load_forex
    pyspark_transform >> check_assets >> load_assets

    [load_forex, load_assets] >> dbt_build


daily_pipeline()
