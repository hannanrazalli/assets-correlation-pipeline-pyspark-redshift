import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from pendulum import datetime

default_args = {
    "owner": "Hannan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

BUCKET = os.getenv("BUCKET_NAME")


@dag(
    dag_id="00_Daily_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 2),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
)
def daily_pipeline():

    # --- 1. Ingest API ---
    @task(task_id="Daily_forex")
    def daily_forex(ds=None):
        from include.ingestions.forex import daily_forex

        daily_forex(ds=ds)

    @task(task_id="Daily_stocks")
    def daily_stocks(ds=None):
        from include.ingestions.stocks import daily_stocks

        daily_stocks(ds=ds)

    # --- 2. PySpark Transform ---
    pyspark_transform = GlueJobOperator(
        task_id="PySpark Medallion",
        job_name="Glue",
        script_args={
            "--ds": "{{ ds }}",
        },
        wait_for_completion=True,
    )

    # --- 3. Copy to Redshift ---
    load_forex = S3ToRedshiftOperator(
        task_id="Copy_forex_to_redshift",
        s3_bucket=BUCKET,
        s3_key="pyspark/forex/"
        "year={{ logical_date.strftime('%Y') }}/"
        "month={{ logical_date.strftime('%m') }}/"
        "day={{ logical_date.strftime('%d') }}/",
        schema="raw_data",
        table="raw_forex",
        copy_options=["FORMAT AS JSON 'auto'"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    load_stocks = S3ToRedshiftOperator(
        task_id="Copy_stocks_to_redshift",
        s3_bucket=BUCKET,
        s3_key="pyspark/assets/"
        "year={{ logical_date.strftime('%Y') }}/"
        "month={{ logical_date.strftime('%m') }}/"
        "day={{ logical_date.strftime('%d') }}/",
        schema="raw_data",
        table="raw_stocks",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    run_forex = daily_forex()
    run_stocks = daily_stocks()

    [run_forex, run_stocks] >> pyspark_transform >> [load_forex, load_stocks]


daily_pipeline()
