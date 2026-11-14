import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from pendulum import datetime

default_args = {
    "owner": "Hannan",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

BUCKET = os.getenv("BUCKET_NAME")


@dag(
    dag_id="Daily_data_ingest",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 11, 14),
    catchup=False,
    max_active_runs=1,
)
def daily_pipeline():

    @task(task_id="FX_API_to_S3")
    def run_forex(ds=None):
        from include.ingestions.forex import daily_forex

        daily_forex(ds=ds)

    @task(task_id="Stocks_API_to_S3")
    def run_stocks(ds=None):
        from include.ingestions.stocks import daily_stocks

        daily_stocks(ds=ds)

    pyspark_transform = GlueJobOperator(
        task_id="medallion_bronze_silver",
        job_name="Glue",
        script_args={"--ds": "{{ ds }}"},
        wait_for_completion=True,
    )

    load_forex = S3ToRedshiftOperator(
        task_id="copy_fx_to_redshift",
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
        task_id="copy_assets_to_redshift",
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

    ingest_forex = run_forex()
    ingest_stocks = run_stocks()

    [ingest_forex, ingest_stocks] >> pyspark_transform >> [load_forex, load_assets]


daily_pipeline()
