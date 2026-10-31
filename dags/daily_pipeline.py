import os
from datetime import timedelta
from pendulum import datetime

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.decorators import dag, task

default_args = {
    "owner": "Hannan Razalli",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

BUCKET=os.getenv("BUCKET_NAME")

@dag(
    dag_id="Daily_ingestions",
    schedule="@daily",
    start_date=datetime(2026, 10, 31),
    default_args=default_args,
)
def daily_pipeline():

    # --- 1. Ingest API ---
    
    @task(task_id="daily_forex")
    def daily_forex(ds=None):
        from include.ingestions.forex import daily_forex

        daily_forex(ds=ds)

    @task(task_id="daily_stocks")
    def daily_stocks(ds=None):
        from include.ingestions.stocks import daily_stocks

        daily_stocks(ds=ds)

    # --- 2. Copy to Redshift ---
    
    load_forex = S3ToRedshiftOperator(
        task_id="load_forex_to_redshift",
        schema="raw_data",
        table="raw_forex",
        s3_bucket=BUCKET,
        s3_key="raw/forex/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/day={{ logical_date.strftime('%d') }}/",
        copy_options=["FORMAT AS JSON 'auto'"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    s3_bucket=BUCKET,
    s3_key="raw/forex/year={{ ds }}.strftime('%Y)/month={{ ds }}.strftime('%Y)/day={{ ds }}.strftime('%Y)/",
    schema=raw_data,
    table=,
    copy_options=,
    redshift_conn_id=,
    method=,

    daily_forex()
    daily_stocks()


daily_pipeline()
