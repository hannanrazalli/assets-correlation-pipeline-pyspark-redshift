import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = os.getenv("BUCKET_NAME")


def has_files(name, logical_date, **_):
    prefix = (
        f"pyspark/{name}/"
        f"year={logical_date:%Y}/month={logical_date:%m}/day={logical_date:%d}/"
    )
    return bool(S3Hook().list_keys(bucket_name=BUCKET, prefix=prefix))
