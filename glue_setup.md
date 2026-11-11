<<<<<<< HEAD
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])  ---> Can change

job_name = args["JOB_NAME"]
target_date = args["ds"]


# 1. Hidupkan enjin asas Spark
sc = SparkContext()

# 2. Balut SparkContext dengan lapisan Glue
glueContext = GlueContext(sc)

# 3. Cipta sesi Spark DataFrame (Inilah variable 'spark' yang sama macam di Databricks)
spark = glueContext.spark_session

# 4. Beritahu AWS Glue bahawa kerja sudah bermula
job = Job(glueContext)
job.init(args["JOB_NAME"], args)





*PRACTICE*

import os
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# STEP 1: SETUP GLUE
=======
*----------*
*GLUE SETUP*
*----------*

# STEP 1:
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

>>>>>>> 7c4ffca0c46b2a478a4d6229ce8bda187525c89f
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])

job_name = args["JOB_NAME"]
target_date = args["ds"]

<<<<<<< HEAD
sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.spark_session
job = Job(glueContext)

job.init(args["JOB_NAME"], args)


# STEP 2: GLUE AIRFLOW SETUP
year, month, day = target_date.split("-")
BUCKET_NAME = os.getenv("BUCKET_NAME")

forex_raw_path = f"s3://{BUCKET_NAME}/raw/forex/year={year}/month={month}/day={day}/"
stocks_raw_path = f"s3://{BUCKET_NAME}/raw/stocks/year={year}/month={month}/day={day}/"

df_raw_fx = spark.read.json(forex_raw_path)
df_raw_fx = spark.read.parquet(stocks_raw_path)

job.commit()
=======

# STEP 2:
1. Hidupkan enjin asas Spark
sc = SparkContext()

2. Balut SparkContext dengan lapisan Glue
glueContext = GlueContext(sc)

3. Cipta sesi Spark DataFrame (Inilah variable 'spark' yang sama macam di Databricks)
spark = glueContext.spark_session

4. Beritahu AWS Glue bahawa kerja sudah bermula
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
>>>>>>> 7c4ffca0c46b2a478a4d6229ce8bda187525c89f
