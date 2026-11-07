*----------*
*GLUE SETUP*
*----------*

# STEP 1:
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])

job_name = args["JOB_NAME"]
target_date = args["ds"]


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