import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "billing"
tbl_name = "aws"

# S3 location for output
output_dir = "s3://sts-billing-etl-temp/output-dir/clean_billing"

# Read data into a DynamicFrame using the Data Catalog metadata
billing_dyf = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)

# Remove erroneous records
billing_df = billling_res.toDF()
billing_df = billing_df.where("`provider id` is NOT NULL")
