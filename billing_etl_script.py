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
billing_df = billing_df.where("`TotalCost` is INTEGER")

# Convert dates into standard format that can be consumed (YY/MM/DD)
# BillingPeriodStartDate
# BillingPeriodEndDate
# UsageStartDate
# UsageEndDate

# breaks: /,/, ,:

#  Turn it back to a dynamic frame
billing_tmp = DynamicFrame.fromDF(billing_df, glueContext, "nested")

# Rename, case, and nest with apply_mapping
billing_nest = billing_tmp.apply_mapping(
#unclear how to use apply_mapping
# LinkedAccountId- string
# BillingPeriodStartDate- string
# BillingPeriodEndDate- string
# LinkedAccountName- string
# ProductCode- string
# ProductName- string
# UsageType-  string
# UsageStartDate- string
# UsageEndDate- string
# UsageQuantity- float (or should it be int?)
# BlendedRate- float
# TotalCost- float
# user:Application clean up tags, harmonize with required tags
# user:Application Group
# user:BillingCode
# user:Environment
# user:Name
# user:Portfolio
# user:ResourceType
)

# Write it out in Parquet (what's parquet)
glueContext.write_dynamic_frame.from_options(frame = billing_nest, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")
