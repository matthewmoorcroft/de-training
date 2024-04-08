# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to:
# MAGIC - #### Copy pulse data to Unity Catalog Volume
# MAGIC - #### Create Bronze layer table
# MAGIC - #### Transform the pulse data
# MAGIC - #### Create Silver layer table

# COMMAND ----------

# DBTITLE 1,Setup Variables
# input your details
username = f"odl_instructor_1279278@databrickslabs.com"
user_prefix = f"mm"
city = "dublin"

# Setup all required paths
source_repo_path = f"/Workspace/Repos/{username}/de-training/DE Training Pulse Check.csv"
my_catalog = f"{user_prefix}_{city}_training"
my_volume = f"pulse_check"
target_file_path = f"/Volumes/{my_catalog}/bronze/{my_volume}/pulse_data.csv"

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
catalog_sql = f"CREATE CATALOG IF NOT EXISTS {my_catalog}"
spark.sql(catalog_sql)

schema_list = ['bronze', 'silver', 'gold']
for schema in schema_list:
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{schema}"
    spark.sql(schema_sql)

volume_sql = f"CREATE VOLUME IF NOT EXISTS {my_catalog}.bronze.{my_volume}"
spark.sql(volume_sql)

# COMMAND ----------

# DBTITLE 1,Copy raw pulse csv to UC Volume
import os
import shutil

# Move the source file to the target path
shutil.copy(source_repo_path, target_file_path)

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Provide the schema
schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("job_role", StringType(), True),
    StructField("training_type", StringType(), True),
    StructField("databricks_knowledge", StringType(), True),
    StructField("favorite_city", StringType(), True),
    # Add as many fields as you have in your CSV
])

# Read the pulse data csv
bronze_df = spark.read.options(multiline = True, delimiter=",", header=True).schema(schema).csv(target_file_path)

# Save as bronze table
bronze_df.write.mode("overwrite").saveAsTable(f"{my_catalog}.bronze.raw_pulse_data")
