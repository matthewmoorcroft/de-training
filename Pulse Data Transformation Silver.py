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
source_repo_path = f"/Workspace/Repos/{username}/de-training/DE Training Pulse Check (Responses) - Form Responses 1.csv"
my_catalog = f"{user_prefix}_{city}_training"
my_volume = f"pulse_check"
target_file_path = f"/Volumes/{my_catalog}/bronze/{my_volume}/pulse_data.csv"

# COMMAND ----------

# DBTITLE 1,Create Silver Table
from pyspark.sql.functions import split

silver_df = spark.read.table(f"{my_catalog}.bronze.raw_pulse_data")

# Split the 'favorite_city' column into two parts: 'city' and 'rating'
# The split function uses a regex pattern that looks for a hyphen possibly surrounded by spaces
split_col = split(silver_df['favorite_city'], ' - | -|-|- ')

# Add the split columns to the DataFrame
silver_df = silver_df.withColumn('city', split_col.getItem(0))
silver_df = silver_df.withColumn('rating', split_col.getItem(1))

# The 'rating' column is currently of type string, convert it to integer
transfomed_df = silver_df.withColumn("rating", silver_df["rating"].cast(IntegerType()))

# Save as silver table
transfomed_df.write.mode("overwrite").saveAsTable(f"{my_catalog}.silver.transformed_pulse_data")
