# Databricks notebook source
# MAGIC %run /Users/kumarnihar09@gmail.com/Flights/Cleansed/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the data

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Performing Transformations

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(`Date-Part`,'yyyy-MM-dd') as Date_Part"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the Data

# COMMAND ----------

df_base.writeStream.trigger(once=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS"
).start("/mnt/cleansed_datalake/unique_carriers")

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of SQL table on top of cleansing table/location to do query/manipulation

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/cleansed_datalake/unique_carriers")
schema = pre_schema(df)
f_delta_cleansed_load('cleansed_flightsproject','unique_carriers',schema,'/mnt/cleansed_datalake/unique_carriers')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.unique_carriers;
