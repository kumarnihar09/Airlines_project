# Databricks notebook source
# MAGIC %run /Flights/Utilities

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the Data

# COMMAND ----------

df = spark.read.json('/mnt/raw_datalake/airlines/')
#for json to exand
df1 = df.selectExpr(
  "explode(response)",
  "`Date-Part` as Date_Part"
)
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC Performing transformations and Writing

# COMMAND ----------

df_final = df1.select("col.*","Date_Part")
display(df_final)
df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airlines")

# COMMAND ----------

#dbutils.fs.rm('/mnt/cleansed_datalake/airlines',True)

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of SQL table on top of cleansing table/location to do query/manipulation

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/cleansed_datalake/airlines").limit(1)
schema = pre_schema(df)
f_delta_cleansed_load('cleansed_flightsproject','airlines',schema,'/mnt/cleansed_datalake/airlines')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cleansed_flightsproject.airlines;
