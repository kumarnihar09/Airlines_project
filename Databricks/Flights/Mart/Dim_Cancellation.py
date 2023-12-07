# Databricks notebook source
# MAGIC %md
# MAGIC Business logic to be answered by this 
# MAGIC -
# MAGIC -
# MAGIC -
# MAGIC -
# MAGIC -
# MAGIC -
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use mart_flightsproject;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.cancellation;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.cancellation

# COMMAND ----------

#dbutils.fs.rm('/mnt/mart_datalake/DIM_CANCELLATION',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DIM_CANCELLATION (
# MAGIC   code STRING,
# MAGIC   description STRING
# MAGIC ) USING DELTA LOCATION '/mnt/mart_datalake/DIM_CANCELLATION'

# COMMAND ----------

# MAGIC %md
# MAGIC Overwriting data of a particular year only, not touching other year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT
# MAGIC   OVERWRITE DIM_CANCELLATION
# MAGIC SELECT
# MAGIC   code,
# MAGIC   description
# MAGIC FROM
# MAGIC cleansed_flightsproject.cancellation 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_CANCELLATION;
