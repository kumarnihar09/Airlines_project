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
# MAGIC select * from cleansed_flightsproject.unique_carriers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.unique_carriers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DIM_UNIQUECARRIERS (
# MAGIC   code STRING,
# MAGIC   description STRING
# MAGIC ) USING DELTA LOCATION '/mnt/mart_datalake/DIM_UNIQUECARRIERS'

# COMMAND ----------

# MAGIC %md
# MAGIC Overwriting data of a particular year only, not touching other year

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT
# MAGIC   OVERWRITE DIM_UNIQUECARRIERS
# MAGIC SELECT
# MAGIC   code,
# MAGIC   description
# MAGIC FROM
# MAGIC   cleansed_flightsproject.unique_carriers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_UNIQUECARRIERS;
