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
# MAGIC select * from cleansed_flightsproject.airlines;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.airlines

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DIM_AIRLINES (
# MAGIC   iata_code STRING,
# MAGIC   icao_code STRING,
# MAGIC   name STRING
# MAGIC ) USING DELTA LOCATION '/mnt/mart_datalake/DIM_AIRLINES'

# COMMAND ----------

# MAGIC %md
# MAGIC Overwriting data of a particular year only, not touching other year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT
# MAGIC   OVERWRITE DIM_AIRLINES
# MAGIC SELECT
# MAGIC   iata_code,
# MAGIC   icao_code,
# MAGIC   name
# MAGIC FROM
# MAGIC cleansed_flightsproject.airlines

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_AIRLINES;
