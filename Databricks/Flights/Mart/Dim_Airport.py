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
# MAGIC select * from cleansed_flightsproject.airport;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.airport

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table dim_airport

# COMMAND ----------

dbutils.fs.rm('/mnt/mart_datalake/DIM_AIRPORT',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DIM_AIRPORT (
# MAGIC   code STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   airport STRING
# MAGIC ) USING DELTA LOCATION '/mnt/mart_datalake/DIM_AIRPORT'

# COMMAND ----------

# MAGIC %md
# MAGIC Overwriting data of a particular year only, not touching other year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT
# MAGIC   OVERWRITE DIM_AIRPORT
# MAGIC SELECT
# MAGIC   code,
# MAGIC   city,
# MAGIC   country,
# MAGIC   airport
# MAGIC FROM
# MAGIC cleansed_flightsproject.airport

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_AIRPORT;
