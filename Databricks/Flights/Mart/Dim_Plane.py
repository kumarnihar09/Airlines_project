# Databricks notebook source
# MAGIC %sql
# MAGIC use mart_flightsproject;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.plane;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.plane

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DIM_PLANE (
# MAGIC   tailid string,
# MAGIC   type string,
# MAGIC   manufacturer string,
# MAGIC   issue_date date,
# MAGIC   model string,
# MAGIC   status string,
# MAGIC   aircraft_type string,
# MAGIC   engine_type string,
# MAGIC   year int
# MAGIC ) USING DELTA LOCATION '/mnt/mart_datalake/DIM_PLANE'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT
# MAGIC   OVERWRITE DIM_PLANE
# MAGIC SELECT
# MAGIC   tailid,
# MAGIC   type,
# MAGIC   manufacturer,
# MAGIC   issue_date,
# MAGIC   model,
# MAGIC   status,
# MAGIC   aircraft_type,
# MAGIC   engine_type,
# MAGIC   year
# MAGIC FROM
# MAGIC   cleansed_flightsproject.plane

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_plane
