# Databricks notebook source
# MAGIC %md
# MAGIC #Business logic to be answered by this 
# MAGIC - Monthly Trend of #delay flights(Both Arrival and Departure)
# MAGIC - Flights cancelled category wise
# MAGIC - Flights in different Airlines 
# MAGIC - Airlines whose flight delayed 
# MAGIC - Plane Data like Build Year, Aircraft_name, Engine_name

# COMMAND ----------

# MAGIC %sql
# MAGIC use mart_flightsproject;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.flight;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_flightsproject.flight

# COMMAND ----------

# MAGIC %md
# MAGIC Creating dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Reporting_Flight (
# MAGIC   date date,
# MAGIC   ArrDelay int,
# MAGIC   DepDelay int,
# MAGIC   Origin string,
# MAGIC   Cancelled int,
# MAGIC   CancellationCode string,
# MAGIC   UniqueCarrier string,
# MAGIC   FlightNum int,
# MAGIC   TailNum string,
# MAGIC   deptime string
# MAGIC ) USING DELTA PARTITIONED BY (date_year INT) LOCATION '/mnt/mart_datalake/Reporting_Flight'

# COMMAND ----------

max_year=spark.sql("select year(max(date)) from cleansed_flightsproject.flight").collect()[0][0]
print(max_year)

# COMMAND ----------

# MAGIC %md
# MAGIC Overwriting data of a particular year only, not touching other year

# COMMAND ----------

spark.sql(
    f"""
INSERT
  OVERWRITE Reporting_Flight PARTITION (date_year = {max_year}) 
SELECT
  date,
  ArrDelay,
  DepDelay,
  Origin,
  Cancelled,
  CancellationCode,
  UniqueCarrier,
  FlightNum,
  TailNum,
  deptime
FROM
cleansed_flightsproject.flight where year(date)={max_year} """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reporting_flight;
