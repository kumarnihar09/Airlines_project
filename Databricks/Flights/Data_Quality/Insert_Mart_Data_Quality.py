# Databricks notebook source
# MAGIC %run /Flights/Utilities

# COMMAND ----------

insert_query = "select count(*) from mart_flightsproject.dim_airport group by code having count(*)>1"
insert_test_cases("mart_flightsproject",1,"Check if code is duplicated in the dim_airport or not",insert_query,0)

# COMMAND ----------

insert_query = "select count(*) from mart_flightsproject.dim_plane group by tailid having count(*)>1"
insert_test_cases("mart_flightsproject",2,"Check if tailid is duplicated in the dim_plane or not",insert_query,0)
