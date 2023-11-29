# Databricks notebook source
# MAGIC %run /Users/kumarnihar09@gmail.com/Flights/Cleansed/Utilities

# COMMAND ----------

list_table_info = [
    ("STREAMING UPDATE", "plane", 100),
    ("STREAMING UPDATE", "cancellation", 100),
    ("STREAMING UPDATE", "flight", 100),
    ("STREAMING UPDATE", "airport", 100),
    ("Write", "airlines", 10),
    ("STREAMING UPDATE", "unique_carriers", 100),
]
for i in list_table_info:
    f_count_check("cleansed_flightsproject", i[0], i[1], i[2])
