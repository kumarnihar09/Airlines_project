# Databricks notebook source
# MAGIC %run /Flights/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the data

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
    .option("cloudFiles.schemaLocation",'/dbfs/FileStore/tables/schema/PLANE')\
    .load('/mnt/raw_datalake/PLANE')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Changing the column datatypes and writing 

# COMMAND ----------

dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/PLANE",True)

# COMMAND ----------

df_base = df.selectExpr("tailnum as tailid","type","manufacturer","to_date(issue_date) as issue_date","model","status","aircraft_type","engine_type","cast('year' as int) as year","to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
# display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/cleansed_datalake/PLANE")

# COMMAND ----------

bze of checkpoint location it is skipping the file if it comes again

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of SQL table on top of cleansing table/location to do query/manipulation

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/cleansed_datalake/PLANE")
schema = pre_schema(df)
f_delta_cleansed_load('cleansed_flightsproject','plane',schema,'/mnt/cleansed_datalake/PLANE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.plane;
