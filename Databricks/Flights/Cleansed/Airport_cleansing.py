# Databricks notebook source
# MAGIC %run /Flights/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the data

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
    .option("cloudFiles.schemaLocation",'/dbfs/FileStore/tables/schema/Airport')\
    .load('/mnt/raw_datalake/Airport')

# COMMAND ----------

# MAGIC %md
# MAGIC Changing the column datatypes and writing 

# COMMAND ----------

#dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/airport",True)

# COMMAND ----------

df_base = df.selectExpr(
    "Code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part",
                        )

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/airport")\
    .start("/mnt/cleansed_datalake/airport")

# COMMAND ----------

#bze of checkpoint location it is skipping the file if it comes again

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of SQL table on top of cleansing table/location to do query/manipulation

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/cleansed_datalake/airport")
schema = pre_schema(df)
f_delta_cleansed_load('cleansed_flightsproject','airport',schema,'/mnt/cleansed_datalake/airport')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.airport;
