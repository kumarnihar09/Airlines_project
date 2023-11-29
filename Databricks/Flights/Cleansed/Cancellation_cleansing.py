# Databricks notebook source
# MAGIC %run /Users/kumarnihar09@gmail.com/Flights/Cleansed/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the data

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'parquet')\
    .option("cloudFiles.schemaLocation",'/dbfs/FileStore/tables/schema/Cancellation')\
    .load('/mnt/raw_datalake/Cancellation')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Changing the column datatypes and writing 

# COMMAND ----------

dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/cancellation",True)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(`Date-Part`,'yyyy-MM-dd') as Date_Part"
                        )
#display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/cancellation")\
    .start("/mnt/cleansed_datalake/cancellation")

# COMMAND ----------

bze of checkpoint location it is skipping the file if it comes again

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of SQL table on top of cleansing table/location to do query/manipulation

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/cleansed_datalake/cancellation")
schema = pre_schema(df)
f_delta_cleansed_load('cleansed_flightsproject','cancellation',schema,'/mnt/cleansed_datalake/cancellation')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_flightsproject.cancellation;
