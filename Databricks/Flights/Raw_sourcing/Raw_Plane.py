# Databricks notebook source
pip install tabula-py

# COMMAND ----------

from datetime import date
print(date.today())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/raw_datalake/'

# COMMAND ----------

import tabula
from datetime import date
def f_source_pdf_datalake(source,sink_path,output_format,page,file_name):
    try:
        dbutils.fs.mkdirs(f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
        tabula.convert_into(f'/dbfs/{source}{file_name}',f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format, pages=page)
    except Exception as err:
        print("Error Occured ", str(err))

# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]
for i in list_files:
    f_source_pdf_datalake('mnt/source_blob/','mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------

# adls_directory_path = '/PLANE'
# directory_path_adls = f"/mnt/raw_datalake{adls_directory_path}"
# if dbutils.fs.mkdirs(directory_path_adls):
from datetime import date
if dbutils.fs.mkdirs(f"/mnt/raw_datalake/PLANE/Date_Part={date.today()}/"):
    print(f"Directory created successfully in ADLS:")

# COMMAND ----------

import tabula
from datetime import date
dbutils.fs.mkdirs(f"/dbfs/mnt/raw_datalake/PLANE/Date_Part={date.today()}/")
tabula.convert_into("/dbfs/mnt/source_blob/PLANE.pdf",f"/dbfs/mnt/raw_datalake/PLANE/Date_Part={date.today()}/PLANE.csv",output_format='csv', pages='all')


# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/raw_datalake/'
