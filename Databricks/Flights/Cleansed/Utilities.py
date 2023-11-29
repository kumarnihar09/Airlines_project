# Databricks notebook source
def pre_schema(df):
    try:
        schema = ""
        for i in df.dtypes:
            schema += i[0] +" "+ i[1] + ','
        return schema[:-1]
    except Exception as err:
        print("Error Occured ", str(err))

# COMMAND ----------

def f_delta_cleansed_load(database,table_name,schema,location):
    try:
      spark.sql(f"""
            create table if not exists {database}.{table_name}
             ({schema})
             using delta
             location '{location}'
           """)
    except Exception as err:
      print("Error Occured ",str(err))


# COMMAND ----------

spark.sql("""DESC HISTORY cleansed_flightsproject.airlines""").createOrReplaceTempView("Table_count")

# COMMAND ----------

def f_count_check(database,operation_type,table_name,number_diff):
    spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_count")
    current_count = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version = (select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")
    if(current_count.first() is None):
        final_current_count = 0
    else:
        final_current_count = current_count.first().numOutputRows

    previous_count = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version < (select version from Table_count where trim(lower(operation))=lower('{operation_type}') order by version desc limit 1)""")
    if(previous_count.first() is None):
        final_previous_count = 0
    else:
        final_previous_count = previous_count.first().numOutputRows
    if(int(final_current_count)-int(final_previous_count)>100):
        print('Difference is huge', table_name)
        raise Exception('Difference is huge in', table_name)
    else:
        pass
     
