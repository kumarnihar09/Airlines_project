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
     

# COMMAND ----------

def insert_test_cases(database,insert_id,insert_test_cases,insert_test_query,insert_expected_result):
    try:
        spark.sql(f"""create table if not exists {database}.insert_test_cases(
            id int,
            test_cases string,
            test_query string,
            expected_result int
        )""")
        spark.sql(f"""insert into {database}.insert_test_cases(id,test_cases,test_query,expected_result) values
                  ({insert_id},'{insert_test_cases}','{insert_test_query}','{insert_expected_result}')""")
    except Exception as err:
        print("Error occurred",str(err))
        

# COMMAND ----------

def execute_test_case(database):
    df = spark.sql(f"""select * from {database}.insert_test_cases""").collect()
    for i in df:
        original_result = spark.sql(f"""{i.test_query}""").collect()
        if(len(original_result) == i.expected_result):
            print("Test case is passed")
        else:
            raise Exception (f"{test_cases} is failed, Kindly check")

