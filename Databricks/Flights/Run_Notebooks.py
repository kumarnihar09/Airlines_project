# Databricks notebook source
dbutils.widgets.text("Layer_Name","")
Layer_Name = dbutils.widgets.getArgument("Layer_Name")

# COMMAND ----------

Notebook_Path_Json = {
    
        "Raw": ["/Flights/Raw_sourcing/Raw_Plane"],
        "Cleansed": [
            "/Flights/Cleansing/Airlines",
            "/Flights/Cleansing/Airport",
            "/Flights/Cleansing/Cancellation",
            "/Flights/Cleansing/Flight",
            "/Flights/Cleansing/Plane",
            "/Flights/Cleansing/Unique_Carrier",
        ],
        "Data_Quality_Cleansed": [
            "/Flights/Data_Quality_Notebook/Cleansing_Data_Quality"
        ],
        "Mart": [
            "/Flights/Mart/Dim_Airlines",
            "/Flights/Mart/Dim_Airport",
            "/Flights/Mart/Dim_Plane",
            "/Flights/Cleansing/Dim_UniqueCarrier",
            "/Flights/Mart/Dim_Cancellation",
            "/Flights/Mart/Reporting_Flight",
        ],
        "Data_Quality_Mart": ["/Flights/Data_Quality/Execute_Mart_Data_Quality"]
    }

# COMMAND ----------

print(Notebook_Path_Json[Layer_Name])

# COMMAND ----------

for notebook_paths in Notebook_Path_Json[Layer_Name]:
    dbutils.notebook.run(notebook_paths,0)
