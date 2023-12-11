# Flights
Process :-
1. Load data from different source with different type of data
   1.Blob - Excel, PDF
   2.Azure SQL Database - 2 tables
   3.Azure DataLake Gen2 - 1 zip folder, 1 csv file
   4.API
2. Dump above mention data into GEN2 Raw container day wise.
3. Clean the data and create cleansed delta tables.
4. Create Fact and Dimension based on business logic and save the data into ADLS Gen2(Mart container/layer).
5. Publish the data into Publish GEN2 layer and Azure SQL DB/DW.
6. Create PowerBI report and show required graphs.
7. Send publish data to respective owners based on given excel file.
8. Create CI/CD pipeline to deploy the code to PROD environment.

Should be followed 
1. Validate the data between source and sink
2. Send Alert when data is wrong or junk(Data Quality checks)
3. Send Alerts when pipelines gets failed
4. Use minimum resources as much as possible, use frameworks which can be used in other projects too
