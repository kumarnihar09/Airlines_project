# Databricks notebook source
# MAGIC %md
# MAGIC Mounting of ADLS container Using SAS token with secret scope and key vault - Source

# COMMAND ----------

# Container name
# Stoage account name
# Mount point name (it could be anything /mnt/.....)
# Databricks secret scope name
# Azure key vault key name containing the secret info
 
#code Template
container =  dbutils.secrets.get(scope="flights-secret-scope",key="blobcontainer")
storage = dbutils.secrets.get(scope="flights-secret-scope",key="blobstorage")
sas = dbutils.secrets.get(scope="flights-secret-scope",key="sas-secret")
config = f"fs.azure.sas.{container}.{storage}.blob.core.windows.net"

mountPoint = '/mnt/source_blob/'
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        #source = 'wasbs://source@flightsblobstorage.blob.core.windows.net',
        source = dbutils.secrets.get(scope="flights-secret-scope",key="blobmountpath"), 
        mount_point= mountPoint,
        extra_configs = {config: sas})

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting of ADLS contianer Using Application Registration with secret scope and key vault - Sink(Raw)

# COMMAND ----------

# MAGIC %md
# MAGIC https://login.microsoftonline.com/cafb6eb2-a7bc-456f-b5a2-dac8f347a291/oauth2/token

# COMMAND ----------

# Container name
# Stoage account name
# Tennant Id
# Application Id
# Mount point name (it could be anything /mnt/.....)
# Databricks secret scope name
# Azure key vault key name containing the secret info
 
#Code Template 
 
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-id"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-secret"),
       "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="flights-secret-scope",key="data-client-refresh-url"),
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
 
mountPoint = "/mnt/raw_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
       source = dbutils.secrets.get(scope="flights-secret-scope",key="datalake-raw"),
       mount_point = mountPoint,
       extra_configs = configs)
 
#abfss://raw@flightsadlssink.dfs.core.windows.net 


# COMMAND ----------

# Container name
# Stoage account name
# Tennant Id
# Application Id
# Mount point name (it could be anything /mnt/.....)
# Databricks secret scope name
# Azure key vault key name containing the secret info
 
#Code Template 
 
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-id"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-secret"),
       "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="flights-secret-scope",key="data-client-refresh-url"),
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
 
mountPoint = "/mnt/cleansed_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
       source = dbutils.secrets.get(scope="flights-secret-scope",key="datalake-cleansed"),
       mount_point = mountPoint,
       extra_configs = configs)
 
#abfss://cleansed@flightsadlssink.dfs.core.windows.net 


# COMMAND ----------

# Container name
# Stoage account name
# Tennant Id
# Application Id
# Mount point name (it could be anything /mnt/.....)
# Databricks secret scope name
# Azure key vault key name containing the secret info
 
#Code Template 
 
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-id"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="flights-secret-scope",key="data-app-secret"),
       "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="flights-secret-scope",key="data-client-refresh-url"),
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
 
mountPoint = "/mnt/mart_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
       source = dbutils.secrets.get(scope="flights-secret-scope",key="datalake-mart"),
       mount_point = mountPoint,
       extra_configs = configs)
 
#abfss://mart@flightsadlssink.dfs.core.windows.net 

