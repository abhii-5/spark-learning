# Databricks notebook source
# MAGIC %md ##### First create connection to storage using secret key(create scope)
# MAGIC - create secret for your storage key(using key vault)
# MAGIC - edit databricks notebook url to open Create Secret Scope page (after# add secrets/createScope)
# MAGIC - Give name for scope name
# MAGIC - manage principal: 
# MAGIC - Enter key vault information(DNS Name & Resource id present inside properties)
# MAGIC   - vault url = DNS NAME
# MAGIC   

# COMMAND ----------

spark.conf.set("fs.azure.account.key.stagingbronze.dfs.core.windows.net", dbutils.secrets.get("Accesskeys","stagingbronze-key"))
# spark.conf.set(
#     "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
#     dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

# MAGIC %md ###First try to create database in ADLS GEN2 storage given location

# COMMAND ----------

#databricksdb / goku
# CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
#  WITH DBPROPERTIES (ID=001, Name='John');
db_name='goku'
location='abfss://databricksdb@stagingbronze.dfs.core.windows.net'


spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} COMMENT 'This is external database stored in container nake databricksdb' LOCATION {location}")
