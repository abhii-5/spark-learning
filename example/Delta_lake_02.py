# Databricks notebook source
spark.conf.set("fs.azure.account.key.asadatalake9mf5gvx.dfs.core.windows.net",dbutils.secrets.get(scope="AccessKeys", key="landingstorage-asadatalake"))

# COMMAND ----------

EMPNO:integer
ENAME:string
JOB:string
MGR:string
HIREDATE:string
SAL:integer
COMM:string
DEPTNO:integer

# COMMAND ----------

#create blank delta table using the given metadata 
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 

emp_table_python = (DeltaTable.create()
    .location('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
    .addColumn('EMPNO', dataType = IntegerType(),nullable = False)
    .addColumn('ENAME', dataType = StringType(), nullable = True)
    .addColumn('JOB', dataType = StringType(), nullable = True)
    .addColumn('MGR', dataType = StringType(), nullable = True)
    .addColumn('HIREDATE', dataType = StringType(), nullable = True)
    .addColumn('SAL', dataType = IntegerType(), nullable = True)
    .addColumn('COMM', dataType = IntegerType(), nullable = True)
    .addColumn('DEPTNO', dataType = IntegerType(), nullable = True)
    .execute())
              

# deltaTable = DeltaTable.create(sparkSession)
#     .tableName("testTable")
#     .addColumn("c1", dataType = "INT", nullable = False)
#     .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1")
#     .partitionedBy("c1")
#     .execute()

# COMMAND ----------

from delta.tables import *
from pyspark.sql.types import * 

emp_table_python = (DeltaTable.replace() 
    .location('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
    .addColumn('EMPNO', dataType = IntegerType(),nullable = False)
    .addColumn('ENAME', dataType = StringType(), nullable = True)
    .addColumn('JOB', dataType = StringType(), nullable = True)
    .addColumn('MGR', dataType = StringType(), nullable = True)
    .addColumn('HIREDATE', dataType = StringType(), nullable = True)
    .addColumn('SALARY', dataType = IntegerType(), nullable = True)
    .addColumn('COMM', dataType = IntegerType(), nullable = True)
    .addColumn('DEPTNO', dataType = IntegerType(), nullable = True)
    .execute())

# if we are using createOrReplace()  and we change column name from sal to salary then we get The specified schema does not match the existing schema at.. path 
# but if we do only replace then it will work and your snapshot version will also increase 


# COMMAND ----------

employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
employees_delta.printSchema()

# COMMAND ----------

employees_delta_after_replace=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
employees_delta_after_replace.printSchema()

# COMMAND ----------

# MAGIC %md <i18n value="3b9c0755-bf72-480e-a836-18a4eceb97d2"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Creating a Delta Table
# MAGIC 
# MAGIC There's not much code you need to write to create a table with Delta Lake. There are a number of ways to create Delta Lake tables that we'll see throughout the course. We'll begin with one of the easiest methods: registering an empty Delta Lake table.
# MAGIC 
# MAGIC We need: 
# MAGIC - A **`CREATE TABLE`** statement
# MAGIC - A table name (below we use **`students`**)
# MAGIC - A schema
# MAGIC 
# MAGIC **NOTE:** In Databricks Runtime 8.0 and above, Delta Lake is the default format and you don’t need **`USING DELTA`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS students_v1
# MAGIC   (id INT, name STRING, value DOUBLE)
# MAGIC -- This table is created inside default database << where is the default location of that table   
