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
# MAGIC 
# MAGIC <i18n value="a00174f3-bbcd-4ee3-af0e-b8d4ccb58481"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC If we try to go back and run that cell again...it will error out! This is expected - because the table exists already, we receive an error.
# MAGIC 
# MAGIC We can add in an additional argument, **`IF NOT EXISTS`** which checks if the table exists. This will overcome our error.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS students_v1
# MAGIC   (id INT, name STRING, value DOUBLE)
# MAGIC -- This table is created inside default database << where is the default location of that table   

# COMMAND ----------

# MAGIC %md #### Now try to insert some row in the table which we created using python api 

# COMMAND ----------

#1 Read data from csv 
emp_data_read = spark.read.format('csv').options(inferSchema="true",header="true").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp.csv')

#2 create view of tables 
emp_data_read.createOrReplaceTempView('insert_records')

from pyspark.sql.functions import col

data_type_change=(emp_data_read.withColumn('COMM',col('COMM').cast("int"))
                  .withColumnRenamed('SAL','SALARY')
                 )

display(data_type_change)

# one more error due to column name mismatch in delta table it is salary but in file it is sal


# COMMAND ----------

# insert in table, we have used append mode 
data_type_change.write.format("delta").mode("append").save('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')

# COMMAND ----------

employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
#DeltaTable.history(employees_delta)
from delta.tables import *
employees_delta_his = DeltaTable.forPath(spark, 'abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
fullHistoryDF = employees_delta_his.history() 
display(fullHistoryDF)
# this is the only method how we can display history

# COMMAND ----------

fullHistoryDF.createOrReplaceTempView('fullHistoryDF')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fullHistoryDF;

# COMMAND ----------

# MAGIC %md <i18n value="4ecaf351-d4a4-4803-8990-5864995287a4"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC What may surprise you is that Delta Lake guarantees that any read against a table will **always** return the most recent version of the table, and that you'll never encounter a state of deadlock due to ongoing operations.
# MAGIC 
# MAGIC To repeat: table reads can never conflict with other operations, and the newest version of your data is immediately available to all clients that can query your lakehouse. Because all transaction information is stored in cloud object storage alongside your data files, concurrent reads on Delta Lake tables is limited only by the hard limits of object storage on cloud vendors. (**NOTE**: It's not infinite, but it's at least thousands of reads per second.)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Updating Records
# MAGIC 
# MAGIC Updating records provides atomic guarantees as well: we perform a snapshot read of the current version of our table, find all fields that match our **`WHERE`** clause, and then apply the changes as described.
# MAGIC 
# MAGIC Below, we find all students that have a name starting with the letter **T** and add 1 to the number in their **`value`** column.

# COMMAND ----------

employees_delta.createOrReplaceTempView('employees_delta')


# COMMAND ----------

# MAGIC %sql
# MAGIC update employees_delta set salary=salary+20;
# MAGIC -- we can see snapshot happend and new file is written

# COMMAND ----------

display(employees_delta)

# COMMAND ----------

output_dir='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta/_delta_log/'
output_dir_2='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta/'

display(dbutils.fs.ls(output_dir))

# COMMAND ----------

from delta.tables import *
employees_delta_his = DeltaTable.forPath(spark, 'abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
employees_delta_his.update(
    set = {'salary':'salary-20'}
)

# we have done update using forPath... command only.. not able using the employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
# also we are not able to describe the table detail using above method hence have to use forPath method

# COMMAND ----------

employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
display(employees_delta)

# COMMAND ----------

# condition using SQL formatted string
deltaTable.update(
    condition = "eventType = 'clck'",
    set = { "eventType": "'click'" } )

# condition using Spark SQL functions
deltaTable.update(
    condition = col("eventType") == "clck", # this is like where clause 
    set = { "eventType": lit("click") } )

# COMMAND ----------

#https://asadatalake9mf5gvx.blob.core.windows.net/cdctest/raw_csv_files/emp_sal_update_1.csv

# COMMAND ----------

# MAGIC %md <i18n value="b5b346b8-a3df-45f2-88a7-8cf8dea6d815"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Using Merge
# MAGIC 
# MAGIC Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command.
# MAGIC 
# MAGIC Databricks uses the **`MERGE`** keyword to perform this operation.
# MAGIC 
# MAGIC Consider the following temporary view, which contains 4 records that might be output by a Change Data Capture (CDC) feed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Using the syntax we've seen so far, we could filter from this view by type to write 3 statements, one each to insert, update, and delete records. But this would result in 3 separate transactions; if any of these transactions were to fail, it might leave our data in an invalid state.
# MAGIC 
# MAGIC Instead, we combine these actions into a single atomic transaction, applying all 3 types of changes together.
# MAGIC 
# MAGIC **`MERGE`** statements must have at least one field to match on, and each **`WHEN MATCHED`** or **`WHEN NOT MATCHED`** clause can have any number of additional conditional statements.
# MAGIC 
# MAGIC Here, we match on our **`id`** field and then filter on the **`type`** field to appropriately update, delete, or insert our records.

# COMMAND ----------

#1 read the file which containe changed data. then using that do megre
emp_data_update = spark.read.format('csv').options(inferSchema="true",header="true").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp_sal_update_1.csv')
#2 change datatype of commission colum

from pyspark.sql.functions import col

data_type_change=(emp_data_update.withColumn('COMM',col('COMM').cast("int"))
                  )

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')

deltaTable.alias("target").merge(
       source=data_type_change.alias("source"),
       condition  = "source.EMPNO = target.EMPNO") \
      .whenMatchedUpdateAll()  \
      .whenNotMatchedInsertAll()\
      .execute()

# COMMAND ----------

display(data_type_change)

# COMMAND ----------

emp_data_updated = spark.read.format('delta').options(header="true").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
display(emp_data_updated)

# COMMAND ----------

fullHistoryDF = deltaTable.history()
display(fullHistoryDF)

# COMMAND ----------

# MAGIC %md ### Find changes between 2 versions of a table

# COMMAND ----------

df1 = spark.read.format("delta").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
df2 = spark.read.format("delta").option("versionAsOf",3).load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
#df1.exceptAll(df2).show()
display(df1.exceptAll(df2))

#since we have all the same row it is showing all rows


# COMMAND ----------

# Again doing merge operation 
from delta.tables import *
from pyspark.sql.functions import col

#1 read the file which containe changed data. then using that do megre
emp_data_new_file = spark.read.format('csv').options(inferSchema="true",header="true").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp_sal_update_1.csv')
#2 change datatype of commission colum from string to int
emp_data_update=(emp_data_new_file.withColumn('COMM',col('COMM').cast("int")))


#3 read the delta table 
deltaTable = DeltaTable.forPath(spark, 'abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')

#4 merger delta table 
deltaTable.alias("target").merge(
       source=emp_data_update.alias("source"),
       condition  = "source.EMPNO = target.EMPNO") \
      .whenMatchedUpdateAll()  \
      .whenNotMatchedInsertAll()\
      .execute()

# COMMAND ----------

df1 = spark.read.format("delta").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
df2 = spark.read.format("delta").option("versionAsOf",5).load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/demployees_delta')
#df1.exceptAll(df2).show()
display(df1.exceptAll(df2))
