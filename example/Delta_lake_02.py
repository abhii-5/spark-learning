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

display(dbutils.fs.ls(output_dir_2))

# COMMAND ----------


