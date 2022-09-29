# Databricks notebook source
# MAGIC %md  connection to ADLS Gen2 storage

# COMMAND ----------

spark.conf.set("fs.azure.account.key.asadatalake9mf5gvx.dfs.core.windows.net",dbutils.secrets.get(scope="AccessKeys", key="landingstorage-asadatalake"))

# COMMAND ----------

# MAGIC %md read a csv file and create dataframe and display the data

# COMMAND ----------

emp_df = spark.read.format('csv').options(inferSchema="true",header="true").load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp.csv')

display(emp_df)


# COMMAND ----------

# MAGIC %md Create Delta table using the dataframe 

# COMMAND ----------

emp_df.write.format("delta").mode("overwrite").save("abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES")

# COMMAND ----------

# MAGIC %md
# MAGIC `createOrReplaceTempView` creates a temporary view based on the DataFrame. The lifetime of the temporary view is tied to the SparkSession that was used to create the DataFrame.

# COMMAND ----------

employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES')
employees_delta.createOrReplaceTempView('employees_delta_a')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees_delta_a;
# MAGIC -- we have use magic command %sql to read the dataframe.. same need to done using pyspark

# COMMAND ----------

display(spark.table('employees_delta')) 
##one method to read table using pyspark api 

# COMMAND ----------

## we can save above query in a variable and run display command also check here we used data frame not view

table = spark.table('employees_delta')
display(table)

# COMMAND ----------

# As per my knowlege we can run some basic file on dataframe also 
table = spark.table('employees_delta')\
.select('EMPNO','ENAME','SAL')\
.where('COMM is not null')\
.orderBy('SAL')
table.collect()

#collect returns an array of all rows in a DataFrame

# COMMAND ----------

#directly using dataframe 
#from pyspark.sql.functions import *
employees_delta.select('empno','ename','sal','comm')\
.filter(employees_delta['sal'] >1300)\
.orderBy(desc('empno'),'sal').show()
#.orderBy(employees_delta['sal'].desc()).show()


#'>' not supported between instances of 'str' and 'int'
#name 'desc' is not defined << to use desc we have to import sql.function >> oderBy(desc('sal'), ename)
# .orderBy(employees_delta['sal'].desc()).show() if we don't want to import the from pyspark.sql.functions import *

#.show() the show action causes the following cell to execute transformations.


# COMMAND ----------

table.display()
# we can see the difference between collect and display 

# COMMAND ----------

# MAGIC %md The **schema** defines the column names and types of a dataframe.
# MAGIC 
# MAGIC Access a dataframe's schema using the `schema` attribute.

# COMMAND ----------

# variable table is also dataframe which is derived from employees_delta
table.schema
#Out[47]: StructType(List(StructField(EMPNO,IntegerType,true),StructField(ENAME,StringType,true),StructField(SAL,IntegerType,true)))
table.printSchema()

# COMMAND ----------

# MAGIC %md View a nicer output for this schema using the `printSchema()` method.

# COMMAND ----------

#same way we check the actual schema of the delta table also.
employees_delta.schema
#Out[48]: StructType(List(StructField(EMPNO,IntegerType,true),StructField(ENAME,StringType,true),StructField(JOB,StringType,true),StructField(MGR,StringType,true),StructField(HIREDATE,StringType,true),StructField(SAL,IntegerType,true),StructField(COMM,StringType,true),StructField(DEPTNO,IntegerType,true)))

employees_delta.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## SparkSession
# MAGIC The `SparkSession` class is the single entry point to all functionality in Spark using the DataFrame API. 
# MAGIC 
# MAGIC In Databricks notebooks, the SparkSession is created for you, stored in a variable called `spark`.
# MAGIC 
# MAGIC Below are several additional methods we can use to create DataFrames. All of these can be found in the <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">documentation</a> for `SparkSession`.
# MAGIC 
# MAGIC #### `SparkSession` Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | sql | Returns a DataFrame representing the result of the given query | 
# MAGIC | table | Returns the specified table as a DataFrame |
# MAGIC | read | Returns a DataFrameReader that can be used to read data in as a DataFrame |
# MAGIC | range | Create a DataFrame with a column containing elements in a range from start to end (exclusive) with step value and number of partitions |
# MAGIC | createDataFrame | Creates a DataFrame from a list of tuples, primarily used for testing |

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md ## Actions
# MAGIC Conversely, DataFrame actions are methods that **trigger computation**.  
# MAGIC Actions are needed to trigger the execution of any DataFrame transformations. 
# MAGIC 
# MAGIC The `show` action causes the following cell to execute transformations.
# MAGIC 
# MAGIC %md Below are several examples of <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a> actions.
# MAGIC 
# MAGIC ### DataFrame Action Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | show | Displays the top n rows of DataFrame in a tabular form |
# MAGIC | count | Returns the number of rows in the DataFrame |
# MAGIC | describe,  summary | Computes basic statistics for numeric and string columns |
# MAGIC | first, head | Returns the the first row |
# MAGIC | collect | Returns an array that contains all rows in this DataFrame |
# MAGIC | take | Returns an array of the first n rows in the DataFrame |
