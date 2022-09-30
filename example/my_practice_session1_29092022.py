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

# COMMAND ----------

# MAGIC %md Manually define the schema by creating a `StructType` with column names and data types

# COMMAND ----------

# we can manullay define the schema by importing the function from pyspark sql library from pyspark.sql.types import LongType, StringType, StructType, StructField
from pyspark.sql.functions import * 
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType

userDefinedSchema_emptable = StructType([
    StructField("EMPNO", IntegerType(), True),
    StructField("EMPLOYEE NAME", StringType(), True),
    StructField("JOB", StringType(), True),
    StructField("MGR", StringType(), True),
    StructField("HIREDATE", StringType(), True),
    StructField("SALARY", IntegerType(), True),
    StructField("COMMISSION", IntegerType(), True),
    StructField("DEPARTMENT NO", IntegerType(), True)
])
#userDefinedSchema_emptable.printSchema() print schema doesn't work on dataframe


# COMMAND ----------

# MAGIC %md Read from CSV using this user-defined schema instead of inferring the schema
# MAGIC           

# COMMAND ----------

raw_file_path='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp.csv'
users_defined_schema = (spark
           .read
           .option("sep", ",")
           .option("header", True)
           .schema(userDefinedSchema_emptable)
           .csv(raw_file_path)
          )
users_defined_schema.printSchema()

# COMMAND ----------

# MAGIC %md Alternatively, define the schema using <a href="https://en.wikipedia.org/wiki/Data_definition_language" target="_blank">data definition language (DDL)</a> syntax.

# COMMAND ----------

#first we need to read the file and get its schema details, then create variable to copy the output 
#%scala
#spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL
#%scala
#spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

# COMMAND ----------

# MAGIC %md ## DataFrameWriter
# MAGIC Interface used to write a DataFrame to external storage systems
# MAGIC 
# MAGIC ```
# MAGIC (df.write                         
# MAGIC   .option("compression", "snappy")
# MAGIC   .mode("overwrite")      
# MAGIC   .parquet(outPath)       
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC DataFrameWriter is accessible through the SparkSession attribute `write`. This class includes methods to write DataFrames to different external storage systems.

# COMMAND ----------

output_dir='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA'

(users_defined_schema.write
    .option("compression","snappy")
    .mode("overwrite")
    .parquet(output_dir))

# COMMAND ----------

display(
    dbutils.fs.ls(output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES')
)

# COMMAND ----------

# writing delta table
spark.conf.set('spark.databricks.delta.defaults.columnMapping.mode','name')
output_dir='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA_DELTA'
raw_file_path='abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/raw_csv_files/emp.csv'
from pyspark.sql.functions import * 
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType

userDefinedSchema_emptable_a = StructType([
    StructField("EMPNO", IntegerType(), True),
    StructField("EMPLOYEE_NAME", StringType(), True),
    StructField("JOB", StringType(), True),
    StructField("MGR", StringType(), True),
    StructField("HIREDATE", StringType(), True),
    StructField("SALARY", IntegerType(), True),
    StructField("COMMISSION", IntegerType(), True),
    StructField("DEPARTMENT_NO", IntegerType(), True)
])
users_defined_schema = (spark
           .read
           .option("sep", ",")
           .option("header", True)
           .schema(userDefinedSchema_emptable_a)
           .csv(raw_file_path)
          )

users_defined_schema.write.format('delta').options(header=True,inferSchema=False).mode("overwrite").save(output_dir)

#got error Please enable column mapping by setting table property 'delta.columnMapping.mode' to 'name'.<< this happend due to columns having space and other special character in between 
#When column mapping is enabled for a Delta table, you can include spaces as well as any of these characters in the table’s column names: ,;{}()\n\t=.


# COMMAND ----------

display(
    dbutils.fs.ls('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA_DELTA')
)

# COMMAND ----------

employees_delta=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES')
employees_delta_via_schema=spark.read.format('delta').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA_DELTA')

employees_parquet=spark.read.format('parquet').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA')

# COMMAND ----------

employees_parquet.printSchema()  
## parquet tables column has space in between where as for employees_delta_via_schema delta we have add underscore to remove space

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC ##### Objectives
# MAGIC 1. Construct columns
# MAGIC 1. Subset columns
# MAGIC 1. Add or replace columns
# MAGIC 1. Subset rows
# MAGIC 1. Sort rows
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `select`, `selectExpr`, `drop`, `withColumn`, `withColumnRenamed`, `filter`, `distinct`, `limit`, `sort`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a>: `alias`, `isin`, `cast`, `isNotNull`, `desc`, operators
# MAGIC 
# MAGIC ## Column Expressions
# MAGIC 
# MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC 
# MAGIC Construct a new Column based on existing columns in a DataFrame

# COMMAND ----------

employees_parquet=spark.read.format('parquet').load('abfss://cdctest@asadatalake9mf5gvx.dfs.core.windows.net/python_delta/EMPLOYEES_TABLE_VIA_SCHEMA')
display(employees_parquet)

# COMMAND ----------

from pyspark.sql.functions import col


employees_parquet["EMPLOYEE NAME"]
col("EMPLOYEE NAME")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are `===` and `=!=`) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

#Create complex expressions with existing columns, operators, and methods.
from pyspark.sql.functions import *

cal_df = (employees_parquet
          .filter(col('COMMISSION').isNull())
          .withColumn('yearly_incentive', (col('SALARY') * 0.20).cast('int') )
          .withColumn('quaterly_incentive', (col('SALARY') * 0.05).cast('int')) 
          .withColumn('sal&comm', (col('SALARY') + when(col('COMMISSION').isNull(),0)).cast('int')) 
          #.withColumn('Total_sal', (col('SALARY') + nanvl(col('COMMISSION'),lit(0)).cast('int') + col('yearly_incentive')).cast('int'))
          .withColumn('Total_sal', (col('SALARY') + when(col('COMMISSION').isNull(),0).cast('int') + col('yearly_incentive')).cast('int'))
          .sort(col("SALARY").desc())
          #Column.when(col('COMMISSION'),0)
         )

display(cal_df)
# check how we have used the lit function & how we handled the null function for commission if not done then o/p would have been null << this doesnot work so we comment the line you can see next cell also to check this
# we had to use when function and isNull https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html >> pyspark.sql.Column.when >> pyspark.sql.Column.isNull¶


# COMMAND ----------

from pyspark.sql.functions import *

cal_df = (employees_parquet
          .filter(col('COMMISSION').isNull())
          .withColumn('yearly_incentive', (col('SALARY') * 0.20).cast('int') )
          .withColumn('quaterly_incentive', (col('SALARY') * 0.05).cast('int')) 
          .withColumn('sal&comm', (col('SALARY') + col('COMMISSION')).cast('int')) 
          .withColumn('Total_sal', (col('SALARY') + col('COMMISSION') + col('yearly_incentive')).cast('int'))
          .sort(col("SALARY").desc())
         )

display(cal_df)

# COMMAND ----------

# MAGIC %md ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | select | Returns a new DataFrame by computing given expression for each element |
# MAGIC | drop | Returns a new DataFrame with a column dropped |
# MAGIC | withColumnRenamed | Returns a new DataFrame with a column renamed |
# MAGIC | withColumn | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | filter, where | Filters rows using the given condition |
# MAGIC | sort, orderBy | Returns a new DataFrame sorted by the given expressions |
# MAGIC | dropDuplicates, distinct | Returns a new DataFrame with duplicate rows removed |
# MAGIC | limit | Returns a new DataFrame by taking the first n rows |
# MAGIC | groupBy | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

select_df = cal_df.select('empno','employee name','quaterly_incentive','salary','SAL&COMM','total_sal')
display(select_df)
#column name are case insensitive 

# COMMAND ----------

from pyspark.sql.functions import col

select_df_2 = cal_df.select(
                    col('empno').alias('employee id'),
                col('employee name').alias('emp_name')
        
            )
display(select_df_2)
# alias and col use case 

# COMMAND ----------

# MAGIC %md ### `selectExpr()`
# MAGIC Selects a list of SQL expressions

# COMMAND ----------

select_df_2 = employees_parquet.select(
                'EMPNO' as 
                )

cal_df = (employees_parquet
          .filter(col('COMMISSION').isNull())
          .withColumn('yearly_incentive', (col('SALARY') * 0.20).cast('int') )
          .withColumn('quaterly_incentive', (col('SALARY') * 0.05).cast('int')) 
          .withColumn('sal&comm', (col('SALARY') + when(col('COMMISSION').isNull(),0)).cast('int')) 
          #.withColumn('Total_sal', (col('SALARY') + nanvl(col('COMMISSION'),lit(0)).cast('int') + col('yearly_incentive')).cast('int'))
          .withColumn('Total_sal', (col('SALARY') + when(col('COMMISSION').isNull(),0).cast('int') + col('yearly_incentive')).cast('int'))
          .sort(col("SALARY").desc())
          #Column.when(col('COMMISSION'),0)
         )

EMPNO:integer
EMPLOYEE NAME:string
JOB:string
MGR:string
HIREDATE:string
SALARY:integer
COMMISSION:integer
DEPARTMENT NO:integer
