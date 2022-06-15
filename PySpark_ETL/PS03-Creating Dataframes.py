# Databricks notebook source
# MAGIC %md
# MAGIC ### Dataframe
# MAGIC 
# MAGIC A distributed collection of data grouped into named columns.
# MAGIC A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various functions in SparkSession.
# MAGIC 
# MAGIC __createDataFrame()__ and __toDF()__ methods are two different way’s to create DataFrame in spark. By using toDF() method, we don’t have the control over schema customization whereas in createDataFrame() method we have complete control over the schema customization. Use toDF() method only for local testing. But we can use createDataFrame() method for both local testings as well as for running the code in production.
# MAGIC 
# MAGIC __toDF()__ is used to convert rdd to dataframe.
# MAGIC 
# MAGIC 
# MAGIC ![DATAFRAME](https://raw.githubusercontent.com/martandsingh/images/master/dataframe.jpeg)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### createDataFrame()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# COMMAND ----------

# Dataframe using list of tuples. Here we can apply custom header
schema = StructType( [\
                 StructField("lang", StringType(),True),\
                 StructField("user", IntegerType(),True)\
])
data = [("English", 12413), ("Hindi", 455543)]
df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# CreateDataFrame using dictionary list

data_tuple = [{"lang":"English", "user": 10000}, {"lang":"Spanish", "user": 12452}]
df = spark.createDataFrame(data_tuple)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### toDF() 
# MAGIC This is used to convert rdd to dataframes.

# COMMAND ----------

#Lets create an rdd with a list
data = [("Football", 34566), ("Cricket", 2536), ("Baseball", 1234)]
rdd = sc.parallelize(data)
df_rdd =rdd.toDF()
display(df_rdd)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### using files
# MAGIC This type of dataframes we are going to see in our whole course. We will create dataframe using CSv, JSON & parquet files. but for this particular notebook demo I will use CSV source.

# COMMAND ----------

df_csv = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
display(df_csv)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
