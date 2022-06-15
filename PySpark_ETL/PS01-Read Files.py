# Databricks notebook source
# MAGIC %md
# MAGIC ### Read CSV file using PySpark
# MAGIC 
# MAGIC ### What is SparkContext?
# MAGIC A SparkContext represents the connection to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster. It is Main entry point for Spark functionality.
# MAGIC 
# MAGIC *Note: Only one SparkContext should be active per JVM. You must stop() the active SparkContext before creating a new one.*
# MAGIC 
# MAGIC ### What is SparkSession?
# MAGIC SparkSession is the entry point to Spark SQL. It is one of the very first objects you create while developing a Spark SQL application.
# MAGIC 
# MAGIC As a Spark developer, you create a SparkSession using the SparkSession.builder method (that gives you access to Builder API that you use to configure the session).
# MAGIC 
# MAGIC 
# MAGIC ### Spark Context Vs Spark Session
# MAGIC SparkSession vs SparkContext – Since earlier versions of Spark or Pyspark, SparkContext (JavaSparkContext for Java) is an entry point to Spark programming with RDD and to connect to Spark Cluster, Since Spark 2.0 SparkSession has been introduced and became an entry point to start programming with DataFrame and Dataset.
# MAGIC 
# MAGIC 
# MAGIC By default, Databricks notebook provides a spark context object named "spark". This is the prebuild context object, we can use it directly.
# MAGIC ![PYSPARK_CSV](https://raw.githubusercontent.com/martandsingh/images/master/pyspark-read-csv.png)

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read CSV file
# MAGIC We will read a CSV file from our __DBFS (Databricks File Storage)__. I have upload my CSV file to FileStore/tables/cancer.csv
# MAGIC You can find all the dataset used in these tutorials at https://github.com/martandsingh/datasets.

# COMMAND ----------

# Import CSV file. For csv file, by default delimiter is comma so no need to mention in case of comma separated values. But for other delmiter you have to mention it.
df_csv = spark \
    .read \
    .option("encoding", "UTF-8") \
    .option("delmiter", ",") \
    .option("header", "True") \
    .csv("/FileStore/datasets/cancer.csv")

# COMMAND ----------

# display function will visualize your dataset in a beautiful way. This is databricks notebook function which will not work in your spark scripts.
display(df_csv)

# COMMAND ----------

df_csv.show() # prints top 20 records. It does not return anyting.

# COMMAND ----------

df_csv.show(2) # showing only top 2 rows with header.

# COMMAND ----------

li = df_csv.take(2) # this will return a list containing 2 rows
print(li)

# COMMAND ----------

# you can access the list
print(len(li))
print(li[0]["State"])

# COMMAND ----------

all_record = df_csv.collect() # return whole dataset. Do not use it until you need it. If you have very big dataset, this will take a huge time & computation to finish.
#print(all_record) # I am commenting the command, you may uncomment and run, but make sure you do not have a very big dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ JSON

# COMMAND ----------

df_json_sales = spark.read.option("multiline", "true").json("/FileStore/datasets/unece.json")

# COMMAND ----------

display(df_json_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we just loaded a JSON file to our spark dataframe. Let's try one more...

# COMMAND ----------

df_json = spark.read.option("multiline", "true").json("/FileStore/datasets/used_cars_nested.json")

# COMMAND ----------

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Oopss....
# MAGIC What kind of json is this? 
# MAGIC 
# MAGIC You must be thinking the same thing. But actually, this is the correct behaviour. If you will compare the structure of our Sales.json & used_cars_nested.json, you will see that later one is nested JSON with complicate structure & complicate means you have to do some more work to clean this json to convert it into a tabular form. In real life, you will not get a simple json like Sales.json, most of the time real life datasets are much complicate. 
# MAGIC 
# MAGIC *__So in our next notebook we will see how to deal with nested or complex json files.__*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Parquet File
# MAGIC ### What is Parquet?
# MAGIC Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Apache Parquet is designed to be a common interchange format for both batch and interactive workloads. It is similar to other columnar-storage file formats available in Hadoop, namely RCFile and ORC.
# MAGIC #### Characteristics of Parquet
# MAGIC 1. Free and open source file format.
# MAGIC 1. Language agnostic.
# MAGIC 1. Column-based format - files are organized by column, rather than by row, which saves storage space and speeds up analytics queries.
# MAGIC 1. Used for analytics (OLAP) use cases, typically in conjunction with traditional OLTP databases.
# MAGIC 1. Highly efficient data compression and decompression.
# MAGIC 1. Supports complex data types and advanced nested data structures.
# MAGIC 
# MAGIC #### Benefits of Parquet
# MAGIC 1. Good for storing big data of any kind (structured data tables, images, videos, documents).
# MAGIC 1. Saves on cloud storage space by using highly efficient column-wise compression, and flexible encoding schemes for columns with different data types.
# MAGIC 1. Increased data throughput and performance using techniques like data skipping, whereby queries that fetch specific column values need not read the entire row of data.

# COMMAND ----------

df_par = spark.read.parquet("/FileStore/datasets/USED_CAR_PARQUET/")
display(df_par)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we simply learnt how to read different format files. This was very basic notebook, so you may wondering is it worth creating a separate notebook for reading files? I would say "yes!!", as databricks is a vast technology growing frequently. So in future there me othere use cases which we can add in our "Read Files" notebook. So it is worth creating a notebook for it.
# MAGIC 
# MAGIC ### Assignment: 
# MAGIC 1. Try loading JSON (simple & nested). We will learn more about nested json in our next notebook.
# MAGIC 2. Try loading TSV(tab separated file) or any other flat file has a delimiter other than comma(',')

# COMMAND ----------


