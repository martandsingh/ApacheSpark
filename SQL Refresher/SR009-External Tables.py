# Databricks notebook source
# MAGIC %md
# MAGIC ### What is external table?
# MAGIC In a typical table, the data is stored in the database; however, in an external table, the data is stored in files in an external stage. External tables store file-level metadata about the data files, such as the filename, a version identifier and related properties. In external table, the data is not stored in database system, it is saved somewhere else in external location. These tables are slow to load as data is loaded from external source every time your run query on it. 
# MAGIC 
# MAGIC SYNTAX:
# MAGIC We use "external" keyword to create an external table.
# MAGIC 
# MAGIC Some characterstics:
# MAGIC 1. Data in external storage
# MAGIC 1. Slower to load
# MAGIC 1. Dropping the table will delete only table meta data. Your external data will be safe.
# MAGIC 1. It give you latest result from the file. If you delete external file then your query will throw exception.
# MAGIC 
# MAGIC Use case:
# MAGIC 1. As a data engineer, I use external table when I want to perform some adhoc analysis on some external file which we use infrequently.
# MAGIC 1. Sometimes you have some data in csv or flat files which your business user provides you & it frequently gets updated (my personal experience), in that case you can use external table(considering file size is small) to load that data, so that everytime you get latest result.
# MAGIC 
# MAGIC ![External_Table](https://raw.githubusercontent.com/martandsingh/images/master/external.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql -- here we are creating an external table from CSV file which is stored in my databricks storage. below command may not run in your system as you may not have same file at the same location. So you can upload a CSV file (in my case it was tab delimited) in your databricks storage and update the path and columns accordingly. th csv file which I used is available at : https://raw.githubusercontent.com/martandsingh/datasets/master/bank-full.csv
# MAGIC DROP TABLE IF EXISTS bank_report;
# MAGIC CREATE EXTERNAL TABLE bank_report (
# MAGIC   age STRING,
# MAGIC   job STRING,
# MAGIC   marital STRING,
# MAGIC   education STRING,
# MAGIC   default STRING,
# MAGIC   balance STRING,
# MAGIC   housing STRING,
# MAGIC   loan STRING,
# MAGIC   contact STRING,
# MAGIC   day STRING,
# MAGIC   month STRING,
# MAGIC   duration STRING,
# MAGIC   campaign STRING,
# MAGIC   pdays STRING,
# MAGIC   previous STRING,
# MAGIC   poutcome STRING,
# MAGIC   y STRING
# MAGIC ) USING CSV OPTIONS (
# MAGIC   path "/FileStore/tables/dataset/*.csv",
# MAGIC   delimiter ";",
# MAGIC   header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql -- now let's select top 100 records from our external table
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bank_report
# MAGIC LIMIT
# MAGIC   100;

# COMMAND ----------

# MAGIC %sql -- We can also create external table without defining schema.
# MAGIC DROP TABLE IF EXISTS bank_report_nc;
# MAGIC CREATE EXTERNAL TABLE bank_report_nc USING CSV OPTIONS (
# MAGIC   path "/FileStore/tables/dataset/*.csv",
# MAGIC   delimiter ";",
# MAGIC   header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_report_nc LIMIT 10;

# COMMAND ----------

# MAGIC %sql -- creating an external table in delta location. We are getting a subset of our external table bank_report and saving the output to a new delta table bank_report_del.
# MAGIC DROP TABLE IF EXISTS bank_report_del;
# MAGIC CREATE TABLE bank_report_del USING DELTA AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bank_report
# MAGIC WHERE
# MAGIC   balance > 1500

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_report_del

# COMMAND ----------

# DO NOT WORRY ABOUT THIS CODE. I am using this code to save parquet file in your dbfs, for the external table demo using parquet file in next cell. We will undertand these code in future. For now, just run this cell.
df = spark.sql("SELECT * FROM bank_report WHERE balance > 1500")
#display(df)

df.write.format("delta").mode("overwrite").save("/delta/bank_users_1500")

# COMMAND ----------

# You can check the files using dbutils
display(dbutils.fs.ls('/delta/bank_users_1500'))

# COMMAND ----------

# MAGIC %sql --We are creating an external table using delta location. We have saved parquet file in the given delta location.
# MAGIC DROP TABLE IF EXISTS bank_report_parq;
# MAGIC CREATE EXTERNAL TABLE bank_report_parq USING DELTA LOCATION "/delta/bank_users_1500/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_report_parq

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


