# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### What are the joins?
# MAGIC PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames. In real life projects, you will be having more than one dataset or table (normalized data). To calculate or get final data sometime you may need to join different datasets.
# MAGIC 
# MAGIC There are many types of joins are available, but we will discuss only most common table join types:
# MAGIC 1. Inner join - returns record which are common in both the tables.
# MAGIC 1. Left outer join - common records + unmatched records from left table (left side of join clause)
# MAGIC 1. Right outer join - common records + unmatched records from right table
# MAGIC 1. Cross join - Cartesian product of two tables. It will create MxN records (M - no of records in left table, N - no of records in right side table).
# MAGIC 1. Full outer join - all the records from both the tables except the common records.
# MAGIC 
# MAGIC ![JOINS](https://raw.githubusercontent.com/martandsingh/images/master/joins.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Here we are creating SQL tables. Do not worry about the code. We have a separate tutorial for this SQL_Refresher. Check out our githb url: https://github.com/martandsingh/ApacheSpark
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS  T1
# MAGIC (
# MAGIC   id VARCHAR(10)
# MAGIC );
# MAGIC CREATE TABLE IF NOT EXISTS T2
# MAGIC (
# MAGIC   id VARCHAR(10)
# MAGIC );
# MAGIC CREATE TABLE IF NOT EXISTS T3
# MAGIC (
# MAGIC   id VARCHAR(10)
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO T1 VALUES ('1'), ('2'), ('3'), ('4'), (NULL);
# MAGIC INSERT INTO T2 VALUES ('1'), ('2');
# MAGIC INSERT INTO T3 VALUES ( '3'), ('4'), ('5'), (NULL);

# COMMAND ----------

# convert tables to dataframe
df_1 = spark.sql("SELECT * FROM T1");
df_2 = spark.sql("SELECT * FROM T2");
df_3 = spark.sql("SELECT * FROM T3");

# COMMAND ----------

display(df_1)
display(df_2)
display(df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### INNER JOIN

# COMMAND ----------

# INNER JOIN
# This join will give only matching records. It will return only the records which are present on both the table.
df_inner = df_1.join(df_2, df_1["id"]==df_2["id"], "inner")
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEFT OUTER JOIN

# COMMAND ----------

# LEFT JOIN
# It will return only the records which are present on both the table and all non-matching records from left table.
df_left = df_1.join(df_2, df_1["id"]==df_2["id"], "left")
display(df_left)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RIGHT OUTER JOIN

# COMMAND ----------

# RIGHT JOIN
# It will return only the records which are present on both the table and all non-matching records from right table.
df_right = df_1.join(df_3, df_1["id"]==df_3["id"], "right")
display(df_right)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FULL OUTER JOIN

# COMMAND ----------

# FULL OUTER JOIN
df_full = df_1.join(df_3, df_1["id"]==df_3["id"], "full")
display(df_full)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CROSS JOIN

# COMMAND ----------

# CROSS JOIN, it will return cartesian product of both the tables
df_cross = df_1.crossJoin(df_3)
display(df_cross)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example
# MAGIC 
# MAGIC Let's see an example using our sales order dataset.

# COMMAND ----------

# We will use sales dataset. We have three tables: order list, order details & sales target. Let's load data first. Order list and details are linked using order id. Order details & Sales target are linked with category.
df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
df_od = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderdetails.csv")
df_st = spark.read.option("header", "true").csv("/FileStore/datasets/sales/salestarget.csv")

# COMMAND ----------

display(df_ol.limit(3))
display(df_od.limit(3))
display(df_st.limit(3))

# COMMAND ----------

df_inner = df_ol \
        .join(df_od, df_ol["Order Id"] == df_od["Order Id"], "inner")\
        .select(df_ol["Order Id"], df_ol["Order Date"], df_od["Amount"], df_od["Profit"], df_od["Category"])
display(df_inner)

# COMMAND ----------

# left_outer, left as same
df_left_outer = df_ol \
        .join(df_od, df_ol["Order Id"] == df_od["Order Id"], "left_outer")\
        .select(df_ol["Order Id"], df_ol["Order Date"], df_od["Amount"], df_od["Profit"], df_od["Category"])
display(df_left_outer)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


