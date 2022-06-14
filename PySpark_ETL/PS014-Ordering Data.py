# Databricks notebook source
# MAGIC %md
# MAGIC ### Ordering Data
# MAGIC The __ORDER BY__ keyword is used to sort the records in ascending order by default. To sort the records in descending order, use the DESC keyword

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
display(df_ol)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Sort in ascending order
df_cust_order = df_ol.filter(col("CustomerName").isNotNull()).orderBy(col("CustomerName"))
display(df_cust_order)

# COMMAND ----------

# Sort in descending order
df_cust_order = df_ol.filter(col("CustomerName").isNotNull()).orderBy(col("CustomerName").desc())
display(df_cust_order)

# COMMAND ----------

# Sort with more than one column - ascending order
df_sort = df_ol\
        .filter(col("CustomerName").isNotNull())\
        .orderBy([col("CustomerName"), col("State")], ascending=True)
display(df_sort)

# COMMAND ----------

# Sort with more than one column - descending order
df_sort = df_ol\
        .filter(col("CustomerName").isNotNull())\
        .orderBy([col("CustomerName"), col("State")], ascending=False)
display(df_sort)

# COMMAND ----------


# Sort in ascending order. If column has null value you can control where to show null values. by default null value will always be on top. 
df_cust_order = df_ol.orderBy(col("CustomerName"))
display(df_cust_order)

# COMMAND ----------

# You can choose where to place null values using  asc_nulls_last(place null at last), asc_nulls_first (place null at first)
df_cust_order = df_ol.orderBy(col("CustomerName").asc_nulls_last())
display(df_cust_order)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
