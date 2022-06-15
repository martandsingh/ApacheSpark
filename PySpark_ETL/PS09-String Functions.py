# Databricks notebook source
# MAGIC %md
# MAGIC ### String Functions
# MAGIC 
# MAGIC In this demo we will learn basic string functions which we use in our daily life projects.

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Length
# MAGIC * Case change & Initcap
# MAGIC * Replace
# MAGIC * Substring
# MAGIC * Concatenation (concat, concat_ws)
# MAGIC * Right/Left padding
# MAGIC * String split
# MAGIC * Trim
# MAGIC * Repeat

# COMMAND ----------

from pyspark.sql.functions import col, length, upper, lower, regexp_extract, regexp_replace, trim, repeat, substring, substring_index, concat_ws, concat, initcap, split, lpad, rpad

# COMMAND ----------

#Add new column cust_length,  the length of customer name 
df_trans = df.withColumn("cust_length", length(col("CustomerName")) )
display(df_trans)

# COMMAND ----------

#Add new column cust_upper & cust_lower which will contain customer name in upper & lower case respectively.
df_trans = df_trans \
        .withColumn("cust_upper", upper("CustomerName") )\
        .withColumn("cust_lower", lower("CustomerName") )\
        .withColumn("cust_initcap", initcap("cust_lower") )
display(df_trans)

# COMMAND ----------

#Add new column hidden_name which include customer name with "a" replaced as *.
df_trans = df_trans \
        .withColumn("hidden_name", regexp_replace(col("CustomerName"), "a", "*" ))
display(df_trans)

# COMMAND ----------

# get starting & last three characters of customer name
df_trans = (df_trans
        .withColumn("customer_name_start", substring(col("CustomerName"), 1, 3))
        .withColumn("customer_name_last", substring(col("CustomerName"), -3, 2))
       )
display(df_trans)

# COMMAND ----------

# Extract year from order date using split functions
df_trans = df_trans.withColumn("Year", split(col("Order Date"), "-")[2] ) 
display(df_trans)

# COMMAND ----------

# Extract year from order date using split functions
df_trans = df_trans.withColumn("repeat_date", repeat("Order Date", 2 ) )
display(df_trans)

# COMMAND ----------

df_trans = df_trans.withColumn("trim_name", trim("CustomerName") )
display(df_trans)

# COMMAND ----------

df_trans = df_trans\
            .withColumn("lpad_name", lpad("CustomerName", 20, "$") )\
            .withColumn("rpad_name", rpad("CustomerName", 20, "#") )
display(df_trans)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
