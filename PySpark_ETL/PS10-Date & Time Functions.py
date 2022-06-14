# Databricks notebook source
# MAGIC %md
# MAGIC ### Date Time Functions
# MAGIC Date is an important data type when it comes to reporting. In most of the time, reports are arranged based on date, month, year or a particular amount of time. To answer all those questions we will see how can we use date & time in pyspark.
# MAGIC 
# MAGIC ![DATETIME](https://raw.githubusercontent.com/martandsingh/images/master/datetime.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Current data time
# MAGIC * Convert string to date
# MAGIC * Extract date part e.g. day, month, year, week
# MAGIC * convert to/from unixtimestamp
# MAGIC * Change date format
# MAGIC * Date difference
# MAGIC * Add/Subtract date

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp, to_date, col, dayofmonth, month, year, quarter, date_add, date_sub, date_trunc, add_months, months_between, datediff

# COMMAND ----------

df_trans = df \
        .withColumn("current_date", current_date())\
        .withColumn("current_timestamp", current_timestamp())

display(df_trans)

# COMMAND ----------

# We can see order date is string type.
df.printSchema()

# COMMAND ----------

# Let's convert Order Date column to date
df_trans = df_trans.withColumn("order_date", to_date(col("Order Date"), "dd-MM-yyyy"))
display(df_trans)

# COMMAND ----------

df_trans.printSchema()

# COMMAND ----------

# Add new column Day, Month & Year from order_date column
df_trans = df_trans\
        .withColumn("Day", dayofmonth("order_date") )\
.withColumn("Month", month("order_date") )\
.withColumn("Year", year("order_date") )\
.withColumn("Quarter", quarter("order_date") )

display(df_trans)

# COMMAND ----------

# Add and Subtract days in order day
df_trans = df_trans\
        .withColumn("order_next_10_days", date_add(col("order_date"), 10))\
        .withColumn("order_prev_10_days", date_sub(col("order_date"), 10))\
        .withColumn("order_add_months", add_months(col("order_date"), 2))\
        .withColumn("date_dff", datediff(col("order_next_10_days"), col("order_date")) )


display(df_trans)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
