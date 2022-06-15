# Databricks notebook source
# MAGIC %md
# MAGIC ### User defined functions (UDF)
# MAGIC UDF will allow us to apply the functions directly in the dataframes and SQL databases in python, without making them registering individually. It can also help us to create new columns to our dataframe, by applying a function via UDF to the dataframe column(s), hence it will extend our functionality of dataframe. It can be created using the udf() method.

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a UDF to extract order number from order id column. for example: In B-25601, 25601 is the order id.

# COMMAND ----------

# Step1: first create a python function
def extract_order_no(order_id):
    if order_id != None and '-' in order_id:
        return order_id.split("-")[1]
    else:
        return 'NA'


# COMMAND ----------

print(extract_order_no("B-25601"))
print(extract_order_no("25601"))

# COMMAND ----------

# Step 2: convert python function to udf
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
extract_order_no = udf(extract_order_no)

# COMMAND ----------

df_trans = df.withColumn("order_number", extract_order_no(col("Order ID")) ) 
display(df_trans)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
