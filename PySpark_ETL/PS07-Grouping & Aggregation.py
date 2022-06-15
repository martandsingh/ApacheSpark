# Databricks notebook source
# MAGIC %md
# MAGIC ### Group  & Aggregation
# MAGIC Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical data into groups on DataFrame and perform aggregate functions on the grouped data.
# MAGIC 
# MAGIC PYSPARK AGG is an aggregate function that is functionality provided in PySpark that is used for operations. The aggregate operation operates on the data frame of a PySpark and generates the result for the same. It operates on a group of rows and the return value is then calculated back for every group.
# MAGIC 
# MAGIC ![GROUPING](https://raw.githubusercontent.com/martandsingh/images/master/grouping.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df = spark.read.parquet('/FileStore/datasets/USED_CAR_PARQUET/')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Group by body type. Query will writted body_type and total count for that particular body type
df_type = df.groupBy("body_type").count().orderBy(col("count").desc())
display(df_type)

# COMMAND ----------

# Grouping using multiple columns. Below query will group your data with body_type & brand_name. e.g. How many suzuki hatchback are there?
display(df.groupBy("brand_name", "body_type" ).count().orderBy(col("count").desc()))

# COMMAND ----------

# Average price of each brand name. Get the average price for each brand name
df_avgPrice = df.groupBy("brand_name").mean("price").orderBy("avg(price)")
display(df_avgPrice)

# with this analysis we can see Daewoo, Suzuki is relatively cheaper car & MG, Haval, Kia are expensive cars

# COMMAND ----------

# Now lets check does body type affect the price of car, what kind of body types are cheaper or expensive.
df_body_price = df.groupBy("brand_name", "body_type").mean("price").orderBy(col("avg(price)").desc())
display(df_body_price)

# so toyota SUV are generally expensive, daewoo & suzuki sedan are cheaper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agg function

# COMMAND ----------

df_agg = df.agg({"brand_name":"count", "body_type":"count", "price": "avg"})
display(df_agg)

# COMMAND ----------

df_agg = df.agg({"brand_name":"count", "body_type":"count", "price": "avg"}) \
        .withColumnRenamed("avg(price)", "avg_price") \
        .withColumnRenamed("count(body_type)", "total_types")\
        .withColumnRenamed("count(brand_name)", "total_brands")
display(df_agg)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
