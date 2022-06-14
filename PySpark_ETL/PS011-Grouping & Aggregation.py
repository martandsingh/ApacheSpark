# Databricks notebook source


# COMMAND ----------

df = spark.read.parquet('/FileStore/datasets/USED_CAR_PARQUET/')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Group by body type
df_type = df.groupBy("body_type").count().orderBy(col("count").desc())
display(df_type)

# COMMAND ----------

# Grouping using multiple columns
display(df.groupBy("body_type", "brand_name").count().orderBy(col("count").desc()))

# COMMAND ----------

# Average price of each brand name
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


