# Databricks notebook source
# MAGIC %md
# MAGIC ### Write DataFrame
# MAGIC This is one of the most important but easy topic in ETL pipelines. write data frame is "L - Load" in ETL. After you transform your data, you need t write it to db or some location (datalake, dbfs). We can use write function to do so.
# MAGIC 
# MAGIC 
# MAGIC df.write.mode("overwrite/append/ignore").csv("/path/file/")
# MAGIC 
# MAGIC __We have three write modes:__
# MAGIC * __append__: Append content of the dataframe to existing data or table.
# MAGIC * __overwrite__: Overwrite existing data with the content of dataframe.
# MAGIC * __ignore__: Ignore current write operation if data / table already exists without any error
# MAGIC * __error or errorifexists__: (default case): Throw an exception if data already exists.

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
df_od = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderdetails.csv")

display(df_ol.limit(3))
display(df_od.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Problem
# MAGIC Let's say our business user wants us to get all the orders which are above $500 & category is Clothing in Maharashtra state.

# COMMAND ----------


df_mah = df_ol\
            .join(df_od, df_ol["Order ID"] == df_od["Order ID"], "inner")\
            .filter((df_od["Category"] == "Clothing") & (df_od["Amount"] > 500) & (df_ol["State"]=="Maharashtra"))\
            .withColumn("Amount", df_od["Amount"].cast("decimal(10, 2)"))\
            .withColumn("order_id", df_ol["Order ID"])\
            .select("order_id", "State", "City", "Category", "Amount")
display(df_mah)

# COMMAND ----------

df_mah.printSchema() # so you can see we have rename column & Amount type is changed to decimal(10, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as parquet files

# COMMAND ----------

# So now after doing this analysis & transformation we need to write this result to somewhere. For the sake of demo I will use delta lake to write the output.

# Write parquet files
df_mah.write.mode("overwrite").parquet("/FileStore/output/ClothingSalesMah_par/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as CSV

# COMMAND ----------


# Write CSV files
df_mah.write.mode("overwrite").csv("/FileStore/output/ClothingSalesMah_csv/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as JSON

# COMMAND ----------

# Write JSON files
df_mah.write.mode("overwrite").json("/FileStore/output/ClothingSalesMah_json/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as delta table (Managed)

# COMMAND ----------

# Write delta lake table. This will create a delta table in default db (you can choose any db). The table name is OrderSalesMah.
#df.write.format("delta").saveAsTable("default.people10m")
df_mah.write.format("delta").saveAsTable("default.OrderSalesMah")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- you can query above table
# MAGIC SELECT * FROM default.OrderSalesMah LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can observer type, location and owner. It tells us that this is a managed delta table.
# MAGIC DESCRIBE EXTENDED OrderSalesMah

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to delta lake

# COMMAND ----------

## Write to delta lake. It will save by default in parquet
df_mah.write.format("delta").mode("overwrite").save("/FileStore/output/ClothingSalesMah_delta/")

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
