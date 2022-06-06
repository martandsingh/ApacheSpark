# Databricks notebook source
# MAGIC %md
# MAGIC ### INTIAL SETUP
# MAGIC This will copy files from github repository to your DBFS. 
# MAGIC 
# MAGIC Repo: https://github.com/martandsingh/datasets
# MAGIC 
# MAGIC You can customize your DBFS location by changin DBFS_DATASET_LOCATION variable.

# COMMAND ----------

print('loading...')
DA = dbutils.widgets.get("CANCER_CSV_PATH")
print(DA)

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/datasets')

# COMMAND ----------

cancer_file= dbutils.widgets.get("DBFS_DATASET_LOCATION")+dbutils.widgets.get("CANCER_FILE_NAME")
unece_file = dbutils.widgets.get("DBFS_DATASET_LOCATION")+dbutils.widgets.get("UNECE_FILE_NAME")
used_car_file=dbutils.widgets.get("DBFS_DATASET_LOCATION")+dbutils.widgets.get("USED_CAR_FILE_NAME")
mall_customer_file=dbutils.widgets.get("DBFS_DATASET_LOCATION")+dbutils.widgets.get("MALL_CUSTOMER_FILE_NAME")
house_price_file=dbutils.widgets.get("DBFS_DATASET_LOCATION")+dbutils.widgets.get("HOUSE_PRICE_FILE")

print(cancer_file)
print(unece_file)
print(used_car_file)


# COMMAND ----------



# COMMAND ----------

dbutils.fs.cp(dbutils.widgets.get("CANCER_CSV_PATH"), cancer_file)

dbutils.fs.cp(dbutils.widgets.get("UNECE_JSON_PATH"),  unece_file)

dbutils.fs.cp(dbutils.widgets.get("USED_CAR_JSON_PATH"), used_car_file)

dbutils.fs.cp(dbutils.widgets.get("MALL_CUSTOMER_PATH"), mall_customer_file)

dbutils.fs.cp(dbutils.widgets.get("HOUSE_PRICE_PATH"), house_price_file)


# COMMAND ----------

from pyspark.sql.functions import explode, col

# COMMAND ----------

parquet_path = dbutils.widgets.get("DBFS_PARQUET_FILE")
print("Writing parquet file to "+ parquet_path)
df = spark \
    .read \
    .option("multiline", "true")\
    .json(used_car_file)

df_exploded = df \
            .withColumn("usedCars", explode(df["usedCars"]))

df_clean = df_exploded \
            .withColumn("vehicle_type", col("usedCars")["@type"])\
            .withColumn("body_type", col("usedCars")["bodyType"])\
            .withColumn("brand_name", col("usedCars")["brand"]["name"])\
            .withColumn("color", col("usedCars")["color"])\
            .withColumn("description", col("usedCars")["description"])\
            .withColumn("model", col("usedCars")["model"])\
            .withColumn("manufacturer", col("usedCars")["manufacturer"])\
            .withColumn("ad_title", col("usedCars")["name"])\
            .withColumn("currency", col("usedCars")["priceCurrency"])\
            .withColumn("seller_location", col("usedCars")["sellerLocation"])\
            .withColumn("displacement", col("usedCars")["vehicleEngine"]["engineDisplacement"])\
            .withColumn("transmission", col("usedCars")["vehicleTransmission"])\
            .withColumn("price", col("usedCars")["price"]) \
            .drop("usedCars")
df_clean.write.mode("overwrite").parquet(parquet_path)
print("Parquet file is read.")

# COMMAND ----------

print('File loaded to DBFS ' + dbutils.widgets.get("DBFS_DATASET_LOCATION"))
