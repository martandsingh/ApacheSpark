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
print(cancer_file)
print(unece_file)
print(used_car_file)


# COMMAND ----------



# COMMAND ----------

dbutils.fs.cp(dbutils.widgets.get("CANCER_CSV_PATH"), cancer_file)

dbutils.fs.cp(dbutils.widgets.get("UNECE_JSON_PATH"),  unece_file)

dbutils.fs.cp(dbutils.widgets.get("USED_CAR_JSON_PATH"), used_car_file)



# COMMAND ----------

parquet_path = dbutils.widgets.get("DBFS_PARQUET_FILE")
print("Writing parquet file to "+ parquet_path)
df = spark.read.option("header", "true").csv(cancer_file)
df.write.mode("overwrite").parquet(parquet_path)
print("Parquet file is read.")

# COMMAND ----------

print('File loaded to DBFS ' + dbutils.widgets.get("DBFS_DATASET_LOCATION"))
