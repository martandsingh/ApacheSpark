# Databricks notebook source
#spark.conf.set("da", self.username)

# Defining datafile paths
CANCER_FILE_NAME='cancer.csv'
UNECE_FILE_NAME='unece.json'
USED_CAR_FILE_NAME='used_cars_nested.json'
DBFS_DATASET_LOCATION = '/FileStore/datasets/'
CANCER_CSV_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + CANCER_FILE_NAME # CSV
UNECE_JSON_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + UNECE_FILE_NAME # simple JSON
USED_CAR_JSON_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + USED_CAR_FILE_NAME # complex JSON
DBFS_PARQUET_FILE = '/FileStore/datasets/USED_CAR_PARQUET/'
DA = {
    "CANCER_FILE_NAME": CANCER_FILE_NAME,
    "UNECE_FILE_NAME": UNECE_FILE_NAME,
    "USED_CAR_FILE_NAME": USED_CAR_FILE_NAME,
    "CANCER_CSV_PATH": CANCER_CSV_PATH,
    "UNECE_JSON_PATH": UNECE_JSON_PATH,
    "USED_CAR_JSON_PATH": USED_CAR_JSON_PATH,
    "DBFS_DATASET_LOCATION": DBFS_DATASET_LOCATION,
    "DBFS_PARQUET_FILE": DBFS_PARQUET_FILE
}



# COMMAND ----------

print('Loading data files...')

# COMMAND ----------

dbutils.notebook.run(path="../SETUP/_pyspark_setup_files", timeout_seconds=60, arguments= DA)

# COMMAND ----------

print('Data files loaded.')
