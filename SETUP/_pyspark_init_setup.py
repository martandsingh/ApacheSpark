# Databricks notebook source
#spark.conf.set("da", self.username)

# Defining datafile paths
CANCER_FILE_NAME='cancer.csv'
UNECE_FILE_NAME='unece.json'
USED_CAR_FILE_NAME='used_cars_nested.json'
MALL_CUSTOMER_FILE_NAME = 'Mall_Customers.csv'
DBFS_DATASET_LOCATION = '/FileStore/datasets/'
CANCER_CSV_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + CANCER_FILE_NAME # CSV
UNECE_JSON_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + UNECE_FILE_NAME # simple JSON
USED_CAR_JSON_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + USED_CAR_FILE_NAME # complex JSON
MALL_CUSTOMER_PATH='https://raw.githubusercontent.com/martandsingh/datasets/master/' + MALL_CUSTOMER_FILE_NAME
DBFS_PARQUET_FILE = '/FileStore/datasets/USED_CAR_PARQUET/'
HOUSE_PRICE_FILE = 'missing_val_dataset.csv'
HOUSE_PRICE_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + HOUSE_PRICE_FILE
GAME_STREAM_FILE = 'steam-200k.csv'
GAME_STREAM_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/' + GAME_STREAM_FILE
ORDER_DETAIL_FILE = 'orderdetails.csv'
ORDER_DETAIL_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/Sales-Order/'+ORDER_DETAIL_FILE
ORDER_LIST_FILE = 'orderlist.csv'
ORDER_LIST_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/Sales-Order/' + ORDER_LIST_FILE
SALES_TARGET_FILE = 'salestarget.csv'
SALES_TARGET_PATH = 'https://raw.githubusercontent.com/martandsingh/datasets/master/Sales-Order/'+SALES_TARGET_FILE
DA = {
    "ORDER_DETAIL_FILE": ORDER_DETAIL_FILE,
    "ORDER_DETAIL_PATH": ORDER_DETAIL_PATH,
    "ORDER_LIST_FILE":ORDER_LIST_FILE,
    "ORDER_LIST_PATH":ORDER_LIST_PATH,
    "SALES_TARGET_FILE": SALES_TARGET_FILE,
    "SALES_TARGET_PATH": SALES_TARGET_PATH,
    "CANCER_FILE_NAME": CANCER_FILE_NAME,
    "UNECE_FILE_NAME": UNECE_FILE_NAME,
    "USED_CAR_FILE_NAME": USED_CAR_FILE_NAME,
    "CANCER_CSV_PATH": CANCER_CSV_PATH,
    "UNECE_JSON_PATH": UNECE_JSON_PATH,
    "USED_CAR_JSON_PATH": USED_CAR_JSON_PATH,
    "DBFS_DATASET_LOCATION": DBFS_DATASET_LOCATION,
    "DBFS_PARQUET_FILE": DBFS_PARQUET_FILE,
    "MALL_CUSTOMER_FILE_NAME": MALL_CUSTOMER_FILE_NAME,
    "MALL_CUSTOMER_PATH": MALL_CUSTOMER_PATH,
    "HOUSE_PRICE_FILE": HOUSE_PRICE_FILE,
    "HOUSE_PRICE_PATH": HOUSE_PRICE_PATH,
    "GAME_STREAM_FILE": GAME_STREAM_FILE,
    "GAME_STREAM_PATH": GAME_STREAM_PATH
}



# COMMAND ----------

print('Loading data files...')

# COMMAND ----------

dbutils.notebook.run(path="../SETUP/_pyspark_setup_files", timeout_seconds=60, arguments= DA)

# COMMAND ----------

print('Data files loaded.')
