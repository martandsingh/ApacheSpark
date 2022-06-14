# Databricks notebook source
# MAGIC %md
# MAGIC ### What is Data Profiling?
# MAGIC Data profiling is the process of examining, analyzing, and creating useful summaries of data. The process yields a high-level overview which aids in the discovery of data quality issues, risks, and overall trends. Data profiling produces critical insights into data that companies can then leverage to their advantage.
# MAGIC 
# MAGIC In this notebook, we will learn few methods to generate our data profile. There are many application available in the market which can help you with data profiling. My main motto of this notebook is to explain how can anyone perform data profiling without purchasing third-party softwares.
# MAGIC 
# MAGIC Also if you understand the correct concept, then you may design your own custom data quality checks which may not be available in any other softwares, As different organization has different business rules. No single software can cover all the requirement, so it is better to know some core concepts.
# MAGIC 
# MAGIC Data profling is a part of Data Quality Checks. If you want to know more about data quality, you can refer:
# MAGIC 
# MAGIC https://www.marketingevolution.com/marketing-essentials/data-quality
# MAGIC 
# MAGIC ![DQC](https://raw.githubusercontent.com/martandsingh/images/master/dqc.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Delta Table
# MAGIC Let's create a delta table to store our data profiling. We can create table using SQL command. You can write SQL code in the notebook using %sql keyword.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new database for DQC.
# MAGIC CREATE DATABASE IF NOT EXISTS DB_DQC;
# MAGIC USE DB_DQC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lets create a delta table to store our profiling data
# MAGIC CREATE OR REPLACE TABLE data_profiling
# MAGIC (
# MAGIC   dqc_name VARCHAR(100),
# MAGIC   stage VARCHAR(20),
# MAGIC   db_name VARCHAR(50),
# MAGIC   table_name VARCHAR(50),
# MAGIC   column_name VARCHAR(50),
# MAGIC   dqc_value VARCHAR(50),
# MAGIC   query VARCHAR(2000),
# MAGIC   description VARCHAR(100),
# MAGIC   created_on TIMESTAMP
# MAGIC  );

# COMMAND ----------

df = spark.read.parquet('/FileStore/datasets/USED_CAR_PARQUET/')
display(df.limit(4))

# COMMAND ----------

from pyspark.sql.functions import col, length
from datetime import datetime

# COMMAND ----------

# get list of column & its type
def get_column_types(df_source):
    dict_types= {}
    for column in  df.schema.fields:
        dict_types[column.name] = str(column.dataType)
    return dict_types
           
# Return dictionary with missing columns & count
def get_null_check(df_source):
    list_cols = df_source.columns
    dict_null = {}
    for column in list_cols:
        null_count=(df.filter(col(column).isNull()).count())
        dict_null[column] = null_count
    return dict_null

# get min value only for numeric columns
def get_min_val(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["longtype", "inttype", "decimaltype", "floattype"]:
            min_val = df_source.agg({column: "min"}).collect()[0]["min("+column+")"]
            data[column] = str(min_val)
        else:
            data[column]="NA"
    return data

# get average value only for numeric columns
def get_avg_val(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["longtype", "inttype", "decimaltype", "floattype"]:
            avg_val = df_source.agg({column: "avg"}).collect()[0]["avg("+column+")"]
            data[column] = str(avg_val)
        else:
            data[column]="NA"
    return data


# get max value only for numeric columns
def get_max_val(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["longtype", "inttype", "decimaltype", "floattype"]:
            max_val = df_source.agg({column: "max"}).collect()[0]["max("+column+")"]
            data[column] = str(max_val)
        else:
            data[column]="NA"
    return data

# get max length of the value in a string type column
def get_max_length_val(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["stringtype"]:
            df_len = df.withColumn("length", length(col(column))).select(column, "length")
            max_length = df_len.agg({"length":"max"}).collect()[0]["max(length)"]
            
            data[column] = str(max_length)
        else:
            data[column]="NA"
    return data


# get min length of the value in a string type column
def get_min_length_val(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["stringtype"]:
            df_len = df.withColumn("length", length(col(column))).select(column, "length")
            min_length = df_len.agg({"length":"min"}).collect()[0]["min(length)"]
            
            data[column] = str(min_length)
        else:
            data[column]="NA"
    return data

#get count of values matching for the given regex. In our case we are counting values with special characters.
def special_character_check(df_source):
    data = {}
    for column in df_source.columns:
        dtype = str(df.schema[column].dataType)
        #print(dtype)
        if dtype.lower() in ["stringtype"]:
            special_character_count = df.filter(col(column).rlike("[()`~/\!@#$%^&*()']")).count()
            data[column] = str(special_character_count)
        else:
            data[column]="NA"
    return data

#Pass the required parameter and it will log DQC stats to delta table.
def log_dqc_checks(dict_dqc, dqc_name, db_name, table_name, stage, query="", description=""):
    data =[]
    for key in dict_dqc:
        dict_result = {
          "dqc_name" : dqc_name,
          "stage" : stage ,
          "db_name" : db_name,
          "table_name" :table_name,
          "column_name": key,
          "dqc_value" : dict_dqc[key],
          "query" : query,
          "description": description,
          "created_on" : datetime.now()
        }
        data.append(dict_result)
    df = spark.createDataFrame(data)
    df_sort = df \
    .select("dqc_name", "stage", "db_name", "table_name", "column_name", "dqc_value", "query", "description", "created_on")
    df_sort.write.insertInto("DB_DQC.data_profiling", overwrite=False)
    

# COMMAND ----------

def dqc_pipeline(df_source):
    DB_NAME = "DB_DQC"
    TABLE_NAME = "data_profiling"
    STAGE = "PRE_ETL"
    
    print("Column type dqc in progress...")
    dict_types = get_column_types(df_source)
    log_dqc_checks(dict_types, "COLUMN_TYPE_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Column type dqc completed.")
    
    # Log NULL CHECK DQC
    print("Missing value dqc in progress...")
    dict_null = get_null_check(df_source)
    log_dqc_checks(dict_null, "NULL_COUNT_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Missing value dqc completed.")
    
    print("Minimum value dqc in progress...")
    dict_min = get_min_val(df_source)
    log_dqc_checks(dict_min, "MIN_VAL_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Minimum value dqc completed.")
    
    print("Maximum value dqc in progress...")
    dict_max = get_max_val(df_source)
    log_dqc_checks(dict_max, "MAX_VAL_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Maximum value dqc completed.")
    
    print("Average value dqc in progress...")
    dict_avg = get_avg_val(df_source)
    log_dqc_checks(dict_max, "AVG_VAL_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Average value dqc completed.")
    
    print("Maximum length dqc in progress...")
    dict_max_length = get_max_length_val(df_source)
    log_dqc_checks(dict_max_length, "MAX_LENGTH_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Maximum length dqc completed.")
    
    print("Minimum length dqc in progress...")
    dict_min_length = get_min_length_val(df_source)
    log_dqc_checks(dict_min_length, "MIN_LENGTH_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Minimum length dqc completed.")
    
    print("Special character dqc in progress...")
    dict_special_character = special_character_check(df_source)
    log_dqc_checks(dict_special_character, "SPECIAL_CHAR_DQC", DB_NAME, TABLE_NAME, STAGE)
    print("Special character dqc completed.")

# COMMAND ----------

dqc_pipeline(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_profiling ORDER BY dqc_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dqc_name, COUNT(dqc_value) FROM data_profiling WHERE (dqc_value) > 1 GROUP BY dqc_name 

# COMMAND ----------

# MAGIC %md
# MAGIC ### What we will do?
# MAGIC We are trying to profile our data which mean we are trying to record table statistic so that we can compare it later with post transformation data.
# MAGIC 
# MAGIC e.g. You have a unprocessed table, it has 5 columns & 100000 records. So we will calculate few basic statistics like total row count, total number of null values for each column, total distinct count, duplicate rows, datatype etc.
# MAGIC 
# MAGIC ### How it is useful?
# MAGIC Once you record these stats, then later we can calculate same statistic after cleaning data. In this way we can compare pre & post transformation stats to see how our data has changed during data pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE DB_DQC CASCADE;

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
