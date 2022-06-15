# Databricks notebook source
# MAGIC %md
# MAGIC ### Caching - Optimize spark performance
# MAGIC Data caching is a very important technique when it comes to optimize your spark performance. Sometimes we have to reuse our big dataframe multiple times. It is not always prefered to load them frequently. SO what else we can do?
# MAGIC 
# MAGIC We can create a local or cached copy to quickly retreive data from it. In this case our original dataframe will not be used. We will be using the cached copy. There may be the cases, new updates are there in original dataframe but you still have stale or older version of data cached. We have to take care of it also.
# MAGIC 
# MAGIC ### Databricks provides two types of caching:
# MAGIC 1. Spark Caching
# MAGIC 1. Delta Caching
# MAGIC 
# MAGIC ### Delta and Apache Spark caching
# MAGIC 
# MAGIC The Delta cache accelerates data reads by creating copies of remote files in nodes’ local storage using a fast intermediate data format. The data is cached automatically whenever a file has to be fetched from a remote location. Successive reads of the same data are then performed locally, which results in significantly improved reading speed.
# MAGIC 
# MAGIC The Delta cache works for all Parquet files and is not limited to Delta Lake format files. The Delta cache supports reading Parquet files in Amazon S3, DBFS, HDFS, Azure Blob storage, Azure Data Lake Storage Gen1, and Azure Data Lake Storage Gen2. It does not support other storage formats such as CSV, JSON, and ORC.
# MAGIC 
# MAGIC Here are the characteristics of each type:
# MAGIC 
# MAGIC * __Type of stored data__: The Delta cache contains local copies of remote data. It can improve the performance of a wide range of queries, but cannot be used to store results of arbitrary subqueries. The Spark cache can store the result of any subquery data and data stored in formats other than Parquet (such as CSV, JSON, and ORC).
# MAGIC 
# MAGIC * __Performance__: The data stored in the Delta cache can be read and operated on faster than the data in the Spark cache. This is because the Delta cache uses efficient decompression algorithms and outputs data in the optimal format for further processing using whole-stage code generation.
# MAGIC 
# MAGIC * __Automatic vs manual control__: When the Delta cache is enabled, data that has to be fetched from a remote source is automatically added to the cache. This process is fully transparent and does not require any action. However, to preload data into the cache beforehand, you can use the CACHE SELECT command (see Cache a subset of the data). When you use the Spark cache, you must manually specify the tables and queries to cache.
# MAGIC 
# MAGIC * __Disk vs memory-based__: The Delta cache is stored on the local disk, so that memory is not taken away from other operations within Spark. Due to the high read speeds of modern SSDs, the Delta cache can be fully disk-resident without a negative impact on its performance. In contrast, the Spark cache uses memory.
# MAGIC 
# MAGIC *Attention: In this chapter  you may not able to see caching effect as our dataset is very small. To see a significant difference, your dataset must be big and must have complex processing. so this notebook is just to show you how we can use caching.*
# MAGIC 
# MAGIC __for more details visits:__
# MAGIC 
# MAGIC https://docs.databricks.com/delta/optimizations/delta-cache.html

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Caching
# MAGIC We have two methods to perform pyspark caching:
# MAGIC 1. cache()
# MAGIC 1. persist()
# MAGIC 
# MAGIC Both caching and persisting are used to save the Spark RDD, Dataframe, and Dataset’s. But, the difference is, RDD cache() method default saves it to memory (MEMORY_ONLY) whereas persist() method is used to store it to the user-defined storage level.
# MAGIC 
# MAGIC __Storage class__
# MAGIC 
# MAGIC class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)
# MAGIC 
# MAGIC Storage level in persist():
# MAGIC 
# MAGIC Now, to decide the storage of RDD, there are different storage levels, which are given below −
# MAGIC 
# MAGIC * DISK_ONLY = StorageLevel(True, False, False, False, 1)
# MAGIC * DISK_ONLY_2 = StorageLevel(True, False, False, False, 2)
# MAGIC * MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
# MAGIC * MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)
# MAGIC * MEMORY_AND_DISK_SER = StorageLevel(True, True, False, False, 1)
# MAGIC * MEMORY_AND_DISK_SER_2 = StorageLevel(True, True, False, False, 2)
# MAGIC * MEMORY_ONLY = StorageLevel(False, True, False, False, 1)
# MAGIC * MEMORY_ONLY_2 = StorageLevel(False, True, False, False, 2)
# MAGIC * MEMORY_ONLY_SER = StorageLevel(False, True, False, False, 1)
# MAGIC * MEMORY_ONLY_SER_2 = StorageLevel(False, True, False, False, 2)
# MAGIC * OFF_HEAP = StorageLevel(True, True, True, False, 1)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DecimalType, IntegerType

# COMMAND ----------

# We are using a game steam dataset.
custom_schema = StructType(
[
    StructField("gamer_id", IntegerType(), True),
    StructField("game", StringType(), True),
    StructField("behaviour", StringType(), True),
    StructField("play_hours", DecimalType(), True),
    StructField("rating", IntegerType(), True)
])
df = spark.read.option("header", "true").schema(custom_schema).csv('/FileStore/datasets/steam-200k.csv')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_pre_cache =  df\
            .groupBy("game", "behaviour")\
            .mean("play_hours")



# COMMAND ----------

display(df_pre_cache)

# COMMAND ----------

df_post_cache = df_pre_cache.cache()

# COMMAND ----------

display(df_post_cache)

# COMMAND ----------

# What will happen if we make changes in original dataset? Will it automaticlly update the cached copy. Let's drop a column in original datset and then compare both.

# step1: cache the original copy
df_cach = df.cache()
df = df.drop("gamer_id")


# COMMAND ----------

display(df.limit(2))
display(df_cach.limit(2))

# so you can see cached data does not gets updated. So always keep this in mind. If you have any changes in original dataframe then you have to delete the cached copy and create a new cache. Do not forget to delete the previous one as it will take extra storage. Always use caching for the dataset which does not gets updated frequently.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist()
# MAGIC Persist is like cache but in this case you can define custom storage level.

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
df_persist = df.persist( StorageLevel.MEMORY_AND_DISK_2)

# COMMAND ----------

display(df_persist)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Caching
# MAGIC Delta caching is stored as local file on worker node. The Delta cache automatically detects when data files are created or deleted and updates its content accordingly. You can write, modify, and delete table data with no need to explicitly invalidate cached data.
# MAGIC 
# MAGIC The Delta cache automatically detects files that have been modified or overwritten after being cached. Any stale entries are automatically invalidated and evicted from the cache.
# MAGIC 
# MAGIC You have to enable delta caching using:
# MAGIC 
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "[true | false]")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled") # it is default in my case. let's enable this

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# Lets create a delta table and see how delta caching works
df.write.format("delta").mode("overwrite").saveAsTable("default.gamestats")

# COMMAND ----------

# MAGIC %sql -- we have our table
# MAGIC SELECT * FROM default.gamestats
# MAGIC WHERE play_hours > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets cache above query using delta caching
# MAGIC CACHE
# MAGIC SELECT * FROM default.gamestats
# MAGIC WHERE play_hours > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After caching this query will take lesser time.
# MAGIC SELECT * FROM default.gamestats
# MAGIC WHERE play_hours > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now lets make some changes in our table and then rerun the same query. Will cache be able to catch new changes?
# MAGIC UPDATE default.gamestats
# MAGIC SET rating = CASE WHEN play_hours <5 THEN 2.5 
# MAGIC WHEN play_hours >=5 AND play_hours<10 THEN 3.5 
# MAGIC ELSE 4.8 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- so now we have updated rating column. Let;s see whether these new changes will be available in our cached copy?
# MAGIC SELECT * FROM default.gamestats LIMIT 10;
# MAGIC -- Yes, we can see updated changes in our cached query result.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.gamestats
# MAGIC WHERE play_hours > 10

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


