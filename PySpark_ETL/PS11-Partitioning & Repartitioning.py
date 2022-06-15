# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Partitioning
# MAGIC In the world of big data, partitioning is an extremely important concept. As name suggest, partitioning means dividing your data into smaller parts based on a partition key. You can also use multiple keys to partition your data.
# MAGIC 
# MAGIC we use partitionBy() to parition our data. Partition means when you choose a partition key, you data is divided into smaller parts based on that key & it will store your data into subfolders. example:
# MAGIC 
# MAGIC If you have 1 billion rows for 1000 users. Everytime when you use filter based on userid (WHERE userid = 'abc'), the executor will scan whole data. Let say you choose userid as partition key, it will divide or partition your data into 1000 sub folders(as we have 1000 unique users). Now the query (WHERE useri='abc') will scan only once folder which contains abc records.
# MAGIC 
# MAGIC ### How to choose a partition key?
# MAGIC 
# MAGIC Let's see a demo.
# MAGIC 
# MAGIC ![Partition](https://raw.githubusercontent.com/martandsingh/images/master/partitioning.png)
# MAGIC 
# MAGIC ### How to decide number of partitions in Spark?
# MAGIC In Spark, one should carefully choose the number of partitions depending on the cluster design and application requirements. The best technique to determine the number of spark partitions in an RDD is to multiply the number of cores in the cluster with the number of partitions.
# MAGIC 
# MAGIC ### How do I create a partition in Spark?
# MAGIC In Spark, you can create partitions in two ways -
# MAGIC 1. Repartition - used to increase and decrease the partitions. Results in more or less equal sized partitions. Since a full shuffle takes place, repartition is less performant than coalesce. Repartition always involves a shuffle.
# MAGIC 1. Coalesce - used to decrease the partition. It creates unequal partitions. Faster than repartition but query performance an be slower. Coalesce doesn’t involve a full shuffle.
# MAGIC 
# MAGIC By invoking partitionBy method on an RDD, you can provide an explicit partitioner,

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

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

df.count()

# COMMAND ----------

df.select("game").distinct().count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# We are using a game steam dataset. It will partition your data into default values of partitions which is 3 in my case you can check using df.rdd.getNumPartitions(). This process will be quicker as we do not have any partition key so spark does not have to sort and partition data based on key.

df.write.mode("overwrite").parquet("/FileStore/output/gamelogs_unpart")

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/output/gamelogs_unpart'))

# COMMAND ----------

# We are using a game steam dataset. This will create multiple folders based on game names. we have 5155 unique game, it will create 5155 folders. This process will take longer time to execute.

df.write.partitionBy("game").mode("overwrite").parquet("/FileStore/output/gamelogs_part")

# COMMAND ----------

df_files =  dbutils.fs.ls('/FileStore/output/gamelogs_part') 
type(df_files)

# COMMAND ----------

len(df_files) # So we can see we have 5156 (5155 for games, 1 for log)

# COMMAND ----------

# Lets read from our partition data
df_game = spark.read.parquet("/FileStore/output/gamelogs_part/")
display(df_game)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(df_game.filter( (col("game") == "Dota 2") & (col("behaviour") == "purchase") & (col("play_hours") == 1 )  ))

# COMMAND ----------

display(df.filter( (col("game") == "Dota 2") & (col("behaviour") == "purchase") & (col("play_hours") == 1 )  ))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Repartition

# COMMAND ----------

df_game.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

# COMMAND ----------

# Add a new column which include partition id. Count total number of record in each partition. It is recommended to have almost equal number of records in ech partition.

display( df_game.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().orderBy("count"))


# COMMAND ----------

# repartition() is used to increase or decrease the RDD, DataFrame, Dataset partitions
# If we want to reduce or increade the paritition size we can use it. 
df_repart = df_game.repartition(40)
display(df_repart.limit(10))

# COMMAND ----------

df_repart.rdd.getNumPartitions() #  40 partitions as we did

# COMMAND ----------

# we can also increase partition
df_repart2 = df_game.repartition(80)
display(df_repart2.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### coalesce()
# MAGIC It is only used to reduce the partition. This is optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce.

# COMMAND ----------

df_col2 = df_game.coalesce(10)
display(df_col2)

# COMMAND ----------

df_col.rdd.getNumPartitions()#  10 partitions as we did

# COMMAND ----------



# COMMAND ----------

# if you will try to increase the partition using coalesce, it will throw error.
df_col3 = df_col.coalesce(1000)
display(df_col3)

# COMMAND ----------

df_col3.rdd.getNumPartitions() #  332 partitions which is the original value. we cannot increase it

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
