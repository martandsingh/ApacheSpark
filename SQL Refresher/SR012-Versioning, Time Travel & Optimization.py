# Databricks notebook source
# MAGIC %md
# MAGIC ### Versioning, Time Travel & Optimization
# MAGIC 
# MAGIC ### OPTIMIZE
# MAGIC Delta Lake on Databricks can improve the speed of read queries from a table. One way to improve this speed is to coalesce small files into larger ones. You trigger compaction by running the OPTIMIZE command
# MAGIC 
# MAGIC ### Z-ORDER
# MAGIC Data Skipping is a performance optimization that aims at speeding up queries that contain filters (WHERE clauses).
# MAGIC 
# MAGIC As new data is inserted into a Databricks Delta table, file-level min/max statistics are collected for all columns (including nested ones) of supported types. Then, when there’s a lookup query against the table, Databricks Delta first consults these statistics to determine which files can safely be skipped.  This is done automatically and no specific commands are required to be run for this.
# MAGIC 
# MAGIC * Z-Ordering is a technique to co-locate related information in the same set of files.
# MAGIC * Z-Ordering maps multidimensional data to one dimension while preserving the locality of the data points.
# MAGIC 
# MAGIC 
# MAGIC ### Z-Order Vs Partition
# MAGIC Partitioning physically splits the data into different files/directories having only one specific value, while ZOrder provides clustering of related data inside the files that may contain multiple possible values for given column.
# MAGIC 
# MAGIC Partitioning is useful when you have a low cardinality column - when there are not so many different possible values - for example, you can easily partition by year & month (maybe by day), but if you partition in addition by hour, then you'll have too many partitions with too many files, and it will lead to big performance problems.
# MAGIC 
# MAGIC ZOrder allows to create bigger files that are more efficient to read compared to many small files.

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DecimalType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

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

df.write.format("delta").saveAsTable("DB_DEMO.game_stats")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED DB_DEMO.game_stats

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/db_demo.db/game_stats"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check num of files before optimize. The moment I run this command it shows 3 as number of files.
# MAGIC DESCRIBE DETAIL db_demo.game_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE combines smaller files and create bigger file.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let;s optimize the table and then see the number of files
# MAGIC OPTIMIZE db_demo.game_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check num of files after optimize. After optimizing it is showing 1 file.
# MAGIC DESCRIBE DETAIL db_demo.game_stats

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/db_demo.db/game_stats"))

# COMMAND ----------

# _delta_log file contains all the transactions files
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/db_demo.db/game_stats/_delta_log/"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM json.`dbfs:/user/hive/warehouse/db_demo.db/game_stats/_delta_log/00000000000000000001.json`"))

# this was the last transaction which is our optimize command. You will see add column which includes new files added, in our case there is only one new file but in delete columns you can see 3 files deleted. We saw same result using DESCRIBE DETAIL employee command earlier.

# COMMAND ----------

display(spark.read.format("delta").load('dbfs:/user/hive/warehouse/db_demo.db/game_stats/'))

# COMMAND ----------

df.write.format("delta").saveAsTable("DB_DEMO.game_stats_new")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-ORDER
# MAGIC It will keep simillary data closer to optimize query.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE db_demo.game_stats_new
# MAGIC ZORDER BY (game)

# COMMAND ----------

display(spark.read.format("delta").load('dbfs:/user/hive/warehouse/db_demo.db/game_stats_new/'))
# if you compare this result with above game_stats result, you will see here data of same types are grouped because we applied ZORDER based on game colmn. It is keeping data of same game together.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Versioning & Timetravel

# COMMAND ----------

# MAGIC %md
# MAGIC Lets update table, each transation will create a new version of table. Does that mean we can still access the old version of data? 
# MAGIC 
# MAGIC Let's try it out.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE DB_DEMO.game_stats
# MAGIC SET rating = CASE WHEN play_hours <10 THEN 3 ELSE 4.5 END

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DB_DEMO.game_stats
# MAGIC -- IN our case we have three versions. Let's access the version befor update. that  mean version 1 as version2 is the latest version which was created by our update command.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- IN this version we should see our rating as 0
# MAGIC SELECT * FROM DB_DEMO.game_stats VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- You can use timestamp also to access older version of data
# MAGIC SELECT * FROM DB_DEMO.game_stats TIMESTAMP AS OF "2022-06-17 07:40:14.000+0000"

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- This will give you the latest version with non-zero ratings 
# MAGIC SELECT * FROM DB_DEMO.game_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROLLBACK
# MAGIC Let's say you update or delete your table by mistake & now you want to rollback to the previous version. Is it possible? 
# MAGIC Lets try it out.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's delete our data from game_stats table
# MAGIC DELETE FROM DB_DEMO.game_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now we can see there are no records
# MAGIC SELECT * FROM DB_DEMO.game_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DB_DEMO.game_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC -- remember version 2 was our version created after we updated the rating . 3rd version is created by delete command. So we want to restore before our delete.
# MAGIC RESTORE TABLE DB_DEMO.game_stats TO VERSION AS OF 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TADA!! We have restored our data.
# MAGIC  SELECT * FROM DB_DEMO.game_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ### Purging data using VACUUM
# MAGIC Delta lake versioning is very helpful to restore or access old data but it is not possible to keep all the versions of big data. It will be very expensive if you have GBs or TBs of data. So we should purge or remove outdated file. VACUUME command helps us to acheive it.
# MAGIC 
# MAGIC __This will delete data files permanently.__

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This will give you error, because it is not a best practice to delete all your old versions. below command woll delete all the old versions because we are using 0 hours. DEFAULT is 168 hours (7 days)
# MAGIC 
# MAGIC -- UNCOMMENT BELOW COMMAND AND RUN
# MAGIC --VACUUM  DB_DEMO.game_stats retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC --# If you want to use 0 hourse as retention period, in that case you have to change spark configuration which checks retention duration.
# MAGIC 
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled=true;

# COMMAND ----------

# MAGIC %md
# MAGIC You can use DRY RUN version of VACUUM to print out all the records to be deleted. it will not delete the files.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM  DB_DEMO.game_stats retain 1 HOURS DRY RUN
# MAGIC -- below versions will be deleted if you run this query.

# COMMAND ----------

# Before deleting the data files let;s check the folder
display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db_demo.db/game_stats/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM  DB_DEMO.game_stats retain 1 HOURS 

# COMMAND ----------

# check the folder after purging. It deleted the versions older than 1 hour
display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db_demo.db/game_stats/'))

# COMMAND ----------

# MAGIC %md
# MAGIC __Sometimes you can still query the older versions because of caching, so it is always better to restart your cluster.__

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


