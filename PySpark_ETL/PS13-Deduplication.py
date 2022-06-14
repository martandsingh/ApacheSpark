# Databricks notebook source
# MAGIC %md
# MAGIC ### Deduplication
# MAGIC Duplicate rows are a big issue in the world of big data. It can not only affect you analysis but also takes extra storage which in result cost you more.
# MAGIC 
# MAGIC __Deduplication__ is the process of removing duplicate data from your dataset.
# MAGIC 
# MAGIC So it is important to find out & cure duplicate rows. There may be case where you want to drop duplicate data (but not always). 
# MAGIC 
# MAGIC We will use:
# MAGIC 1. Distinct - It gives you distinct resultset which mean all the rows are unique. There are no duplicate rows. 
# MAGIC 1. DropDuplicates() - This function will help you to drop duplicate rows from your dataset.
# MAGIC 
# MAGIC ![DUPLICATE_DATA](https://raw.githubusercontent.com/martandsingh/images/master/duplicate.jpg)

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

# MAGIC %md
# MAGIC ### DISTINCT()

# COMMAND ----------

# Lets check if our recordset has any duplicate rows
total_count = df.count()
distinct_count = df.distinct().count()
print('total: ', total_count)
print('distinct: ', distinct_count)
print('duplicate: ', total_count-distinct_count)
# so you can see we have few duplicate records. Keep in mind these duplicate records are comparing whole row which mean there exist two or more rows which have exact same value for all the columns in the table.

# COMMAND ----------

# Lets find out duplicate values for specific column or multiple columns
# lets find all the distinct game names. there are 5155 distinct games in our dataset.
print(df.select("game").distinct().count())


# COMMAND ----------

# Find distinct record based on game & behaviour.
distinct_selected_column= df.select("game", "behaviour").distinct().count()
print(distinct_selected_column)

# COMMAND ----------

# So we saw how we can find distinct records based on few columns and all the columns. Now let's see how to drop duplicates values from our dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DropDuplicates()

# COMMAND ----------

# you can match this count with our distinct_count variable.  
df_distinct =  df.drop_duplicates()
df_distinct.count()

# COMMAND ----------

# Drop duplicates on selected column. It will check the duplicate rows based on combination of the given columns. it should match our distinct_selected_column value.
df_drop_selected = df.drop_duplicates(["game", "behaviour"])
df_drop_selected.count()

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


