# Databricks notebook source
# MAGIC %md
# MAGIC ### JSON Handling
# MAGIC 1. How to read simple & nested JSON. 
# MAGIC 2. How to create new columns using nested json
# MAGIC 
# MAGIC ![PYSPARK_JSON](https://raw.githubusercontent.com/martandsingh/images/master/json_pyspark.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df = spark \
    .read \
    .option("multiline", "true")\
    .json("/FileStore/datasets/used_cars_nested.json")

display(df)

# COMMAND ----------

# Imports
from pyspark.sql.functions import explode, col

# COMMAND ----------

df_exploded = df \
            .withColumn("usedCars", explode(df["usedCars"]))

display(df_exploded)

# COMMAND ----------

# Now we will read JSON values and add new columns, later we will delete usedCars(Raw json) column as we do not need it.
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
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC So now we have our clean dataframe df_clean. So we saw how explode function create a row for each element of an array. In our case, our array had struct items. So each row created had a struct type item (df_exploded), which we later used to create new columns (df_clean). 
# MAGIC 
# MAGIC ### Explode
# MAGIC The explode() method converts each element of the specified column(s) into a row. 
# MAGIC 
# MAGIC __Syntax:__
# MAGIC 
# MAGIC dataframe.explode(column, ignore_index)
# MAGIC 
# MAGIC But there may some cases where we have a string type column which consist of JSON string. how to deal with it? Obviously, you can convert it to struct type and then follow the same process we did earlier. Apart from this, there is one more cleaner way to achieve this.
# MAGIC 
# MAGIC We can use json_tuple, get_json_object functions.
# MAGIC 
# MAGIC __get_json_object()__: This is used to query json object inline.
# MAGIC 
# MAGIC __json_tuple()__: *We can use this if json has only one level of nesting*
# MAGIC 
# MAGIC Confused????? Let's try with an example.

# COMMAND ----------

from pyspark.sql.functions import get_json_object, json_tuple

# COMMAND ----------

# lets create a test dataframe, which contains JSON string. Range function creates a simple dataframe with given number of rows.
df_json_string = spark.range(1)\
.selectExpr("""
'{"Car" : {"Model" : ["i10", "i20", "Verna"], "Brand":"Hyundai" }}' as Cars
""")

display(df_json_string)

# COMMAND ----------

# MAGIC %md 
# MAGIC Our task is to create a new dataframe with 2 columns:
# MAGIC * Model - take only first item in array. Just for the sake of tutorial. It will tell you how to pick a specific item using json_tuple
# MAGIC * Brand

# COMMAND ----------

df_cars = df_json_string \
        .withColumn("Brand", json_tuple(col("Cars"), "Car") )\
        .withColumn("Model", get_json_object(col("Cars"), "$.Car.Model[1]"))

display(df_cars)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment
# MAGIC 1. Download https://github.com/martandsingh/datasets/blob/master/person_details.json &  try to read it using spark
# MAGIC 2. Create a dataframe with column: name, age, cars(Array type), city, state, country

# COMMAND ----------


