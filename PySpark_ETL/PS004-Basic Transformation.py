# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Basic Data Transformation
# MAGIC In this notebook we will learn:
# MAGIC 1. Read parquet file
# MAGIC 1. Check row & columns
# MAGIC 1. Describe function
# MAGIC 1. Select columns
# MAGIC 1. Filter columns
# MAGIC 1. Case statement
# MAGIC 1. Add new column
# MAGIC 1. Rename column
# MAGIC 1. Drop column
# MAGIC 1. String comparison
# MAGIC 1. AND & OR condition
# MAGIC 1. EXPR
# MAGIC 
# MAGIC ![DATA_TRANSFORMATION](https://raw.githubusercontent.com/martandsingh/images/master/transformation.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Parquet File
# MAGIC pyspark_init_setup will setup data files in your dbfs location including parquet file. We will use it to perform basic operations. 

# COMMAND ----------

df_raw = spark.read.parquet("/FileStore/datasets/USED_CAR_PARQUET/")
display(df_raw)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Print Schema

# COMMAND ----------

# Check dataframe schema
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Describe function
# MAGIC As name suggests it will give you a quick summary of your dataframe e.g. count, mean, median and other information.  

# COMMAND ----------

df_describe = df_raw.describe() # returns a dataframe
display(df_describe)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Check Rows & Columns

# COMMAND ----------

# df.columns returns python list including all the columns
all_columns = df_raw.columns 
print(all_columns)

# COMMAND ----------

total_rows = df_raw.count()
print(total_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Columns

# COMMAND ----------

# Choose only required columns
display(df_raw.select("vehicle_type", "brand_name", "model", "price"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### col function
# MAGIC Returns a Column based on the given column name. We can use col("columnname") to select column

# COMMAND ----------

# include col function
from pyspark.sql.functions import col

# COMMAND ----------

# below query will return sae resultset as above
display(df_raw.select( col("vehicle_type"), col("brand_name"), col("model"), col("price") ))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### LIMIT
# MAGIC limit function is used to limit number of rows. If you want to select top n records, you can use limit functions.

# COMMAND ----------

# select top 5 records
df_limit = df_raw.select( col("vehicle_type"), col("brand_name"), col("model"), col("price") ).limit(5)
display(df_limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add New Column
# MAGIC We can add new column using withColumn("{column-name}", {value})

# COMMAND ----------

# Lets create a new column full_name with brand_name + model name in capital letters & select only top 5 full_name & price column. 
# withColumn is used to add new column.
# import concat_ws functions. this function concat two string with the provided separator.
from pyspark.sql.functions import concat_ws, upper

df_processed = df_raw \
            .withColumn("full_name", concat_ws(' ', col("brand_name"), col("model")) ) 
            
            
display(df_processed)
# You can see the full_name column added to the dataframe (scroll right).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Column
# MAGIC You can rename your column with 

# COMMAND ----------

df_processed = df_processed \
            .withColumnRenamed("full_name", "vehicle_name")

display(df_processed.limit(5))
# so we can see our output has new column name.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Drop Column(s)
# MAGIC You can drop one or more column using drop()

# COMMAND ----------

display(df_raw.drop("description", "ad_title", "seller_location"))
# so here we used drop to delete few columns. But there is one very interesting thing with spark dataframe. Spark dataframes are immutable. This means whenever you perform any transformation in your dataset, it creates a new dataset. This mean dropping those columns will generate a new dataframe, it will not delete those columns from the original dataframe(df_raw). let try displa(df_raw) in next cell.

# COMMAND ----------

display(df_raw.limit(4)) 
# we can see those 4 columns are still there. This is because of immutabile characterstics of spark dataframe. This is one of the major differences between spark & pandas dataframe.
# So everytime you perform any transformation, you have to create a new dataframe to persist those changes.

# COMMAND ----------

# thats why we will create a new dataframe 
df_processed = df_raw.drop("description", "ad_title", "seller_location")
df_processed.printSchema() # now this dataframe will not include dropped columns.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filter Data
# MAGIC You can use filter() to filter you data based on specific condition. It is simillar to SQL WHERE keyword.

# COMMAND ----------

# Lets select all the cars with body_type as SUV. Only select brand_name, body_type, price, displacement
df_SUV = df_processed \
        .filter(col("body_type")== "SUV")\
        .select("brand_name", "body_type", "price", "displacement")

display(df_SUV)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CASE Statement
# MAGIC The SQL CASE Statement. The CASE statement goes through conditions and returns a value when the first condition is met (like an if-then-else statement).
# MAGIC 
# MAGIC Let's categorise our data. Create a new column called "power" based on displacement value.  
# MAGIC 
# MAGIC - > Displacement < 1500 (less than 1500) -> Low 
# MAGIC 
# MAGIC - > 1500 >= Displacement < 2500 -> Medium  
# MAGIC 
# MAGIC - > Displacement >= 2500 -> Strong

# COMMAND ----------

from pyspark.sql.functions import when, regexp_replace

# we are adding a new column "power" which is being calculated using displacement. displacement contains cc. So first we will use regexp_replace to replace text cc. i.e. 100cc -> 100, 800cc -> 800. Then we will use "when" to perform conditional check.
df_power = df_processed \
        .withColumn("power", 
                    when(regexp_replace(df_raw.displacement, "cc", "") < 1500, "LOW")
                    .when(( regexp_replace(df_raw.displacement, "cc", "") >= 1500) & (df_raw.displacement < 2500),  "MEDIUM")
                    .otherwise("HIGH")
                   ).select("vehicle_type", "brand_name", "displacement", "power")

display(df_power)

# COMMAND ----------

# MAGIC %md
# MAGIC ### AND & OR
# MAGIC These keyword comes handy when we are defining multiple filter condition.
# MAGIC 
# MAGIC 1. AND- represented by &. it means all the condition in the expression must be true. 
# MAGIC 
# MAGIC example: I need a White Suzuki car. It mean my car must be white & brand must be Suzuki. It has to satisfy both the criteria. I will not accept Black Suzuki or White Honda.
# MAGIC 
# MAGIC TRUE & TRUE = TRUE ( this is and condition in binary form. So both of the expression must be true)
# MAGIC 
# MAGIC 2. OR - represent by |. It means any one of multiple condition is true.
# MAGIC 
# MAGIC TRUE + FALSE = TRUE

# COMMAND ----------

# Let's see one more example. But this time we will use 2 filter conditions. Select all white suzuki cars. 
df_suz_wh = df_processed.filter( (col("brand_name")=="Suzuki") & (col("color")=="White") )
display(df_suz_wh)
# we can see our resultset only include White Suzuki. 

# COMMAND ----------

# I want to buy a car, but I have one condition. It should be either Honda or Suzuki. let's write the query. Here we will use OR condition as I have two options Honda & Suzuki. Any one of them is acceptable. Keep in mind comparison is always case sensitive. Honda is different then honda. So col("brand_name") == "Honda" & col("brand_name") == "honda" will produce different resultset. Try this in your assignment.

df_car = df_processed.filter( (col("brand_name") == "Honda") | (col("brand_name") == "Suzuki" ) )
display(df_car)

# COMMAND ----------

# MAGIC %md
# MAGIC ## String comparison is case sensitive !!!
# MAGIC As I mentioned earlier text based comparison is always case sensitive. Honda is different than honda. So what is the best possible way to compare string? 
# MAGIC 
# MAGIC There are multiple ways but which I prefer is using __UPPER OR LOWER__ case function while comparing. Let's see the using example. We will slightly change above query.

# COMMAND ----------

from pyspark.sql.functions import upper

# I added upper function & if you noticed, I changed the case of "Honda" to "honda". It is still giving me same result. The query will convert both side of comparison string to upper case and then compare. In this way you guarantee that both sides will always have same case. you can use lower function also for this.
df_car = df_processed.filter( (upper(col("brand_name")) == "honda".upper() ) | ( upper(col("brand_name")) == "Suzuki".upper() ) )
display(df_car)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPR - expression
# MAGIC Using EXPR we can run SQL statements in pyspark statements.

# COMMAND ----------

# lets categorize our data using SQL expression.
from pyspark.sql.functions import expr
df_categorized_expr = df_processed \
                       .withColumn("power", expr('''
                           CASE WHEN displacement < 1500 THEN "LOW"
                           WHEN displacement >= 1500 AND displacement < 2500 THEN "MEDIUM"
                           ELSE "HIGH" END''' )) \
                       .select("vehicle_type", "body_type", "brand_name", "displacement", "power")

display(df_categorized_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chaining
# MAGIC In our examples we apply different kind of operations in different cell, but apache spark allows you to chain multiple statements.
# MAGIC e.g. df.operation1.operation2.operation3.....
# MAGIC 
# MAGIC __Lets do following operations on our dataset:__
# MAGIC 1. delete column description, manufacturer & adtitle
# MAGIC 1. update column displacement, remove cc from values. e.g. 800cc -> 800
# MAGIC 1. create a new column price_in_k = price/1000. it will proce price in 1000s.
# MAGIC 1. select only body_type, brand_name, color, displacement, price_in_k

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df_processed = df_raw\
    .withColumn("displacement", regexp_replace(col("displacement"), "cc", ""))\
    .withColumn("price_in_k", col("price")/1000)\
    .drop("description", "manufacturer", "adtitle")\
    .select("body_type", "brand_name", "color", "displacement", "price_in_k")

display(df_processed)

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment 1
# MAGIC 1. Add a new column price_usd to your dataframe df_raw. This column will save price of car in USD. price_usd = price * (0.0051);
# MAGIC 2. Add one more column brand_name_upper, which will have brand_name in upper cases.
# MAGIC 3. Select only brand_name_upper, color, price_usd columns to your final resultset.
# MAGIC 
# MAGIC ### Assignment 2
# MAGIC 1. Select all the Grey Honda Hatchback cars.

# COMMAND ----------


