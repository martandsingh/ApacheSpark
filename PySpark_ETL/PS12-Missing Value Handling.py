# Databricks notebook source
# MAGIC %md
# MAGIC ### Handling Missing Values
# MAGIC In real life, data is not fine & curated. It is ambigous, dirty. It may contains null values or invalid values. for example. You have a hospital dataset, the age of patient is 1000. I am not sure about other planets but on earth it is impossible for a person to live 1000 years. So clearly it is a mistake. There can be case where firstname of patient is null. 
# MAGIC 
# MAGIC So you will see these kind of issues in raw data. These values can hamper your analysis. So we have to find and fix those values.
# MAGIC 
# MAGIC ![MISSING_VALUES](https://raw.githubusercontent.com/martandsingh/images/master/missing-values.png)
# MAGIC 
# MAGIC 
# MAGIC There are many ways to handle these values:
# MAGIC 
# MAGIC 1. Drop - We can drop the entire row if we find any null value. But as per my experience I do not prefer this method as sometime you delete important information or anamolies if you use this method.
# MAGIC 
# MAGIC 1. Fill - Here we try to fill our null values with some valid values. These values can be mean, mode, median or some other logic you define. The concept it to replace null values with most common or likely value. In this way you do not lose the entire row.
# MAGIC 
# MAGIC 1. Replace - Replace is more flexible option than fill. It can do the operation with fill() does but apart from that it is also helpful when you want to replace a string with another.
# MAGIC 
# MAGIC ### What is Imputation?
# MAGIC The process of preserving all cases by replacing missing data with an estimated value based on other available information is called imputation. Fill() & Replace() are used for imputation. 
# MAGIC 
# MAGIC *Let's not go deep in theory and see the action.

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.option("header", "true").csv('/FileStore/datasets/missing_val_dataset.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Null values count

# COMMAND ----------


#Lets check null values for one column. It will give you
null_count = df.filter(col("cloud").isNull()).count()  # count of all the null values
not_null_count = df.filter(col("vintage").isNotNull()).count() # count of not null values
print("Total null values: ", null_count)
print("Total not null values: ", not_null_count)

# COMMAND ----------

# You can get same result using sql expression
null_count_sql = df.filter("cloud IS NULL").count()  # count of all the null values
not_null_count_sql = df.filter("vintage IS NOT NULL").count() # count of not null values
print("Total null values: ", null_count_sql)
print("Total not null values: ", not_null_count_sql)

# COMMAND ----------

 # Check null count based on multiple columns

null_count_ml = df.filter(col("cloud").isNull() & col("vintage").isNull() ).count()  # count of all the null values
not_null_count_ml = df.filter(col("cloud").isNotNull() & col("cloud").isNotNull()  ).count() # count of not null values
print("Total null values: ", null_count_ml)
print("Total not null values: ", not_null_count_ml)

# COMMAND ----------

display(df.filter(col("cloud").isNull()))

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import count, when, isnan, lower

# COMMAND ----------

# Here we can see we will not catch 'NA' values from vintage as NA is not a valid null character. To catch that we can 
df_null = df.select([count(when( isnan(c) | col(c).isNull() , c)).alias(c) for c in df.columns])
display(df_null)

# COMMAND ----------

# So to catch NA we added one more condition. Now in our below condition we are considering "NA" as invalid values.
df_null = df.select([count(when( isnan(c) | col(c).isNull() | (col(c) == "NA") , c)).alias(c) for c in df.columns])
display(df_null)

# COMMAND ----------

# MAGIC %md
# MAGIC Fill(), DROP() & REPLACE() function will handle only system null values. So we should replace our "NA" values with system NULL. Below code is doing the same thing. It is replacn "NA" with None.

# COMMAND ----------

# Here we are replacing all the NA values to null as NA values are not valid missing value. So to process them we are changing them to system null values.
df_trans = df.withColumn("vintage", when(col("vintage") == "NA", None)
                        .otherwise(col("vintage")) )

display(df_trans)

# COMMAND ----------

# Now let's check null value counts after reaplcing "NA" with None.
df_null = df_trans.select([count(when( isnan(c) | col(c).isNull()  , c)).alias(c) for c in df_trans.columns])
display(df_null)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Missing Values
# MAGIC There are multiple way to deal with missing values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Way1 : Drop
# MAGIC 
# MAGIC Drop rows with null values. 
# MAGIC 
# MAGIC It has 2 options __Any__ and __All__.
# MAGIC 
# MAGIC In "Any" row is deleted, if any one of all the columns has a null or invalid values. In contrary, "All" will delete row only when all the columns has null values. You can also use subset if the columns. 
# MAGIC 
# MAGIC *Let's check with examples...

# COMMAND ----------

# before dropping lets count the total number of rows.
total_count=df_trans.count() # total count
print(total_count)

# COMMAND ----------

# it will drop complete row if any one column has null value. It will not NA value rows from vintage column
# Using any deleted 59 columns out of 61.
dropna_any = df_trans.na.drop(how="any") 
display(dropna_any)

# COMMAND ----------

# it will drop complete row if all the column has null value. We can see this statement did not delete any records as there are no records with all column null. It did not delete any as there are no rows with all null values.
df.na.drop(how="all").count() 

# COMMAND ----------



# COMMAND ----------

# Take null value count in variable so that we can verify later with our drop() function.
cld_all_null = (df_trans.filter("cloud IS NULL AND vintage IS NULL")).count()
print(cld_all_null)

cld_any_null =  (df_trans.filter("cloud IS NULL OR vintage IS NULL")).count()
print(cld_any_null)

# COMMAND ----------

# apply drop to few columns. This will delete rows where either cloud or vintage is null. This will delte 59 rows, you can confirm with variable cld_any_null
df_trans.na.drop(how="any", subset=["cloud", "vintage"]).count() 
# this count is after deleting rows. total_rows - cld_any_null

# COMMAND ----------

# apply drop to few columns. This will delete rows where cloud and vintage both columns are null. It will delete 8 rows as you can confirm with variable cld_all_null value.
df_trans.na.drop(how="all", subset=["cloud", "vintage"]).count()
# this count is after deleting rows. total_rows - cld_all_null

# COMMAND ----------

# MAGIC %md
# MAGIC ### Way 2: Fill
# MAGIC Now let's see how to fill null value. This is an imputation technique.

# COMMAND ----------

# Fill function is used to fill null values. Below code will replace all the null values in all the columns with NULL_VAL_REPLACE value. You can use any custom value. Just take care of column length and type.

display(df_trans.na.fill("NULL_VAL_REPLACE").select("cloud", "vintage"))

# COMMAND ----------

# You can use subset if you want to fill missing values only in few columns.
display(df_trans.na.fill("MISSING_VALUE", subset=["cloud", "vintage"]))

# COMMAND ----------

# You can define new value for each column. Below code will replace all the null values in cloud with NEW_VALUE & vintage with 2022.

missing_values = {
    "cloud": "NEW_VALUE",
    "vintage": "2022"
}
display(df_trans.na.fill(missing_values))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Way 3: REPLACE

# COMMAND ----------

display(df_trans.na.replace("GloBI", "NEW_GLOBI_NAME"))

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
