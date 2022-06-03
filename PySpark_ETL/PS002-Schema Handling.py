# Databricks notebook source
# MAGIC %md 
# MAGIC ### Schema Handling
# MAGIC In this notebook, we will learn:
# MAGIC 1. What is inferSchema?
# MAGIC 1. How to check dataframe schema?
# MAGIC 1. How to define custom schema?
# MAGIC 
# MAGIC ### Whats is inferSchema?
# MAGIC Infer schema will automatically guess the data types for each field. If we set this option to TRUE, the API will read some sample records from the file to infer the schema. 
# MAGIC 
# MAGIC InferSchema option is __false by default__ that is why all the columns are string by default. By setting __inferSchema=true__, Spark will automatically go through the csv file and infer the schema of each column. This requires an extra pass over the file which will result in reading a file with __inferSchema set to true being slower__. But in return the dataframe will most likely have a correct schema given its input.
# MAGIC 
# MAGIC ### How to check dataframe schema?
# MAGIC To check dataframe schema you can simply use printSchema().
# MAGIC 
# MAGIC example: df.printSchema()

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, StructField, StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Inferschema

# COMMAND ----------

df_infer = spark \
            .read \
            .option("header", "true") \
            .csv("/FileStore/datasets/cancer.csv")

df_infer.printSchema()

# You can see all the columns has string type as inferSchema is set false by default.

# COMMAND ----------

df_infer = spark \
            .read \
            .option("header", "true") \
            .option("inferSchema", "true")\
            .csv("/FileStore/datasets/cancer.csv")

df_infer.printSchema()
# So now you will see datatypes are different, schema will check all the data and will choose corrct type for each columns. Keep in mind this option is slower as spark has to traverse whole data to identity data type. So avoid this for big data sets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Custom Schema

# COMMAND ----------

# Let's create a test dataframe & a custom schema. We can you the csv file to create dataframe also but just for the demo I want to use smaller number of columns which will help you to understand better & save time also.

# Create python list of data
data = [("James","","Smith","36636","M",50,3000),
    ("Michael","Rose","","40288","M",43,4000),
    ("Robert","","Williams","42114","M",23,4000),
    ("Maria","Anne","Jones","39192","F",56,4000),
    ("Jen","Mary","Brown","33341","F",34,1500)
  ]

# defining custom schema. Your schema will be StructType of StructField array. then you can define any custom name for your column & type.
# StructField("middlename",StringType(),True) - this means our column name is middlename which is String type. The third True parameter determines whether the column can contain null values or not? True mean column allows null values.

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("age", IntegerType(), True), 
    StructField("salary", IntegerType(), True) 
  ])
 


# COMMAND ----------

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
# so you can see our age & salary columns are integer type while all other have string type. This way of assigning schema is faster than inferSchema.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's read one CSV with custom schema & header.

# COMMAND ----------

# Lets define a custom schema first
custom_schema = StructType(
    [StructField("customer_id", IntegerType(), False),
    StructField("gender", StringType(), True),
     StructField("age", IntegerType(), True),
     StructField("annual_income_k_usd", IntegerType(), True),
     StructField("score", IntegerType(), True),
    ]
)

# COMMAND ----------

# assigning custome schema
df_mall = spark \
        .read \
        .schema(custom_schema) \
        .option("header", "true") \
        .csv("/FileStore/datasets/Mall_Customers.csv")
display(df_mall)

# COMMAND ----------

# we can see the column names & types are correct. It is always recommended to check dataframe schema before performing other operations. It will be helpful to decide whether your dataframe needs any type casting. We will study about type casting in further notebooks. So do not worry if you do not understand any piece of code. This is just introductory notebook. There is a separate notebook to explain all the basic transformation using pyspark.

df_mall.printSchema()

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


