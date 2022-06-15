# Databricks notebook source
# MAGIC %md
# MAGIC ### Case Study - Sales Order Analysis
# MAGIC 
# MAGIC ![SALES_ORDER](https://raw.githubusercontent.com/martandsingh/images/master/case_study_1.jpg)
# MAGIC 
# MAGIC We have Order-Sales dataset. It includes three dataset:
# MAGIC 
# MAGIC __Order List - it contains the list of all the order with amount, city, state.__
# MAGIC 
# MAGIC Order List
# MAGIC * Order ID: string (nullable = true)
# MAGIC * Order Date: string (nullable = true)
# MAGIC * CustomerName: string (nullable = true)
# MAGIC * State: string (nullable = true)
# MAGIC * City: string (nullable = true)
# MAGIC 
# MAGIC 
# MAGIC __Order Details - detail of the order. Order list has 1-to-many relationship with this dataset.__
# MAGIC 
# MAGIC Order Details
# MAGIC * Order ID: string (nullable = true)
# MAGIC * Amount: string (nullable = true)
# MAGIC * Profit: string (nullable = true)
# MAGIC * Quantity: string (nullable = true)
# MAGIC * Category: string (nullable = true)
# MAGIC * Sub-Category: string (nullable = true)
# MAGIC 
# MAGIC 
# MAGIC __Sales Target - This contains the monthly sales target of product category.__
# MAGIC 
# MAGIC Sales Target
# MAGIC * Month of Order Date: string (nullable = true)
# MAGIC * Category: string (nullable = true)
# MAGIC * Target: string (nullable = true)
# MAGIC  
# MAGIC  
# MAGIC __We will try to answer following question asked by our business user:__
# MAGIC 1. Top 10 most selling categories & sub-categories (based on number of orders).
# MAGIC 1. Which order has the highest & lowest profit.
# MAGIC 1. Top 10 states & cities with highest total bill amount
# MAGIC 1. In which month & year we received most number of orders with total amount (show top 10).
# MAGIC 1. Which category fullfiled the month target. Add one extra column "IsTargetCompleted" with values Yes or No.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load datasets

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
df_od = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderdetails.csv")
df_st = spark.read.option("header", "true").csv("/FileStore/datasets/sales/salestarget.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check Schema

# COMMAND ----------

df_ol.printSchema()
df_od.printSchema()
df_st.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Top 10 most selling categories & sub-categories.

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

df_most_selling_cat = df_od \
        .groupBy("category", "Sub-Category")\
        .count()\
        .withColumnRenamed("count", "total_records")\
        .orderBy(col("total_records").desc())\
        .limit(10)
display(df_most_selling_cat)

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Which order has the highest & lowest profit.

# COMMAND ----------

df_order_profit= df_od\
            .withColumn("Profit_Numeric", col('Profit').cast("decimal") )\
            .groupBy("Order ID")\
            .sum("Profit_Numeric").withColumnRenamed("sum(Profit_Numeric)", "total_profit")\

lowest = df_order_profit\
        .withColumn("Type", lit("Lowest"))\
        .orderBy("total_profit")\
        .limit(1)\
      

highest= df_order_profit\
         .withColumn("Type", lit("Highest"))\
         .orderBy(col("total_profit").desc())\
         .limit(1)\
         

df_profit_stats =lowest.union(highest)
display(df_profit_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 states & cities with highest total bill amount

# COMMAND ----------

df_high_city = df_ol\
            .join(df_od, df_ol["Order ID"] == df_od["Order ID"], "inner")\
            .selectExpr("State", "City", "CAST(Amount AS Decimal) AS amount_decimal")\
            .groupBy("State", "City")\
            .sum("amount_decimal")\
            .withColumnRenamed("sum(amount_decimal)", "total_amount")\
            .orderBy(col("total_amount").desc())\
            .limit(10)


display(df_high_city)

# COMMAND ----------

# MAGIC %md
# MAGIC #### In which month & year we received most number of orders with total amount (show top 10)

# COMMAND ----------

# to do this first we have to add a new columne which contain order date in date format
df_date =  df_ol\
            .join(df_od, df_ol["Order ID"] == df_od["Order ID"], "inner")\
            .select(df_ol["Order ID"], "Order Date", "Amount")
display(df_date)

# COMMAND ----------

df_date.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date, month, year, date_format

# COMMAND ----------

df_year_month = df_date\
        .withColumn("order_date", to_date("Order Date", "dd-MM-yyyy"))\
        .withColumn("order_month", date_format("order_date", "MMM"))\
        .withColumn("order_year", year("order_date"))\
        .groupBy("order_year", "order_month")\
        .agg({"Amount":"sum", "Order ID":"count"})\
        .withColumnRenamed("count(Order ID)", "order_count")\
        .withColumnRenamed("sum(Amount)", "total_amount")\
        .orderBy(col("order_count").desc(), col("total_amount").desc())\
        .limit(10)


display(df_year_month)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which category fullfiled the month target. Add one extra column "IsTargetCompleted" with values Yes or No.

# COMMAND ----------

from pyspark.sql.functions import concat_ws, substring, when

# COMMAND ----------

df_order_details  = df_ol\
                .join(df_od, df_ol["Order ID"]==df_od["Order ID"], "inner")\
                .select(df_ol["Order ID"], "Order Date", "Amount", "Category")\
                .withColumn("order_date", to_date("Order Date", "dd-MM-yyyy"))\
                .withColumn("target_month"\
                            , concat_ws("-", date_format("order_date", "MMM"), substring(year("order_date"), 3, 2) ) )\
                .withColumn("amount_decimal", col("Amount").cast("decimal"))\
                .groupBy("target_month", "Category")\
                .sum("amount_decimal")\
                .withColumnRenamed("sum(amount_decimal)", "total_month_sales_amount")

df_final_target = df_order_details\
            .join(df_st\
                  , (df_order_details["target_month"]==df_st["Month of Order Date"]) &\
                  (df_order_details["Category"]==df_st["Category"]), "inner")\
            .select(\
                    df_order_details["Category"]\
                    , "target_month"\
                    , "total_month_sales_amount"\
                    , "Target")\
            .withColumn("TargetAcheived", when(col("Target") < col("total_month_sales_amount"), "No" )\
                       .otherwise("Yes"))


display(df_final_target)

# COMMAND ----------

# you can see how many category achevied the targets
display(df_final_target.groupBy("TargetAcheived").count())

# COMMAND ----------

# you can see how many category achevied the targets with Category name
display(df_final_target.groupBy("Category", "TargetAcheived").count().orderBy("Category"))

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up
