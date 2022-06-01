# Databricks notebook source
# MAGIC %md
# MAGIC ### Introduction
# MAGIC This course is the first installment of databricks data engineering course. In this course you will learn basic SQL concept which include:
# MAGIC 1. Create, Select, Update, Delete tables
# MAGIC 1. Create database
# MAGIC 1. Filtering data
# MAGIC 1. Group by & aggregation
# MAGIC 1. Ordering
# MAGIC 1. SQL joins
# MAGIC 1. Common table expression (CTE)
# MAGIC 1. External tables
# MAGIC 1.  Sub queries
# MAGIC 1. Views & temp views
# MAGIC 1. UNION, INTERSECT, EXCEPT keywords
# MAGIC 
# MAGIC you can download all the notebook from our 
# MAGIC 
# MAGIC github repo: https://github.com/martandsingh/ApacheSpark
# MAGIC 
# MAGIC facebook: https://www.facebook.com/codemakerz
# MAGIC 
# MAGIC email: martandsays@gmail.com
# MAGIC 
# MAGIC ### SETUP folder
# MAGIC you will see initial_setup & clean_up notebooks called in every notebooks. It is mandatory to run both the scripts in defined order. initial script will create all the mandatory tables & database for the demo. After you finish your notebook, execute clean up notebook, it will clean all the db objects.
# MAGIC 
# MAGIC ![SQL](https://raw.githubusercontent.com/martandsingh/images/master/sql.png)

# COMMAND ----------

# MAGIC %
