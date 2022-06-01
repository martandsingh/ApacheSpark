# Databricks notebook source
print('Cleaning up all the database & tables....')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS DB_DEMO CASCADE;

# COMMAND ----------

print('Tables & database deleted sucessfully.')
