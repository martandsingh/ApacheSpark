# Databricks notebook source
# MAGIC %md
# MAGIC ### What is Delta Lake?
# MAGIC Delta Lake is an open source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs. Delta Lake on Databricks allows you to configure Delta Lake based on your workload patterns.
# MAGIC 
# MAGIC * Delta Lake is not a data warehouse

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE DATABASE
# MAGIC CREATE DATABASE IF NOT EXISTS DB_DEMO;
# MAGIC USE DB_DEMO;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LIST DOWN ALL THE DATABASES IN OUR DELTA LATE
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE
# MAGIC CREATE TABLE IF NOT EXISTS employee
# MAGIC (
# MAGIC   firstname VARCHAR(50),
# MAGIC   lastname VARCHAR(50),
# MAGIC   salary DECIMAL(10, 2),
# MAGIC   dept VARCHAR(50)
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- LIST DOWN TABLE IN SELECTED DATABASE;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHECK DETAILS ABOUT TABLES
# MAGIC DESCRIBE TABLE employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHECK EXTENDED DETAILS ABOUT TABLE INCLUDING META DATA
# MAGIC DESCRIBE  TABLE EXTENDED employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERTING VALUES
# MAGIC INSERT INTO employee
# MAGIC (firstname, lastname, salary, dept)
# MAGIC VALUES
# MAGIC ('Thomas', 'Edison', 100000, 'Electronics'),
# MAGIC ('C.V', 'Raman', 500000, 'Botany'),
# MAGIC ('Albert', 'Einstein', 4500000, 'Physics'),
# MAGIC ('John', 'Wick', 1500000, 'Physics'),
# MAGIC ('Steve', 'Jobs', 6500000, 'Electronics'),
# MAGIC ('Bill', 'Gates', 3500000, 'Electronics')

# COMMAND ----------

# data is divided in multiple parquet files. Parquet format is default for deltalake table.
display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db_demo.db/employee')) 
# this is the path we got from extended table details. All the data related to this table will get saved at this location.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- List down all the rows of employee table
# MAGIC SELECT * FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- grouping data based on department
# MAGIC SELECT dept AS Department, COUNT(1) AS total_members
# MAGIC FROM employee
# MAGIC GROUP BY dept
# MAGIC ORDER BY total_members

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- updating employee table
# MAGIC UPDATE employee
# MAGIC SET salary = '2300000'
# MAGIC WHERE firstname = 'John' AND lastname='Wick';

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM employee

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- deleting rows based on condition
# MAGIC DELETE FROM employee WHERE lastname = 'Gates'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregation
# MAGIC SELECT dept AS department, ROUND(AVG(salary), 2) AS average_salary
# MAGIC FROM employee
# MAGIC GROUP BY dept
# MAGIC ORDER BY average_salary;

# COMMAND ----------


