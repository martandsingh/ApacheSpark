# Databricks notebook source
# MAGIC %md
# MAGIC ### What is a lakehouse?
# MAGIC New systems are beginning to emerge that address the limitations of data lakes. A lakehouse is a new, open architecture that combines the best elements of data lakes and data warehouses. Lakehouses are enabled by a new system design: implementing similar data structures and data management features to those in a data warehouse directly on top of low cost cloud storage in open formats. They are what you would get if you had to redesign data warehouses in the modern world, now that cheap and highly reliable storage (in the form of object stores) are available.
# MAGIC 
# MAGIC A lakehouse has the following key features:
# MAGIC 
# MAGIC * Transaction support: In an enterprise lakehouse many data pipelines will often be reading and writing data concurrently. Support for ACID transactions ensures consistency as multiple parties concurrently read or write data, typically using SQL.
# MAGIC * Schema enforcement and governance: The Lakehouse should have a way to support schema enforcement and evolution, supporting DW schema architectures such as star/snowflake-schemas. The system should be able to reason about data integrity, and it should have robust governance and auditing mechanisms.
# MAGIC * BI support: Lakehouses enable using BI tools directly on the source data. This reduces staleness and improves recency, reduces latency, and lowers the cost of having to operationalize two copies of the data in both a data lake and a warehouse.
# MAGIC * Storage is decoupled from compute: In practice this means storage and compute use separate clusters, thus these systems are able to scale to many more concurrent users and larger data sizes. Some modern data warehouses also have this property.
# MAGIC * Openness: The storage formats they use are open and standardized, such as Parquet, and they provide an API so a variety of tools and engines, including machine learning and Python/R libraries, can efficiently access the data directly.
# MAGIC * Support for diverse data types ranging from unstructured to structured data: The lakehouse can be used to store, refine, analyze, and access data types needed for many new data applications, including images, video, audio, semi-structured data, and text.
# MAGIC * Support for diverse workloads: including data science, machine learning, and SQL and analytics. Multiple tools might be needed to support all these workloads but they all rely on the same data repository.
# MAGIC * End-to-end streaming: Real-time reports are the norm in many enterprises. Support for streaming eliminates the need for separate systems dedicated to serving real-time data applications.
# MAGIC 
# MAGIC ### What is Delta Lake?
# MAGIC Delta Lake is an open source project that enables building a Lakehouse architecture on top of data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of existing data lakes.
# MAGIC 
# MAGIC In this demo we will see delta lake basic concepts:
# MAGIC 1. How to create table?
# MAGIC 1. How to insert data?
# MAGIC 1. How to query data?
# MAGIC 1. How to check table meta data?
# MAGIC 1. How to group & aggregate data?
# MAGIC 1. How to drop table & database?
# MAGIC 
# MAGIC ** Source: Databricks & Microsoft

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

# MAGIC %sql 
# MAGIC -- dropping table
# MAGIC DROP TABLE employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop database
# MAGIC DROP DATABASE DB_DEMO;

# COMMAND ----------


