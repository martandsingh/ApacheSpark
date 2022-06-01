# Databricks notebook source
# MAGIC %md
# MAGIC ### What is metadata?
# MAGIC Metadata is descriptive information. So whenever you create a database or table, metadata is generated in backend. During your data engineering process, you may need to see details or metadata about your table & database. We can do it using "DESCRIBE" keyword.
# MAGIC 
# MAGIC Let's have a demo.
# MAGIC 
# MAGIC ![METADATA](https://raw.githubusercontent.com/martandsingh/images/master/metadat.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check database details
# MAGIC DESCRIBE DATABASE DB_DEMO;
# MAGIC 
# MAGIC -- you can see it will give you basic information about database. You can see the location of data stored. You can change this location when creating the database,.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED DB_DEMO;
# MAGIC -- When you write extended, it will give you more details about database. We have not defined any Properties so it is showing empty column for that. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE employee
# MAGIC 
# MAGIC -- or you can simply write DESCRIBE employee. You can see all the columns and data types. We do not have any partitioning for now.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED employee
# MAGIC -- this will show detailed information about table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check all the databases in system
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check all the tables in all the databases. before running this command you have to select a database. We have selected the DB_DEMO database in our initial_setup script
# MAGIC SHOW TABLES 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW PARTITIONS DB_DEMO.employee;
# MAGIC -- we can use above table to check partitions. As our table does not have partition, we cannot run the command.

# COMMAND ----------


