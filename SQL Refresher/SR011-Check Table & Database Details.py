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
# MAGIC DESCRIBE EXTENDED employee
# MAGIC -- this will show detailed information about table. 

# COMMAND ----------

# In above column you can see the location of the table. location shows where are all the data files saved for that particular table. let's explore that folder
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/db_demo.db/employee/"))

# COMMAND ----------

# All the transaction logs is saved in _delta_log folder
display(dbutils.fs.ls("dbfs:/user/hive/warehouse/db_demo.db/employee/_delta_log/"))

# COMMAND ----------

# transaction logs will be saved in json files. let's explore one.
df_trans = spark.sql(f"SELECT * FROM json.`dbfs:/user/hive/warehouse/db_demo.db/employee/_delta_log/00000000000000000001.json`")
display(df_trans)

# add column shows files added for that particular transaction & remove shows which file is removed for that transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employee
# MAGIC -- Using this you can more details about table. We are interested in numFiles. numfiles shows the data files used for this table. But we earlier saw there are more than 8 files in file location. what is that? 
# MAGIC -- Delta lake keeps the older version of data to use time travel functionality. When you run query it checks the meta data and only include the files which are valid and ignore all others. We do not have to go so deeply in this just better to know how it works.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can check history for a table. It will show you all the changes saved in transaction logs
# MAGIC DESCRIBE HISTORY db_demo.employee

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


