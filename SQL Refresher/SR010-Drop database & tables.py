# Databricks notebook source
# MAGIC %md
# MAGIC ### Drop database
# MAGIC DROP DATABASE {DB_NAME}
# MAGIC 
# MAGIC ### DROP table
# MAGIC DROP TABLE {TABLE_NAME}
# MAGIC 
# MAGIC Our initial setup script creates DB_DEMO & employee table. Let's check a demo.

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC --list databases
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- we can see there is a database name db_demo & few tables. let's drop employee table & then whole database.
# MAGIC DROP TABLE employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
# MAGIC -- now you will not see employee table in the list

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets delete whole database. if your database has table then we have to use one keyword CASCADE, it will delete all the table and other objects in side the database and then drop the database.
# MAGIC DROP DATABASE DB_DEMO CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES
# MAGIC -- db_demo will not be in the list.

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


