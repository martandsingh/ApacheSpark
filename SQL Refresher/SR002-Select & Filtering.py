# Databricks notebook source
# MAGIC %md
# MAGIC ### What is filtering?
# MAGIC Selecting a subset of data based on some business logic is called filtering.
# MAGIC e.g. You have data for multiple countries, then you may want to select data only for one particular country or city or both.
# MAGIC 
# MAGIC ![Filtering](https://raw.githubusercontent.com/martandsingh/images/master/filtering.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- * astrix will select all the columns and rows. As a big data engineer, you should avoid this because in real life scenario your table will have billions of records which you do not want to fetch frequently.
# MAGIC SELECT * FROM club;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM department;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee;

# COMMAND ----------

# MAGIC %sql -- Projection: select only few columns. this is a prefferd practice. Only select the columns which you need. It will optimize your query.
# MAGIC SELECT
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   dept_id
# MAGIC FROM
# MAGIC   employee;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT top 5 records. 
# MAGIC SELECT
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   dept_id
# MAGIC FROM
# MAGIC   employee
# MAGIC LIMIT
# MAGIC   5;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- apply filters using WHERE keyword choose all the employee of department DEP001
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   employee
# MAGIC WHERE
# MAGIC   dept_id = 'DEP001'

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- apply filters using WHERE keyword choose all the employee of department DEP001 & club C1
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   employee
# MAGIC WHERE
# MAGIC   dept_id = 'DEP001'
# MAGIC   AND club_id = 'C1'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all the employees from club C1, C2 & C3
# MAGIC SELECT * FROM employee
# MAGIC WHERE club_id IN ('C1', 'C2', 'C3')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all the employees which are not in club C1, C2 & C3
# MAGIC SELECT * FROM employee
# MAGIC WHERE club_id NOT IN ('C1', 'C2', 'C3')

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


