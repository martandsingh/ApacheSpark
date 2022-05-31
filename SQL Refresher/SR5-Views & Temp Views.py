# Databricks notebook source
# MAGIC %md
# MAGIC ### What is View?
# MAGIC View is a virtual table based on resultset of a query. A view always gives you latest resultset. In real scenario, if you have a complex query with many joins & sub queries which you want to reuse, you can create a view for that query and use it like a physical table. 
# MAGIC 
# MAGIC SYNTAX:
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS {ViewName} AS
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM TABLE
# MAGIC 
# MAGIC ### What is Temp View?
# MAGIC TEMPORARY views are session-scoped and is dropped when session ends because it skips persisting the definition in the underlying metastore, if any. GLOBAL TEMPORARY views are tied to a system preserved temporary schema global_temp.
# MAGIC 
# MAGIC SYNTAX:
# MAGIC 
# MAGIC CREATE TEMP VIEW IF NOT EXISTS {tempviewname} AS
# MAGIC 
# MAGIC SELECT * FROM TABLE
# MAGIC 
# MAGIC 
# MAGIC ![SQL_JOINS](https://raw.githubusercontent.com/martandsingh/images/master/view-demo.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql --Lets say we want to find all the the employee with their department & club name. For that we have to write a complex query with multiple join.
# MAGIC SELECT
# MAGIC   E.firstname,
# MAGIC   E.lastname,
# MAGIC   D.dept_name AS Department,
# MAGIC   C.club_name AS Club
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC   INNER JOIN club C ON E.club_id = C.club_id

# COMMAND ----------

# MAGIC %md 
# MAGIC Now we have a complex query which we may want to reuse that query in multiple procedure. You are in your scrum meeting and find out, you are using the wrong logic. Now you have to make changes in multiple procedure. It will:
# MAGIC 1. Waste your time & effort
# MAGIC 1. You may miss some procedures
# MAGIC 
# MAGIC So best way to peform this task is to create a view. You can use that view in your procedures. In case of any logic change, now you only have to update your view. As view gives you the latest result, your changes will immediately reflect to all the procedures. tada!!!!

# COMMAND ----------

# MAGIC %sql CREATE VIEW IF NOT EXISTS VW_GET_EMPLOYEES AS
# MAGIC SELECT
# MAGIC   E.firstname,
# MAGIC   E.lastname,
# MAGIC   D.dept_name AS Department,
# MAGIC   C.club_name AS Club
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC   INNER JOIN club C ON E.club_id = C.club_id

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have our complex query available as a view VW_GET_EMPLOYEES, which we can use as a regular table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- it will give you exact same result as our complex query
# MAGIC SELECT * FROM VW_GET_EMPLOYEES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can list views using SHOW VIEWS
# MAGIC SHOW VIEWS

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Now let's create a temp view. How is it different than a regular view?
# MAGIC 
# MAGIC Well a temp view will be available only for your current session. If you restart your session, you will loose your temp view. It will not affect any physical table or data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_VW_GET_EMPLOYEES AS
# MAGIC SELECT
# MAGIC   E.firstname,
# MAGIC   E.lastname,
# MAGIC   D.dept_name AS Department,
# MAGIC   C.club_name AS Club
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC   INNER JOIN club C ON E.club_id = C.club_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEMP_VW_GET_EMPLOYEES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can drop views using DROP VIEW
# MAGIC DROP VIEW VW_GET_EMPLOYEES

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


