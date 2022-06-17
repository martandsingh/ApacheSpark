# Databricks notebook source
# MAGIC %md
# MAGIC ### What are Joins?
# MAGIC Joins are used to combine two or more tables based on one or more column. This is used to select data from multiple table.
# MAGIC 
# MAGIC ### Types of join
# MAGIC There are 4 major kind of joins:
# MAGIC 1. Inner join
# MAGIC 1. Left outer join
# MAGIC 1. Right outer join
# MAGIC 1. Full outer join
# MAGIC 1. Cross join
# MAGIC ![SQL_JOIN](https://raw.githubusercontent.com/martandsingh/images/master/join_demo.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Database & table details
# MAGIC _initial_setup command will run our setup notebook where we are creating DB_DEMO database & 3 table (employee, department, club). employee table include employee details including department id(dept_id) & club id (club_id) which are the Foreign key related with department & club table respectively. Below entity diagram shows the relation between tables.
# MAGIC 
# MAGIC 
# MAGIC ![my_test_image](https://raw.githubusercontent.com/martandsingh/images/master/entity_diag.png)

# COMMAND ----------

# MAGIC %md 

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN
# MAGIC Returns rows that have matching values in both the table (LEFT table, RIGHT table). Left table is the one mentioned before JOIN clause & RIGHT table is the one mentioned after JOIN clause.
# MAGIC 
# MAGIC Syntax:
# MAGIC 
# MAGIC SELECT A.col, B.col
# MAGIC 
# MAGIC FROM {LEFT_TABLE} A
# MAGIC 
# MAGIC INNER {JOIN RIGHT_TABLE} B
# MAGIC 
# MAGIC ON A.{col} = B.{col}

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql -- We can see, we have department_id in our employee table which tell us about department of the employee. But what if we want department
# MAGIC -- name instead? We have to put an inner join employee table with department table based on dept id
# MAGIC SELECT
# MAGIC   E.firstname, E.lastname, D.dept_name AS department
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   E.firstname, E.lastname, D.dept_name AS department
# MAGIC FROM
# MAGIC   employee E
# MAGIC   LEFT JOIN department D ON E.dept_id = D.dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   E.firstname, E.lastname, D.dept_name AS department
# MAGIC FROM
# MAGIC   employee E
# MAGIC   RIGHT JOIN department D ON E.dept_id = D.dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   E.firstname, E.lastname, D.dept_name AS department
# MAGIC FROM
# MAGIC   employee E
# MAGIC   FULL JOIN department D ON E.dept_id = D.dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT M.meal_name, D.drink_name
# MAGIC FROM meal M CROSS JOIN drink D

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


