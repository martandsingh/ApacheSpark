# Databricks notebook source
# MAGIC %md
# MAGIC ### What is Grouping?
# MAGIC The GROUP BY statement groups rows that have the same values into summary rows, like "find the number of customers in each country". The GROUP BY statement is often used with aggregate functions ( COUNT() , MAX() , MIN() , SUM() , AVG() ) to group the result-set by one or more columns.
# MAGIC 
# MAGIC Keyword: GROUP BY
# MAGIC 
# MAGIC ### What is ordering?
# MAGIC The SQL ORDER BY clause is used to sort the data in ascending or descending order, based on one or more columns. Some databases sort the query results in an ascending order by default.
# MAGIC 
# MAGIC Keyword: ORDER BY
# MAGIC 
# MAGIC ![Grouping](https://raw.githubusercontent.com/martandsingh/images/master/grouping.png)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM club

# COMMAND ----------

# MAGIC %sql -- Let's caculate number of employees for each club
# MAGIC SELECT
# MAGIC   club_id,
# MAGIC   COUNT(1) AS total_members
# MAGIC FROM
# MAGIC   employee
# MAGIC GROUP BY
# MAGIC   club_id

# COMMAND ----------

# MAGIC %sql -- Let's caculate number of employees for each club and sort the result in DECREASING order of total_members
# MAGIC SELECT
# MAGIC   club_id,
# MAGIC   COUNT(1) AS total_members
# MAGIC FROM
# MAGIC   employee
# MAGIC GROUP BY
# MAGIC   club_id
# MAGIC ORDER BY
# MAGIC   total_members DESC

# COMMAND ----------

# MAGIC %sql -- Let's caculate number of employees for each club and sort the result in INCREASING order of total_members
# MAGIC SELECT
# MAGIC   club_id,
# MAGIC   COUNT(1) AS total_members
# MAGIC FROM
# MAGIC   employee
# MAGIC GROUP BY
# MAGIC   club_id
# MAGIC ORDER BY
# MAGIC   total_members

# COMMAND ----------

# MAGIC %md
# MAGIC Above query does not seems perfect, As our user does not know what is C1, C2... so on. We need to specify the name of the club. For that we have to perform an inner join with club table.
# MAGIC Lets find out name of the club and total members. You have to select only club which has more than one member. Arrange them in decreasing order of total members.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   C.club_name,
# MAGIC   COUNT(1) AS total_members
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN club C ON E.club_id = C.club_id
# MAGIC GROUP BY
# MAGIC   C.club_name
# MAGIC HAVING
# MAGIC   total_members > 1
# MAGIC ORDER BY
# MAGIC   total_members DESC

# COMMAND ----------

# MAGIC %md 
# MAGIC We can group & order our resultset based on more than one column. lets group our data based on club & department.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   D.dept_name AS Department,
# MAGIC   C.club_name AS Club,
# MAGIC   COUNT(1) AS total_members
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN club C ON E.club_id = C.club_id
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC GROUP BY
# MAGIC   D.dept_name,
# MAGIC   C.club_name
# MAGIC ORDER BY
# MAGIC   total_members -- so now you can see, there are multiple rows for marketing & IT. As these two department has members from different club.

# COMMAND ----------

# MAGIC %md
# MAGIC Most of the time we use aggregation functions with grouped data. Lets calculate average basic salary of each department. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   employee
# MAGIC order by
# MAGIC   empcode

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   D.dept_name,
# MAGIC   ROUND(AVG(ES.basic_salary), 2) AS AVG_BASIC_SALARY
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN emp_salary ES ON E.empcode = ES.empcode
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC GROUP BY
# MAGIC   D.dept_name
# MAGIC ORDER BY
# MAGIC   AVG_BASIC_SALARY

# COMMAND ----------

# MAGIC %md 
# MAGIC Our finance team is now wants to downsize the company(not a good news for employees... :( ). They want you to calculate total salary distributed by each department.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   D.dept_name,
# MAGIC   ROUND(SUM(ES.basic_salary), 2) AS TOTAL_BASIC_SALARY
# MAGIC FROM
# MAGIC   employee E
# MAGIC   INNER JOIN emp_salary ES ON E.empcode = ES.empcode
# MAGIC   INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC GROUP BY
# MAGIC   D.dept_name
# MAGIC ORDER BY
# MAGIC   TOTAL_BASIC_SALARY

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


