# Databricks notebook source
# MAGIC %md
# MAGIC ### Whats is subquery?
# MAGIC A subquery is a SQL query nested inside a larger query. The subquery can be nested inside a SELECT, INSERT, UPDATE, or DELETE statement or inside another subquery. A subquery is usually added within the WHERE Clause of another SQL SELECT statement.
# MAGIC 
# MAGIC A subquery can be used anywhere an expression is allowed. A subquery is also called an inner query or inner select, while the statement containing a subquery is also called an outer query or outer select.
# MAGIC 
# MAGIC  In Transact-SQL, there is usually no performance difference between a statement that includes a subquery and a semantically equivalent version that does not. For architectural information on how SQL Server processes queries, see SQL statement processing.However, in some cases where existence must be checked, a join yields better performance. Otherwise, the nested query must be processed for each result of the outer query to ensure elimination of duplicates. In such cases, a join approach would yield better results.
# MAGIC  
# MAGIC 
# MAGIC *Note: there are multiple ways to write same query. You have to select the best way. This notebook is specifically for discussing sub queries, so some queries may not make sense but that is just for example. I want to show you multiple ways of generating same result. Later in this series we will have a specific notebook to talk about query execution order & optimization. There we will talk in detail about performance.*
# MAGIC 
# MAGIC ![Subquery](https://raw.githubusercontent.com/martandsingh/images/master/subquery.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %md
# MAGIC -- lets say we want to get all the employees who are the member of existing club(the club exists in club table). There are two ways to achieve this:
# MAGIC 1. Inner Join between employee & club based on club_id
# MAGIC 1. Using sub query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets use join first. This will return full name of all the employees who are member of a valid club(existing club).
# MAGIC SELECT concat(E.firstname, ' ', E.lastname) AS FullName
# MAGIC FROM employee E
# MAGIC INNER JOIN club C
# MAGIC ON E.club_id = C.club_id
# MAGIC ORDER BY FullName

# COMMAND ----------

# MAGIC %sql
# MAGIC -- other way to get same result using sub query or inner query. Below query will return exactly same result as above. The query inside the paranthesis (SELECT club_id FROM club) is your sub query. First this query is executing and providing a resultset which later will be used in WHERE condition for the parent query.
# MAGIC 
# MAGIC SELECT concat(E.firstname, ' ', E.lastname) AS FullName
# MAGIC FROM employee E
# MAGIC WHERE club_id IN (SELECT club_id FROM club)
# MAGIC ORDER BY FullName

# COMMAND ----------

# MAGIC %md
# MAGIC Do not use sub queries blindly, sometimes it is not efficient to use sub queries. As we mentioned earlier, In case of existence check we should prefer JOINS over sub queries. You can compare the execution time of both the queries. We have a very small set of data, which may not show you a significat difference between queries. In real life case where you deal with GB, TB of data, the difference can be huge.
# MAGIC 
# MAGIC Let's take one more example. We have to find out average basic salary of IT department. The output resultset must return only one column which is avg salary for IT department. Let's do this task with inner join & sub query. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT ES.basic_salary FROM 
# MAGIC employee E INNER JOIN emp_salary ES ON E.empcode=  ES.empcode WHERE  dept_id = 'DEP001'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INNER JOIN
# MAGIC SELECT ROUND(AVG(ES.basic_salary), 2) AS AVG_BASIC_SALARY
# MAGIC from employee E
# MAGIC INNER JOIN department D
# MAGIC ON E.dept_id = D.dept_id
# MAGIC INNER JOIN emp_salary ES
# MAGIC ON E.empcode = ES.empcode
# MAGIC GROUP BY D.dept_name
# MAGIC HAVING D.dept_name = 'IT'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Above task using inner query or subquery
# MAGIC SELECT ROUND(AVG(ES.basic_salary), 2) AS AVG_BASIC_SALARY
# MAGIC from employee E
# MAGIC INNER JOIN emp_salary ES
# MAGIC ON E.empcode = ES.empcode
# MAGIC WHERE E.dept_id = ( SELECT dept_id FROM department WHERE dept_name = 'IT')
# MAGIC  

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


