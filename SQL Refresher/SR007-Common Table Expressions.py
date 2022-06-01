# Databricks notebook source
# MAGIC %md
# MAGIC ### What is common table expression (CTE)?
# MAGIC A Common Table Expression (or CTE) is a feature in several SQL versions to improve the maintainability and readability of an SQL query.
# MAGIC 
# MAGIC It is also known as:
# MAGIC 1. Common Table Expression
# MAGIC 1. Subquery Factoring
# MAGIC 1. SQL WITH Clause
# MAGIC 
# MAGIC A Common Table Expression (or CTE) is a query you can define within another SQL query. 
# MAGIC 
# MAGIC ### What is the difference between Subquery & CTE?
# MAGIC If you are new to SQL concept, then it may sound like a subquery (believe me you are totally normal, it happened to me too :p). A CTE also generates a result that contains rows and columns of data. The difference is that you can give this result a name, and you can refer to it multiple times within your main query. So in other words, CTE provides you a named resultset.
# MAGIC 
# MAGIC You can use a CTE in:
# MAGIC 1. SELECT
# MAGIC 1. INSERT
# MAGIC 1. UPDATE
# MAGIC 1. DELETE
# MAGIC 
# MAGIC 
# MAGIC ![cte](https://raw.githubusercontent.com/martandsingh/images/master/cte.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql WITH cte_department_employee_count AS(
# MAGIC   SELECT
# MAGIC     D.dept_name AS Department,
# MAGIC     COUNT(1) AS `Total Members`
# MAGIC   FROM
# MAGIC     employee E
# MAGIC     INNER JOIN department D ON E.dept_id = D.dept_id
# MAGIC   GROUP BY
# MAGIC     D.dept_name
# MAGIC   ORDER BY
# MAGIC     `Total Members` DESC
# MAGIC ) -- so here we can use cte_department_employee_count as our named result set immediately after CTE is defined.
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   cte_department_employee_count;
# MAGIC -- Keep in mind once you reed CTE, it will dissappear & throw error if you try to run it again.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nested CTE
# MAGIC You can use nested CTE if you want to reuse your CTE in another.
# MAGIC 
# MAGIC Syntax:
# MAGIC 
# MAGIC WITH CTE1 AS (
# MAGIC   
# MAGIC   {Query1}
# MAGIC   
# MAGIC ),
# MAGIC 
# MAGIC CTE2 AS (
# MAGIC 
# MAGIC   {QUERY2}
# MAGIC   
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM CTE1;
# MAGIC 
# MAGIC SELECT * FROM CTE2;
# MAGIC 
# MAGIC let's try it out.

# COMMAND ----------

# MAGIC %sql -- let's create one CTE which will calculate department wise salary. We will use full join so that we can include department which does not exists, this will generate NULL values for AVG salary field. In the second expression we will remove those invalid rows (basic salary with null values). This can be done in a simpler way using a single query but for the sake of tutorial, I am using 2 different CTE to achevie this task, but in real life scenario CTE is not a good way to acheive this. You can simple do this by one group statement & filter (WHERE). You can see in the below query we applied outer join (just for the sake of tutorial, do not think about the business logic here). You can see many invalid rows with dept_name and AVG_BASIC_SALARY as null. Now we will change this query to CTE & using another CTE we will clean these rows.
# MAGIC --WITH cte_dept_salary AS(
# MAGIC SELECT
# MAGIC   D.dept_name,
# MAGIC   AVG(ES.basic_salary) AS AVG_BASIC_SALARY
# MAGIC FROM
# MAGIC   employee E FULL
# MAGIC   JOIN emp_salary ES ON E.empcode = ES.empcode FULL
# MAGIC   JOIN department D ON E.dept_id = D.dept_id
# MAGIC GROUP BY
# MAGIC   D.dept_name --)

# COMMAND ----------

# MAGIC %sql WITH cte_avg_salary AS (
# MAGIC   SELECT
# MAGIC     D.dept_name,
# MAGIC     AVG(ES.basic_salary) AS AVG_BASIC_SALARY
# MAGIC   FROM
# MAGIC     employee E FULL
# MAGIC     JOIN emp_salary ES ON E.empcode = ES.empcode FULL
# MAGIC     JOIN department D ON E.dept_id = D.dept_id
# MAGIC   GROUP BY
# MAGIC     D.dept_name
# MAGIC ),
# MAGIC cte_avg_salary_clean AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     cte_avg_salary
# MAGIC   WHERE
# MAGIC     dept_name IS NOT NULL
# MAGIC     AND AVG_BASIC_SALARY IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   cte_avg_salary_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ### CTE with View
# MAGIC You can also create view using your CTE.
# MAGIC 
# MAGIC Syntax:
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW {ViewName} AS
# MAGIC 
# MAGIC WITH CTE AS (
# MAGIC 
# MAGIC   {COMPLEX QUERY}
# MAGIC   
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM CTE

# COMMAND ----------

# MAGIC %sql -- We can use CTE with views also. Let's use above nested cte in a view
# MAGIC CREATE
# MAGIC OR REPLACE VIEW VW_DEPT_SALARY AS WITH cte_avg_salary AS (
# MAGIC   SELECT
# MAGIC     D.dept_name,
# MAGIC     AVG(ES.basic_salary) AS AVG_BASIC_SALARY
# MAGIC   FROM
# MAGIC     employee E FULL
# MAGIC     JOIN emp_salary ES ON E.empcode = ES.empcode FULL
# MAGIC     JOIN department D ON E.dept_id = D.dept_id
# MAGIC   GROUP BY
# MAGIC     D.dept_name
# MAGIC ),
# MAGIC cte_avg_salary_clean AS (
# MAGIC   SELECT
# MAGIC     dept_name AS Department,
# MAGIC     ROUND(AVG_BASIC_SALARY, 2) AS AVG_BASIC_SALARY
# MAGIC   FROM
# MAGIC     cte_avg_salary
# MAGIC   WHERE
# MAGIC     dept_name IS NOT NULL
# MAGIC     AND AVG_BASIC_SALARY IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   cte_avg_salary_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   VW_DEPT_SALARY;

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


