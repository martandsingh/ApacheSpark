# Databricks notebook source
# MAGIC %md
# MAGIC ### What is CRUD?
# MAGIC CRUD stands for CREATE/RETRIEVE/UPDATE/DELETE. In this demo we will see how can we create a table and perform basic operations on it.
# MAGIC 
# MAGIC ![CRUD](https://raw.githubusercontent.com/martandsingh/images/master/crud.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE A TABLE. Below command will create student table with three columns. IF NOT EXISTS clause will create table only if it does not exists, if you have table then it will ignore the statement.
# MAGIC CREATE  TABLE IF NOT EXISTS students(
# MAGIC   student_id VARCHAR(10),
# MAGIC   student_name VARCHAR(50),
# MAGIC   course VARCHAR(50)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check table details. it will return you table information like column name, datatypes
# MAGIC DESCRIBE students

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's add some data.
# MAGIC INSERT INTO students
# MAGIC (student_id, student_name, course)
# MAGIC VALUES
# MAGIC ('ST001', 'ABC', 'BBA'),
# MAGIC ('ST002', 'XYZ', 'MBA'),
# MAGIC ('ST003', 'PQR', 'BCA');
# MAGIC --above statement will insert three records to student tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query students table
# MAGIC SELECT * FROM students;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now lets update cours for student_id ST003. The student wants to change his/her course in the middle of semester. Now we have to update course in the student table.
# MAGIC UPDATE students
# MAGIC SET course = 'B.Tech'
# MAGIC WHERE student_id = 'ST003'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students
# MAGIC -- course changed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets delete ST003 as he was not happy with the college, he decided to move to another college. So we have to remove his name from the table.
# MAGIC DELETE FROM students WHERE student_id = 'ST003'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# This was a very basic CRUD demo to give you a quick start. You will more detailed queries in further demos. So keep going... You can do it.

# COMMAND ----------


