# Databricks notebook source
# MAGIC %md
# MAGIC ### UNION
# MAGIC The UNION operator is used to combine the result-set of two or more SELECT statements. It will remove duplicate rows from the final resultset.
# MAGIC 
# MAGIC ### UNION ALL
# MAGIC The UNION operator is used to combine the result-set of two or more SELECT statements. It will include duplicate rows in the final resultset.
# MAGIC 
# MAGIC ### INTERSECT
# MAGIC The INTERSECT clause in SQL is used to combine two SELECT statements but the dataset returned by the INTERSECT statement will be the intersection of the data-sets of the two SELECT statements. In simple words, the INTERSECT statement will return only those rows which will be common to both of the SELECT statements.
# MAGIC 
# MAGIC ### EXCEPT
# MAGIC The SQL EXCEPT operator is used to return all rows in the first SELECT statement that are not returned by the second SELECT statement.
# MAGIC 
# MAGIC *to use all these operations, both the table should have same number of columns & same types of columns*
# MAGIC 
# MAGIC ![Union_Intersection](https://raw.githubusercontent.com/martandsingh/images/master/union.jpg)

# COMMAND ----------

# MAGIC %run ../SETUP/_initial_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM supplier_india

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM supplier_nepal

# COMMAND ----------

# MAGIC %md 
# MAGIC ### UNION DEMO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- here union will combine our both the dataset excluding duplicates. But you must be wondering why Martand Singh  & Gaurav Chadwani are showing twice?
# MAGIC -- The reason behind this is, as you are selecing supp_id & city in your resultset, which makes your row unique than other row. UNION check for duplicate value for the combination of the columns in the resultset. If you remove supp_id & city from query then it will remove duplicate suppliers. Let's try it in the next cell.
# MAGIC 
# MAGIC SELECT supp_id, supp_name, city FROM supplier_nepal
# MAGIC UNION
# MAGIC SELECT supp_id, supp_name, city FROM supplier_india

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now you can see thoe tw duplicate suppliers are gone now. This is because we are only selecting supplier_name
# MAGIC SELECT supp_name FROM supplier_nepal
# MAGIC UNION
# MAGIC SELECT  supp_name FROM supplier_india

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION ALL DEMO
# MAGIC UNION ALL perform same as UNION except union all will include duplicate rows also in resultset.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now you will see duplicate records as we are using UNION ALL. You can see Martand Singh & Gaurav Chandawani are duplicate. If all the records are unique then union and union all behave same. UNION ALL is faster than UNION as it does not have to perform checks for duplicate rows. So until you dont need duplicate check, try to use UNION ALL.
# MAGIC SELECT supp_name FROM supplier_nepal
# MAGIC UNION ALL
# MAGIC SELECT  supp_name FROM supplier_india

# COMMAND ----------

# MAGIC %md
# MAGIC ###INTERSECT DEMO
# MAGIC Let's say we want to find common supplier names. Intersection will give you records which are available in both the tables. Again it will find out common rows based on all the columns in your select query. So if we include city or supplier id, it will not give you any result as all three column makes your record unique.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this will return zero rows
# MAGIC SELECT supp_id, supp_name, city FROM supplier_india
# MAGIC INTERSECT
# MAGIC SELECT supp_id, supp_name, city FROM supplier_nepal

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this will return 2 rows as these two supplier names are common in both the tables.
# MAGIC SELECT supp_name FROM supplier_india
# MAGIC INTERSECT
# MAGIC SELECT supp_name FROM supplier_nepal

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXCEPT DEMO
# MAGIC The SQL EXCEPT operator is used to return all rows in the first SELECT statement that are not returned by the second SELECT statement.
# MAGIC 
# MAGIC Let's find all the suppliers from India which are only managing Indian market.
# MAGIC Or in other words, Let's find all the Indian supplier which are not operating in Nepal.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- so to calculate above query, we will select all the Indian supplier which are not available in nepali supplier table
# MAGIC SELECT supp_name FROM supplier_india
# MAGIC EXCEPT
# MAGIC SELECT supp_name FROM supplier_nepal
# MAGIC 
# MAGIC --Above query says, get all supplier name from India which are not available in Nepal.
# MAGIC --So there are only two suppliers which operation only in India other two (Martand & Gaurav) they operate in Nepal also, as they are available in Nepal supplier list too.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- so if you reverse the query, then it says, get all the supplier from nepal which are only operating in nepal (or not operating in india).
# MAGIC SELECT supp_name FROM supplier_nepal
# MAGIC EXCEPT
# MAGIC SELECT supp_name FROM supplier_india

# COMMAND ----------

# MAGIC %run ../SETUP/_clean_up

# COMMAND ----------


