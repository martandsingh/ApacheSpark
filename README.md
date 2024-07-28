## Data Engineering Using Azure Databricks

##### This is just a test for feature1 branch (just for my practice). please ignore this pull request.

### Introduction

This course include multiple sections. We are mainly focusing on Databricks Data Engineer certification exam. We have following tutorials:
1. Spark SQL ETL
2. Pyspark ETL

### DATASETS
All the datasets used in the tutorials are available at: https://github.com/martandsingh/datasets

### HOW TO USE?
follow below article to learn how to clone this repository to your databricks workspace.

https://www.linkedin.com/pulse/databricks-clone-github-repo-martand-singh/

### Spark SQL
This course is the first installment of databricks data engineering course. In this course you will learn basic SQL concept which include:
1. Create, Select, Update, Delete tables
1. Create database
1. Filtering data
1. Group by & aggregation
1. Ordering
1. [SQL joins](https://www.scaler.com/topics/sql/joins-in-sql/)
1. Common table expression (CTE)
1. External tables
1. [Sub queries](https://www.geeksforgeeks.org/sql-subquery/)
1. Views & temp views
1. UNION, INTERSECT, EXCEPT keywords
1. Versioning, time travel & optimization

### PySpark ETL
This course will teach you how to perform ETL pipelines using pyspark. ETL stands for Extract, Load & Transformation. We will see how to load data from various sources & process it and finally will load the process data to our destination.

This course includes:
1. Read files
2. Schema handling
3. Handling JSON files
4. Write files
5. Basic transformations
6. partitioning
7. caching
8. joins
9. missing value handling
10. Data profiling
11. date time functions
12. string function
13. deduplication 
14. grouping & aggregation
15. User defined functions
16. Ordering data
17. Case study - sales order analysis



you can download all the notebook from our 

github repo: https://github.com/martandsingh/ApacheSpark

facebook: https://www.facebook.com/codemakerz

email: martandsays@gmail.com

### SETUP folder
you will see initial_setup & clean_up notebooks called in every notebooks. It is mandatory to run both the scripts in defined order. initial script will create all the mandatory tables & database for the demo. After you finish your notebook, execute clean up notebook, it will clean all the db objects.

pyspark_init_setup - this notebook will copy dataset from my github repo to dbfs. It will also generate used car parquet dataset. All the datasets will be avalable at

**/FileStore/datasets**


![d5859667-databricks-logo](https://user-images.githubusercontent.com/32331579/174993501-dc93102a-ec36-4607-a3dc-ab67a54a341b.png)
