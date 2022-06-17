# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### What is unit testing?
# MAGIC UNIT TESTING is a type of software testing where individual units or components of a software are tested. The purpose is to validate that each unit of the software code performs as expected. Unit Testing is done during the development (coding phase) of an application by the developers.
# MAGIC 
# MAGIC unit testing
# MAGIC By
# MAGIC TechTarget Contributor
# MAGIC Unit testing is a software development process in which the smallest testable parts of an application, called units, are individually and independently scrutinized for proper operation. This testing methodology is done during the development process by the software developers and sometimes QA staff.  The main objective of unit testing is to isolate written code to test and determine if it works as intended.
# MAGIC 
# MAGIC Unit testing is an important step in the development process, because if done correctly, it can help detect early flaws in code which may be more difficult to find in later testing stages.
# MAGIC 
# MAGIC Unit testing is a component of test-driven development (TDD), a pragmatic methodology that takes a meticulous approach to building a product by means of continual testing and revision. This testing method is also the first level of software testing, which is performed before other testing methods such as integration testing. Unit tests are typically isolated to ensure a unit does not rely on any external code or functions. Testing can be done manually but is often automated.
# MAGIC 
# MAGIC How unit tests work
# MAGIC A unit test typically comprises of three stages: plan, cases and scripting and the unit test itself. In the first step, the unit test is prepared and reviewed. The next step is for the test cases and scripts to be made, then the code is tested.
# MAGIC 
# MAGIC Test-driven development requires that developers first write failing unit tests. Then they write code and refactor the application until the test passes. TDD typically results in an explicit and predictable code base.
# MAGIC 
# MAGIC 
# MAGIC Each test case is tested independently in an isolated environment, as to ensure a lack of dependencies in the code. The software developer should code criteria to verify each test case, and a testing framework can be used to report any failed tests. Developers should not make a test for every line of code, as this may take up too much time. Developers should then create tests focusing on code which could affect the behavior of the software being developed.
# MAGIC 
# MAGIC Unit testing involves only those characteristics that are vital to the performance of the unit under test. This encourages developers to modify the source code without immediate concerns about how such changes might affect the functioning of other units or the program as a whole. Once all of the units in a program have been found to be working in the most efficient and error-free manner possible, larger components of the program can be evaluated by means of integration testing. Unit tests should be performed frequently, and can be done manually or can be automated.
# MAGIC 
# MAGIC ### Types of unit testing
# MAGIC * Unit tests can be performed manually or automated. Those employing a manual method may have an instinctual document made detailing each step in the process; however, automated testing is the more common method to unit tests. Automated approaches commonly use a testing framework to develop test cases. These frameworks are also set to flag and report any failed test cases while also providing a summary of test cases.
# MAGIC 
# MAGIC ### Advantages and disadvantages of unit testing
# MAGIC Advantages to unit testing include:                              
# MAGIC * The earlier a problem is identified, the fewer compound errors occur.
# MAGIC * Costs of fixing a problem early can quickly outweigh the cost of fixing it later.
# MAGIC * Debugging processes are made easier.
# MAGIC * Developers can quickly make changes to the code base.
# MAGIC * Developers can also re-use code, migrating it to new projects.
# MAGIC 
# MAGIC ### Concepts in an object-oriented way for Python Unittest
# MAGIC * Test fixture- the preparation necessary to carry out test(s) and related cleanup actions.
# MAGIC * Test case- the individual unit of testing.
# MAGIC * A Test suite- collection of test cases, test suites, or both.
# MAGIC * Test runner- component for organizing the execution of tests and for delivering the outcome to the user.
# MAGIC 
# MAGIC __*always start unittest function name with "test_".*__

# COMMAND ----------

#Creating Add function for addition
def add(a, b):
    return a + b
#Creating multi function for multiplication
def multi(a,b):
    return a*b

def subt(a,b):
    return a-b

# COMMAND ----------

import unittest 

#Creating Class for Unit Testing
class test_class(unittest.TestCase):
    def test_add(self):
          self.assertEqual(10, add(7, 3))
            
    def test_multi(self):
          self.assertEqual(25,multi(5,5))
            
    @unittest.skip("OBSELETE METHOD")
    def test_multi(self):
          self.assertEqual(25,multi(5,5))
    
# create a test suite  using loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
#Running test cases using Test Cases Suit.
p = (unittest.TextTestRunner(verbosity=2).run(suite))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo
# MAGIC Let's perform unit testing with real dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_init_setup

# COMMAND ----------

df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
df_od = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderdetails.csv")
df_st = spark.read.option("header", "true").csv("/FileStore/datasets/sales/salestarget.csv")

# COMMAND ----------

from pyspark.sql.functions import col, round

# COMMAND ----------

# MAGIC %md
# MAGIC ### joining order list and order details.

# COMMAND ----------

df_join = df_ol\
        .join(df_od, df_ol["Order ID"]==df_od["Order ID"], "inner")\
        .withColumn("Profit", col("Profit").cast("decimal(10,2)"))\
        .withColumn("Amount", col("Amount").cast("decimal(10,2)"))\
        .select(df_ol["Order ID"], "Amount", "Profit", "Quantity", "Category", "State", "City")\
        .limit(50)
display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing smaller dataset for unit testing

# COMMAND ----------

df_ut = df_join.limit(50)  # defining dataframe to perform our unit test. We will tes our functions for a smaller dataset that is why we are only taking 50 records.

display(df_ut)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions to calculate total & average sales

# COMMAND ----------

# function calculates average profit for each state, round upto 2 decimal digits
def state_avg_profit(df):
    return df.groupBy("State").mean("Profit").withColumn("avg(Profit)", round(col("avg(Profit)"), 2)).orderBy("State")

# calculates total sales for each state
def state_total_sales(df):
    return df.groupBy("State").sum("Amount").withColumn("sum(Amount)", round(col("sum(Amount)"), 2)).orderBy("State")

# COMMAND ----------

# df_avg_profit consists average sale for each state. Calculating average & total sales for our testing dataframe
df_avg_profit = state_avg_profit(df_ut)
display(df_avg_profit)

df_total_sales = state_total_sales(df_ut)
display(df_total_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Now our aim is to test our state_avg_profit() function.
# MAGIC We will compare Gujarat's average sale & total sale. for testing purpose I have calculated Gujarat's average profit in an excel sheet(for out unit testing dataframe (50 records) ). It should be -225.6.

# COMMAND ----------

# Defining variable which include average & total sales of gujarat.

# COMMAND ----------

avg_sale_guj = float(df_avg_profit.where(col("State")=="Gujarat" ).collect()[0]["avg(Profit)"])
total_sale_guj = float(df_total_sales.where(col("State")=="Gujarat" ).collect()[0]["sum(Amount)"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining unit test  & performing 

# COMMAND ----------

import unittest 

#Creating Class for Unit Testing
class sales_unit_class(unittest.TestCase):
    
    def test_avg_profit(self):
        expected_avg_sales = -225.60 # This is the value which is expected we calculated using excel
        self.assertEqual(expected_avg_sales, avg_sale_guj)
    
    def test_record_count(self):
        # this is the expected sale, I have put wrong value to fail this test. Correct value should be 1782.0
        #If you will change it to correct value, it will pass the unit test.
        expected_total_sales = 1789.0 
        self.assertEqual(expected_total_sales, total_sale_guj)
    
# create a test suite  for test_class using  loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(sales_unit_class)
#Running test cases using Test Cases Suit..
unittest.TextTestRunner(verbosity=2).run(suite)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attention:
# MAGIC __*This is for demo purpose that is why we are writing unit testing in the same notebook but it is always prefered to write unit testin in a separate notebook.*__

# COMMAND ----------

# MAGIC %run ../SETUP/_pyspark_clean_up

# COMMAND ----------


