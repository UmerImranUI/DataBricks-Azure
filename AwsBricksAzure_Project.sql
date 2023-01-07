-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://aws-bricks-azure@umerstorage.blob.core.windows.net",
-- MAGIC   mount_point = "/mnt/raw1",
-- MAGIC   extra_configs = {"fs.azure.account.key.umerstorage.blob.core.windows.net":"UkGqcob0z4Gr8aTiJWoyYGrr2C85k1ITbBB62sheUDE6p7spP4LsUuBOkrGSjRR+cVY6NCmlGiYr+AStw/7hvg=="})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import urllib
-- MAGIC 
-- MAGIC 
-- MAGIC df = spark.read.format('csv')\
-- MAGIC .load("/mnt/raw1/Sample - Superstore.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/mnt/raw1/Sample - Superstore.csv"
-- MAGIC file_type = "csv"
-- MAGIC # CSV options
-- MAGIC infer_schema = 'true'
-- MAGIC first_row_is_header = 'true'
-- MAGIC delimiter = ','
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC .option('inferSchema', infer_schema) \
-- MAGIC .option('header', first_row_is_header) \
-- MAGIC .option('sep', delimiter) \
-- MAGIC .load(file_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode('overwrite').saveAsTable('superstore')

-- COMMAND ----------

select * from superstore

-- COMMAND ----------

select City,sum(Profit)  from superstore 
group by city
order by sum(Profit) desc

-- COMMAND ----------

select sum(Profit), Region from superstore
group by Region
order by sum(Profit) desc

-- COMMAND ----------


