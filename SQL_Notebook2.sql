-- Databricks notebook source
show tables

-- COMMAND ----------

select * from data_1_csv



-- COMMAND ----------

select * from data_1_csv where 
Shipping Cost>100

-- COMMAND ----------

select  sum(Sales), Market
group by Market

-- COMMAND ----------

select sum(Sales), Market from data_1_csv
group by Market

-- COMMAND ----------

select * from data_1_csv order by Market

-- COMMAND ----------

select max(Sales) from data_1_csv

-- COMMAND ----------

select sum(Sales) from data_1_csv

-- COMMAND ----------

create or replace temp view filter_data as
select sum(Sales), Market from data_1_csv
group by Market

-- COMMAND ----------

select * from filter_data

-- COMMAND ----------

select *, row_number() over(order by Sales desc) as rn from data_1_csv

-- COMMAND ----------

select *,lead(Sales) over(order by Sales desc) from data_1_csv

-- COMMAND ----------


