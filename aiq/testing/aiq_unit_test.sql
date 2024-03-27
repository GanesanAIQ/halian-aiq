-- Databricks notebook source
-- MAGIC %md
-- MAGIC Fact table count check

-- COMMAND ----------

select count(order_id) from halian_aiq.aiq_sales.vw_fct_aiq_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fsct table quantity customer wise

-- COMMAND ----------

select customer_id, sum(quantity) quantity from halian_aiq.aiq_sales.vw_fct_aiq_sales
group by  customer_id
order by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Customer API Random Record Check

-- COMMAND ----------

select * from  halian_aiq.aiq_sales.vw_dim_aiq_customer_weather
where lattitude = -37.3159 and logitude = 81.1496

-- COMMAND ----------

drop table halian_aiq.aiq_sales.dim_aiq_customer

-- COMMAND ----------

drop table halian_aiq.aiq_sales.dim_aiq_customer_weather

-- COMMAND ----------

--drop table halian_aiq.aiq_sales.fct_aiq_sales

-- COMMAND ----------

--update halian_aiq.aiq_sales.aiq_elt_config set last_load_date = '2000-01-01'

-- COMMAND ----------

desc halian_aiq.aiq_sales.aiq_elt_config

-- COMMAND ----------

select * from halian_aiq.aiq_sales.aiq_elt_config

-- COMMAND ----------


