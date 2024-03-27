-- Databricks notebook source
-- MAGIC %md
-- MAGIC Import Level1 namespace catalog

-- COMMAND ----------

USE CATALOG halian_aiq

-- COMMAND ----------

desc extended  aiq_sales.vw_aiq_weather_sales_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns the sales dataset merged with customer and weather datails associated with each sales

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_consolidated_dataset


-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns total salves value for each customer

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_customer_sales_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns top 5 customers

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_top_customer_sales_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns average sale amount per weather

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_weather_sales_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns average quantity sold for each product

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_product_sales_dataset 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View that returns top 5 products

-- COMMAND ----------

SELECT * FROM aiq_sales.vw_aiq_top_product_sales_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Note Period wise trends hve been analysed in Power BI
