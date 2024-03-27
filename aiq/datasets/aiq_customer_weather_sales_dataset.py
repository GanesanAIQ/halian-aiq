# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Weather wise average sale amount

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW aiq_sales.vw_aiq_weather_sales_dataset AS
# MAGIC SELECT dim_cust.weather_description
# MAGIC 	,cast(avg(fct_sales.sales_amount) as decimal(18,6)) AS avg_sales_amount
# MAGIC FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC INNER JOIN aiq_sales.vw_dim_aiq_customer_weather dim_cust ON fct_sales.customer_id = dim_cust.customer_id
# MAGIC GROUP BY dim_cust.weather_description

# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 customers based on total sales

# COMMAND ----------


