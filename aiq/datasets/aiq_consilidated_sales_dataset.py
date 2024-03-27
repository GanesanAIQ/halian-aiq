# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Associate customer details and weather details against each sale as suggested in requirement documnet

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view aiq_sales.vw_aiq_consolidated_dataset as
# MAGIC SELECT dim_cust.customer_id
# MAGIC 	,dim_cust.name
# MAGIC 	,dim_cust.email
# MAGIC 	,dim_cust.phone
# MAGIC 	,dim_cust.city
# MAGIC 	,dim_cust.website
# MAGIC 	,dim_cust.company_name
# MAGIC 	,dim_cust.lattitude
# MAGIC 	,dim_cust.logitude
# MAGIC 	,dim_cust.TEMP
# MAGIC 	,dim_cust.humidity
# MAGIC 	,dim_cust.weather_description
# MAGIC 	,dim_cust.sunrise_time
# MAGIC 	,dim_cust.sunset_time
# MAGIC   ,fct_sales.quantity
# MAGIC   ,fct_sales.price
# MAGIC 	,fct_sales.sales_amount
# MAGIC FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC INNER JOIN aiq_sales.vw_dim_aiq_customer_weather dim_cust ON fct_sales.customer_id = dim_cust.customer_id

# COMMAND ----------


