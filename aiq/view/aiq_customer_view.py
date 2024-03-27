# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC 	OR replace VIEW aiq_sales.vw_dim_aiq_customer_weather AS
# MAGIC
# MAGIC SELECT dim_cust.id AS customer_id
# MAGIC 	,dim_cust.name
# MAGIC 	,dim_cust.email
# MAGIC 	,dim_cust.phone
# MAGIC 	,dim_cust.city
# MAGIC 	,dim_cust.website
# MAGIC 	,dim_cust.company_name
# MAGIC 	,dim_cust_wthr.lat AS lattitude
# MAGIC 	,dim_cust_wthr.lon AS logitude
# MAGIC 	,dim_cust_wthr.TEMP
# MAGIC 	,dim_cust_wthr.humidity
# MAGIC 	,dim_cust_wthr.weather_description
# MAGIC 	,dim_cust_wthr.sunrise_time
# MAGIC 	,dim_cust_wthr.sunset_time
# MAGIC FROM aiq_sales.dim_aiq_customer dim_cust
# MAGIC INNER JOIN aiq_sales.dim_aiq_customer_weather dim_cust_wthr ON dim_cust_wthr.customer_id = dim_cust.id

# COMMAND ----------


