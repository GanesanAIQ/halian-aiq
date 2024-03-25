# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC desc aiq_sales.dim_aiq_customer_weather

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC dim_cust.id as customer_id,
# MAGIC dim_cust.name, dim_cust.email, dim_cust.phone,dim_cust.city, dim_cust.website ,dim_cust.website, dim_cust_wthr.lat as lattitude,dim_cust_wthr.lon as logitude, dim_cust_wthr.temp, dim_cust_wthr.humidity, dim_cust_wthr.weather_description,
# MAGIC dim_cust_wthr.sunrise_time, dim_cust_wthr.sunset_time, sum(fct_sales.price*fct_sales.quantity) as sales_amount
# MAGIC from aiq_sales.fct_aiq_sales fct_sales
# MAGIC inner join aiq_sales.dim_aiq_customer dim_cust on fct_sales.customer_id=dim_cust.id
# MAGIC inner join aiq_sales.dim_aiq_customer_weather dim_cust_wthr on dim_cust_wthr.customer_id=dim_cust.id 
# MAGIC group by
# MAGIC dim_cust.id,
# MAGIC dim_cust.name, dim_cust.email, dim_cust.phone,dim_cust.city, dim_cust.website ,dim_cust.website, dim_cust_wthr.lat ,dim_cust_wthr.lon , dim_cust_wthr.temp, dim_cust_wthr.humidity, dim_cust_wthr.weather_description,dim_cust_wthr.sunrise_time, dim_cust_wthr.sunset_time 

# COMMAND ----------


