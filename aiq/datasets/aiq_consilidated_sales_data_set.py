# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dim_cust.customer_id
# MAGIC 	,dim_cust.name
# MAGIC 	,dim_cust.email
# MAGIC 	,dim_cust.phone
# MAGIC 	,dim_cust.city
# MAGIC 	,dim_cust.customer_website
# MAGIC 	,dim_cust.website
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

# MAGIC %sql
# MAGIC select count(*)  FROM aiq_sales.fct_aiq_sales 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.vw_dim_aiq_customer_weather 

# COMMAND ----------


