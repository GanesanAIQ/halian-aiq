# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.vw_dim_aiq_customer_weather 

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
# MAGIC 	,sum(fct_sales.sales_amount) AS total_sales_amount
# MAGIC FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC INNER JOIN aiq_sales.vw_dim_aiq_customer_weather dim_cust ON fct_sales.customer_id = dim_cust.customer_id
# MAGIC GROUP BY dim_cust.customer_id
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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 customers based on total sales

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales_customers
# MAGIC AS (
# MAGIC 	SELECT dim_cust.customer_id
# MAGIC 		,dim_cust.name
# MAGIC 		,sum(sales_amount) sales_amount
# MAGIC 	FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC 	INNER JOIN aiq_sales.vw_dim_aiq_customer_weather dim_cust ON fct_sales.customer_id = dim_cust.customer_id
# MAGIC 	GROUP BY dim_cust.customer_id
# MAGIC 		,dim_cust.name
# MAGIC 	)
# MAGIC SELECT customer_id
# MAGIC 	,name
# MAGIC 	,sales_amount
# MAGIC FROM (
# MAGIC 	SELECT customer_id
# MAGIC 		,name
# MAGIC 		,sales_amount
# MAGIC 		,dense_rank() OVER (
# MAGIC 			ORDER BY sales_amount DESC
# MAGIC 			) AS sales_rank
# MAGIC 	FROM sales_customers
# MAGIC 	)
# MAGIC WHERE sales_rank <= 5

# COMMAND ----------


