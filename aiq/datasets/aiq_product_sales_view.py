# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Average Sales Quantity by Product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fct_sales.product_id
# MAGIC 	,cast(avg(fct_sales.quantity) AS DECIMAL(15, 6)) average__order_quantity
# MAGIC FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC GROUP BY fct_sales.product_id

# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 products based on total sales amount

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales_product
# MAGIC AS (
# MAGIC 	SELECT fct_sales.product_id
# MAGIC 		,sum(sales_amount) sales_amount
# MAGIC 	FROM aiq_sales.vw_fct_aiq_sales fct_sales
# MAGIC 	GROUP BY fct_sales.product_id
# MAGIC 	)
# MAGIC SELECT product_id
# MAGIC 	,sales_amount
# MAGIC FROM (
# MAGIC 	SELECT product_id
# MAGIC 		,sales_amount
# MAGIC 		,dense_rank() OVER (
# MAGIC 			ORDER BY sales_amount DESC
# MAGIC 			) AS sales_rank
# MAGIC 	FROM sales_product
# MAGIC 	)
# MAGIC WHERE sales_rank <= 5

# COMMAND ----------


