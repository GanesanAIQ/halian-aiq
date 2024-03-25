# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select fct_sales.product_id, avg(fct_sales.quantity) average__order_quantity from  aiq_sales.fct_aiq_sales fct_sales
# MAGIC group by fct_sales.product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC with sales_product as
# MAGIC (
# MAGIC select fct_sales.product_id, avg(fct_sales.quantity*fct_sales.price) sales_amount  from  aiq_sales.fct_aiq_sales fct_sales
# MAGIC group by fct_sales.product_id
# MAGIC ) 
# MAGIC select product_id, sales_amount from
# MAGIC (
# MAGIC select product_id, sales_amount, dense_rank() over(order by sales_amount desc) as sales_rank from sales_product
# MAGIC )
# MAGIC where sales_rank<=5

# COMMAND ----------


