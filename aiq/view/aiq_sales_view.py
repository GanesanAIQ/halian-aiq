# Databricks notebook source
# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC 	OR replace VIEW aiq_sales.vw_fct_aiq_sales AS
# MAGIC
# MAGIC SELECT *
# MAGIC 	,cast(price * quantity AS DECIMAL(18, 6)) sales_amount
# MAGIC FROM aiq_sales.fct_aiq_sales

# COMMAND ----------


