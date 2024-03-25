# Databricks notebook source
# MAGIC %md
# MAGIC Import lib's, global variables and common functions notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_elt_config_file_name=path_elt_config_file+'/'+elt_config_file_name
path_tgt=path_gold+'elt_config_delta'

# COMMAND ----------

# MAGIC %md
# MAGIC Read ELT Config File

# COMMAND ----------

df_read_config=readDataframe(path_elt_config_file_name,file_format_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the dataframe with casting to proper data types

# COMMAND ----------

df_transform_config=df_read_config.withColumn('last_load_date',to_date(df_read_config.last_load_date)).withColumn('created_date',to_date(df_read_config.created_date)).withColumn('updated_date',to_date(df_read_config.updated_date)).withColumn('is_active',df_read_config.is_active.cast(BooleanType()))

# COMMAND ----------

# MAGIC %md
# MAGIC Create database

# COMMAND ----------

qry_create_db=f"""
CREATE DATABASE IF NOT EXISTS aiq_sales
"""

# COMMAND ----------

spark.sql(qry_create_db)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temp view of the elt config file red

# COMMAND ----------

df_transform_config.createOrReplaceTempView('elt_config_stg')

# COMMAND ----------

# MAGIC %md
# MAGIC Create table query

# COMMAND ----------

qry_create_table=f"""
CREATE TABLE IF NOT EXISTS aiq_sales.aiq_elt_config USING DELTA LOCATION '{path_tgt}'
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Merge table query

# COMMAND ----------

qry = f"""
MERGE INTO aiq_sales.aiq_elt_config tgt
USING 
(
  select
  config_key
  ,project
  ,source
  ,source_type
  ,source_desc
  ,source_file_name
  ,source_format
  ,index_if_excel
  ,endpoint_if_api
  ,api_qry_parm
  ,source_query_if_db
  ,data_nature
  ,adls_storage_account
  ,adls_container
  ,adls_inbound_path
  ,adls_path
  ,bronze_nb
  ,silver_nb
  ,gold_nb
  ,target_schema
  ,target_table
  ,last_load_date
  ,is_active
  ,created_date
  ,updated_date
from elt_config_stg
) stg
ON tgt.config_key = stg.config_key
WHEN MATCHED THEN UPDATE SET
  tgt.config_key = stg.config_key
  ,tgt.project = stg.project
  ,tgt.source = stg.source
  ,tgt.source_type = stg.source_type
  ,tgt.source_desc = stg.source_desc
  ,tgt.source_file_name = stg.source_file_name
  ,tgt.source_format = stg.source_format
  ,tgt.index_if_excel = stg.index_if_excel
  ,tgt.endpoint_if_api = stg.endpoint_if_api
  ,tgt.api_qry_parm = stg.api_qry_parm
  ,tgt.source_query_if_db = stg.source_query_if_db
  ,tgt.data_nature = stg.data_nature
  ,tgt.adls_storage_account = stg.adls_storage_account
  ,tgt.adls_container = stg.adls_container
  ,tgt.adls_inbound_path = stg.adls_inbound_path
  ,tgt.adls_path = stg.adls_path
  ,tgt.bronze_nb = stg.bronze_nb
  ,tgt.silver_nb = stg.silver_nb
  ,tgt.gold_nb = stg.gold_nb
  ,tgt.target_schema = stg.target_schema
  ,tgt.target_table = stg.target_table
  ,tgt.last_load_date = stg.last_load_date
  ,tgt.is_active = stg.is_active
  ,tgt.created_date = stg.created_date
  ,tgt.updated_date = current_date()

WHEN NOT MATCHED
  THEN INSERT *
"""


# COMMAND ----------

try:
    if DeltaTable.isDeltaTable(spark, path_tgt):
        print("Executing Delta Merge")
        spark.sql(qry)
    else:
        print("Writing the data first time into delta table")
        writetotarget(df_transform_config, file_format_delta, path_tgt)
        spark.sql(qry_create_table)
except Exception as e:
    output = f"{e}"
    outputres = re.sub("[^a-zA-Z0-9 \n\.]", "", output)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.aiq_elt_config

# COMMAND ----------

# MAGIC %md
# MAGIC Create audit log table

# COMMAND ----------


qry_create_audit_log=f"""CREATE TABLE IF NOT EXISTS aiq_sales.aiq_elt_audit_log (
project_name STRING,
file_name STRING,
notebook_name STRING,
notebook_path STRING,
notebook_status STRING,
zone STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
log_message STRING
)
USING delta
LOCATION '{path_elt_log}'"""

# COMMAND ----------

spark.sql(qry_create_audit_log)
