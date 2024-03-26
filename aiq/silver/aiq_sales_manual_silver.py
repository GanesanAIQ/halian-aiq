# Databricks notebook source
# MAGIC %md
# MAGIC Import lib's, global variables and common functions notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/aiq-sales-assignment/halian-aiq/aiq/lib/aiq_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Capture notebook execution start time for logging purpose

# COMMAND ----------

start_time = datetime.now() 

# COMMAND ----------

# MAGIC %md
# MAGIC Get notebook path and notebook name to be used to filter ELT config table

# COMMAND ----------

nb_path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path_split=nb_path.split(sep='/')
nb_name=path_split[len(path_split)-1]

# COMMAND ----------

# MAGIC %md
# MAGIC Get config details to drive the load from ELT config table using notebook name as input

# COMMAND ----------

config_query=f"""
select * from aiq_sales.aiq_elt_config where silver_nb = '{nb_name}'
"""
config_result=spark.sql(config_query).toPandas().to_dict(orient='list')
project_name=config_result['project'][0]
source_file_name=config_result['source_file_name'][0]
adls_bronze_target_path=config_result['adls_path'][0]
adls_silver_target_path=config_result['adls_path'][0]
last_load_date=config_result['last_load_date'][0]


# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_src=path_bronze+adls_bronze_target_path+'*.'+file_format_csv
path_tgt=path_silver+adls_silver_target_path

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from bronze/sales path 

# COMMAND ----------

df_read_sales_bronze=readDataframe(path_src,file_format_csv)

# COMMAND ----------

#last_load_date=date(2023,12 , 1)

# COMMAND ----------

# MAGIC %md
# MAGIC Filter only increment data by using the config table last_load date watermark column 

# COMMAND ----------

df_sales_increment_load=df_read_sales_bronze.filter(df_read_sales_bronze.order_date>last_load_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Append audit columns

# COMMAND ----------

df_sales_to_silver=addAuditColumn(df_sales_increment_load)

# COMMAND ----------

# MAGIC %md
# MAGIC Create partition columns based on order_date 

# COMMAND ----------

df_sales_to_silver = df_sales_to_silver.withColumn(
    "part_year", year("order_date")
).withColumn("part_month", month("order_date"))


# COMMAND ----------

# MAGIC %md
# MAGIC Capture notebook execution end time for logging purpose

# COMMAND ----------

end_time= datetime.now() 

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to target silver path 

# COMMAND ----------

try:
        writeToTarget(df_sales_to_silver, file_format_parquet, path_tgt)
        # insert a log entry for the success case
        log_message = "Data ingestion successful to " + adb_silver_zone
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_silver_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)

except Exception as e:
    output = f"{e}"
    outputres = re.sub("[^a-zA-Z0-9 \n\.]", "", output)
    # insert a log entry for the exception case
    log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_silver_zone}','{start_time}','{end_time}','{outputres}')"""
    spark.sql(log_query)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.aiq_elt_audit_log

# COMMAND ----------


