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

config_query = f"""
select * from aiq_sales.aiq_elt_config where bronze_nb = '{nb_name}'
"""
config_result = spark.sql(config_query).toPandas().to_dict(orient="list")
project_name = config_result["project"][0]
source_file_name = config_result["source_file_name"][0]
inbound_path = config_result["adls_inbound_path"][0]
adls_bronze_target_path = config_result["adls_path"][0]


# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_src=path_inbound+inbound_path+source_file_name+'.'+file_format_csv
path_meta_data_src=path_meta_data+inbound_path+source_file_name+'.'+file_format_csv 
path_tgt=path_bronze+adls_bronze_target_path

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from inbound/customer path 

# COMMAND ----------

df_read_sales=readDataframe(path_src,file_format_csv)


# COMMAND ----------

# MAGIC %md
# MAGIC Read the metadata file from metadata/customer path

# COMMAND ----------

df_read_sales_meta_data=readDataframe(path_meta_data_src,file_format_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC Capture notebook execution end time for logging purpose

# COMMAND ----------

end_time= datetime.now() 

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to target bronze path 

# COMMAND ----------

try:
    # validate the schema of metadata and actual file is matching, if yes then write it to bronze
    if schemaValidation(df_read_sales_meta_data, df_read_sales):
        writeToTarget(df_read_sales, file_format_csv, path_tgt)

        log_message = "Data ingestion successful to " + adb_bronze_zone
        
        # insert a log entry for the success case
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_bronze_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)
    else:
        log_message = (
            "Data ingestion has been failed in "
            + adb_bronze_zone
            + " Schema mismatch found between actual file received and meta data"
        )

        # insert a log entry for the failure case
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_bronze_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)
except Exception as e:
    output = f"{e}"
    outputres = re.sub("[^a-zA-Z0-9 \n\.]", "", output)
    # insert a log entry for the exception case
    log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_bronze_zone}','{start_time}','{end_time}','{outputres}')"""
    spark.sql(log_query)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.aiq_elt_audit_log

# COMMAND ----------


