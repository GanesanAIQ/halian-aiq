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

# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_src=path_bronze+adls_bronze_target_path
path_tgt=path_silver+adls_silver_target_path
path_error=path_error+adls_silver_target_path

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from bronze/customer path 

# COMMAND ----------

df_read_customer_bronze=readDataframe(path_src,file_format_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the JSON structure to flat dataframe

# COMMAND ----------

df_flat_customer = df_read_customer_bronze.select(
    "id",
    "name",
    "username",
    "email",
    "address.city",
    "address.geo.lat",
    "address.geo.lng",
    "address.street",
    "address.suite",
    "phone",
    "website",
    col("company.name").alias("company_name"),
    col("company.bs").alias("company_bs"),
    col("company.catchPhrase").alias("company_catch_phrase"),
)


# COMMAND ----------

# MAGIC %md
# MAGIC Append audit columns

# COMMAND ----------

df_customer_audit_column=addAuditColumn(df_flat_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC Data quality check on e-mail id, and split the records into good and bad based on quality check outcome

# COMMAND ----------

df_customer_audit_column = df_customer_audit_column.withColumn(
    "data_quality",
    when(df_customer_audit_column["email"].contains("@"), lit("good")).otherwise(
        lit("bad")
    ),
).filter(col("data_quality") == "good")
df_customer_bad_records = df_customer_audit_column.filter(
    col("data_quality") == "bad"
).withColumn("quality_violation", lit("email"))
df_customer_to_silver = df_customer_audit_column.filter(
    col("data_quality") == "good"
).drop("data_quality")


# COMMAND ----------

# MAGIC %md
# MAGIC Write the bad records into error folder

# COMMAND ----------

if not(df_customer_bad_records.isEmpty()):
    writeToTarget(df_customer_bad_records, file_format_csv, path_tgt)

# COMMAND ----------

#df_customer_to_silver=df_customer_to_silver.filter(df_customer_to_silver.id =='ab')

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
    # check if the data is available to write, if yes then write
    if not (df_customer_to_silver.isEmpty()):
        writeToTarget(df_customer_to_silver, file_format_parquet, path_tgt)
        # insert a log entry for the success case
        log_message = "Data ingestion successful to " + adb_silver_zone
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_silver_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)

    else:
        # insert a log entry for the failure case
        log_message = (
            "Data is not ingested to "
            + adb_silver_zone
            + " check the notebook "
            + nb_path
        )
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_silver_zone}','{start_time}','{end_time}','{log_message}')"""
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


