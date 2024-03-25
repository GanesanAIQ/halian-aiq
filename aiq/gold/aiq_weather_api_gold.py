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
select * from aiq_sales.aiq_elt_config where gold_nb = '{nb_name}'
"""
config_result=spark.sql(config_query).toPandas().to_dict(orient='list')
project_name=config_result['project'][0]
source_file_name=config_result['source_file_name'][0]
adls_path=config_result['adls_path'][0]



# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_src=path_silver+adls_path
path_tgt=path_gold+lookup_tables_prefix+adls_path

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from silver/weather path 

# COMMAND ----------

df_read_customer_weather_silver=readDataframe(path_src,file_format_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC Create source data as a temp view to merge it with target

# COMMAND ----------

df_read_customer_weather_silver.createOrReplaceTempView('customer_weather_stg')

# COMMAND ----------

# MAGIC %md
# MAGIC Create table query

# COMMAND ----------

qry_create_table=f"""
CREATE TABLE IF NOT EXISTS aiq_sales.dim_aiq_customer_weather USING DELTA LOCATION '{path_tgt}'"""

# COMMAND ----------

# MAGIC %md
# MAGIC Merge table query

# COMMAND ----------

qry_merge_table=f"""
MERGE INTO aiq_sales.dim_aiq_customer_weather tgt
USING (
	SELECT customer_id
		,lat
		,lon
		,TEMP
		,humidity
		,weather_description_long
		,weather_description
		,sunrise_time
		,sunset_time
		,lh_created_date
		,lh_updated_date
	FROM customer_weather_stg
	) stg
	ON stg.customer_id = tgt.customer_id
WHEN MATCHED
	THEN
		UPDATE
		SET tgt.customer_id = stg.customer_id
			,tgt.lat = stg.lat
			,tgt.lon = stg.lon
			,tgt.TEMP = stg.TEMP
			,tgt.humidity = stg.humidity
			,tgt.weather_description_long = stg.weather_description_long
			,tgt.weather_description = stg.weather_description
			,tgt.sunrise_time = stg.sunrise_time
			,tgt.sunset_time = stg.sunset_time
			,tgt.lh_created_date = stg.lh_created_date
			,tgt.lh_updated_date = current_timestamp()
WHEN NOT MATCHED
	THEN
		INSERT *
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Capture notebook execution end time for logging purpose

# COMMAND ----------

end_time= datetime.now() 

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to target gold path as delta table

# COMMAND ----------

try:
    # check if the target is a delta table, if its yes them already data is available it can be merged
    if DeltaTable.isDeltaTable(spark, path_tgt):
        print("Executing Delta Merge")
        spark.sql(qry_merge_table)
    else:
        # if no then we are writing the data first time then call write to target function and execute the create table query
        print("Writing the data first time into gold")
        writeToTarget(df_read_customer_weather_silver, file_format_delta, path_tgt)
        spark.sql(qry_create_table)
    # insert a log entry for the success case
    log_message = "Data ingestion successful to " + adb_gold_zone
    log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_gold_zone}','{start_time}','{end_time}','{log_message}')"""
    spark.sql(log_query)
except Exception as e:
    output = f"{e}"
    outputres = re.sub("[^a-zA-Z0-9 \n\.]", "", output)
    # insert a log entry for the exception case
    log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_gold_zone}','{start_time}','{end_time}','{outputres}')"""
    spark.sql(log_query)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.dim_aiq_customer_weather

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.aiq_elt_audit_log

# COMMAND ----------


