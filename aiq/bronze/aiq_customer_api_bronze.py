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
api_end_point = config_result["endpoint_if_api"][0]
adls_bronze_target_path = config_result["adls_path"][0]


# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_tgt=path_bronze+adls_bronze_target_path

# COMMAND ----------

# MAGIC %md
# MAGIC Capture notebook execution end time for logging purpose

# COMMAND ----------

end_time= datetime. now() 

# COMMAND ----------

# MAGIC %md
# MAGIC Send a GET request to JSONPlaceholder API and write it into bronze layer as a JSON file

# COMMAND ----------

try:
    # validate the request gives successful response using the status code, if yes then implement further logic
    if requests.request("GET", api_end_point).status_code == 200:
        json_api_data = requests.request("GET", api_end_point)
        json_data = json_api_data.json()

        # pyspark. SparkContext. parallelize is a function in SparkContext that is used to create a Resilient Distributed Dataset (RDD) from a local Python collection. This allows Spark to distribute the data across multiple nodes, instead of depending on a single node to process the data.

        rdd = spark.sparkContext.parallelize([json_data])
        df_customer_api_data = spark.read.json(rdd)

        # write the json response to the target adls path (bronze)
        writeToTarget(df_customer_api_data, file_format_json, path_tgt)

        # custom log message
        log_message = "Data ingestion successful to " + adb_bronze_zone

        # insert a log entry for the success case
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_bronze_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)
    else:
        # insert a log entry for the failure case
        log_message = (
            "Data ingestion has been failed in "
            + adb_bronze_zone
            + " exception occoured while reading REST API data "
        )
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


