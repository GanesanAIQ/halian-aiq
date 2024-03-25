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
# MAGIC
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
select * from aiq_sales.aiq_elt_config where bronze_nb = '{nb_name}'
"""
config_result=spark.sql(config_query).toPandas().to_dict(orient='list')
project_name=config_result['project'][0]
source_file_name=config_result['source_file_name'][0]
adls_bronze_target_path=config_result['adls_path'][0]
api_end_point= config_result['endpoint_if_api'][0]
api_query_parm= config_result['api_qry_parm'][0]
customer_source=config_result['adls_inbound_path'][0] 


# COMMAND ----------

# MAGIC %md
# MAGIC Declare the ADLS paths to read/write data 

# COMMAND ----------

path_src=path_silver+customer_source
path_tgt=path_bronze+adls_bronze_target_path

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from customer silver data to lookup the weather details using lat and long of customer data

# COMMAND ----------

df_read_customer_silver=readDataframe(path_src,file_format_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the API URL using the ELT config values api_end_point and api_query_parm

# COMMAND ----------

df_read_customer_create_url=df_read_customer_silver.withColumn('api_url',concat(lit(api_end_point), lit(api_query_parm)))

# COMMAND ----------

# MAGIC %md
# MAGIC Create URL's for each customer entries by dynamically assigning the lat and log to the above generated API URL along with the API credential stored in Databricks scope credential (extracted in lib and stored as a variable)

# COMMAND ----------

df_customer_weather_api= df_read_customer_create_url.withColumn(
    "weather_api_url",
    format_string(
        api_end_point+api_query_parm,
        col('lat'), col('lng'), lit(weather_api_credential)
    )
).drop('api_url')

# COMMAND ----------

# MAGIC %md
# MAGIC Function to request API which will be passed for cuncurrent execution and will return the api response as JSON against each customer id

# COMMAND ----------

def apiRequestUsingGeoLocation(id,url):
    if(requests.request("GET",url).status_code==200):
        json_api_data = requests.request("GET",url)
        json_data = json_api_data.json()
        json_string = json.dumps(json_data)
        return (id,json_string)
    else:
        return (id,None)

# COMMAND ----------

# MAGIC %md
# MAGIC Invoke the API request for each customer using python multi thread

# COMMAND ----------

# declare a dictionary that has the customer id and the api url to invoke the apiRequestUsingGeoLocation function
dict = {
    row[0]: row[1]
    for row in df_customer_weather_api.select("id", "weather_api_url").collect()
}

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Submit each url task to the thread pool since loop will violate the spark's parellel processing
    futures = [
        executor.submit(apiRequestUsingGeoLocation, key, value)
        for key, value in dict.items()
    ]
    # Wait for all tasks to complete and retrieve the results
    results = [future.result() for future in concurrent.futures.as_completed(futures)]
# store the api weathere response agaist each customer id as a json dataframe
columns = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("weather_api_response", StringType(), True),
    ]
)
df_customer_weather_to_bronze = spark.createDataFrame(results, columns)


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
    # validate the dataframe to be written is not empty then write
    if not (df_customer_weather_to_bronze.isEmpty()):
        writeToTarget(df_customer_weather_to_bronze, file_format_json, path_tgt)
        # insert a log entry for the success case
        log_message = "Data ingestion successful to " + adb_bronze_zone
        log_query = f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_bronze_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)
    else:
        # insert a log entry for the failure case
        log_message = (
            "Data is not ingested to "
            + adb_bronze_zone
            + " check the notebook "
            + nb_path
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


