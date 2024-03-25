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

# COMMAND ----------

# MAGIC %md
# MAGIC Read source data from inbound/customer path 

# COMMAND ----------

df_read_customer_weather_bronze=readDataframe(path_src,file_format_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the data stored as Json string to Json object using the schema of actual file data (sample record)

# COMMAND ----------

schema = schema_of_json(
    """{"coord": {"lon": 62.5342, "lat": -31.8129}, "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10n"}], "base": "stations", "main": {"temp": 294.12, "feels_like": 294.31, "temp_min": 294.12, "temp_max": 294.12, "pressure": 1019, "humidity": 78, "sea_level": 1019, "grnd_level": 1019}, "visibility": 10000, "wind": {"speed": 12.5, "deg": 156, "gust": 13.2}, "rain": {"1h": 0.11}, "clouds": {"all": 100}, "dt": 1711117477, "sys": {"sunrise": 1711072474, "sunset": 1711115924}, "timezone": 14400, "id": 0, "name": "", "cod": 200}"""
)

df_customer_weather_bronze = df_read_customer_weather_bronze.select(
    "customer_id",
    from_json(col("weather_api_response"), schema).alias("weather_api_response_data"),
)


# COMMAND ----------

# MAGIC %md
# MAGIC Flaten the Json structure to the dataframe table structure

# COMMAND ----------

df_customer_weather_flat = df_customer_weather_bronze.select(
    "customer_id",
    "weather_api_response_data.coord.lat",
    "weather_api_response_data.coord.lon",
    "weather_api_response_data.main.temp",
    "weather_api_response_data.main.humidity",
    "weather_api_response_data.sys.sunrise",
    "weather_api_response_data.sys.sunset",
    explode("weather_api_response_data.weather").alias("explode_weather"),
    col("explode_weather.description").alias("weather_description_long"),
    col("explode_weather.main").alias("weather_description"),
).drop("explode_weather")


# COMMAND ----------

# MAGIC %md
# MAGIC Transform the sunraise and sunset times stored as epoch time to the clock time

# COMMAND ----------

df_customer_weather_transformed = (
    df_customer_weather_flat.withColumn("sunrise_time", to_timestamp("sunrise"))
    .withColumn("sunset_time", to_timestamp("sunset"))
    .drop("sunrise", "sunset")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Append audit column

# COMMAND ----------

df_customer_weather_to_silver=addAuditColumn(df_customer_weather_transformed)

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
    #check if the data is available to write, if yes then write
    if not(df_customer_weather_to_silver.isEmpty()):
        writeToTarget(df_customer_weather_to_silver, file_format_parquet, path_tgt)
        #insert a log entry for the success case
        log_message = 'Data ingestion successful to '+adb_silver_zone
        log_query= f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_success}','{adb_silver_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)

    else: 
        #insert a log entry for the failure case
        log_message = 'Data is not ingested to '+adb_silver_zone+' check the notebook '+nb_path
        log_query= f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_silver_zone}','{start_time}','{end_time}','{log_message}')"""
        spark.sql(log_query)
except Exception as e:
    output = f"{e}"  
    outputres=re.sub('[^a-zA-Z0-9 \n\.]', '', output)
    #insert a log entry for the exception case
    log_query= f""" INSERT INTO aiq_sales.aiq_elt_audit_log VALUES ('{project_name}','{source_file_name}','{nb_name}','{nb_path}', '{return_code_failure}','{adb_silver_zone}','{start_time}','{end_time}','{outputres}')"""
    spark.sql(log_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aiq_sales.aiq_elt_audit_log

# COMMAND ----------


