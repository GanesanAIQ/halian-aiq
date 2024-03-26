# Databricks notebook source
# MAGIC %md
# MAGIC Import necessary packages used in this asssignment

# COMMAND ----------

from pyspark.sql.functions import *
from delta import *
from datetime import *
from pyspark.sql.types import *
import re, requests, json, concurrent.futures

# COMMAND ----------

# MAGIC %md
# MAGIC Declare global variables and get seecrets (credentials) 

# COMMAND ----------

storage_account_name = "halianaiq"
container_name = "aiq"
scope_name = "aiq-key-vault-secret"
weather_api_name = "open-weather-api-key"
weather_api_credential = dbutils.secrets.get(scope=scope_name, key=weather_api_name)
tstamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
yr = datetime.now().strftime("%Y")
mth = datetime.now().strftime("%m")
dt = datetime.now().strftime("%d")
date_path = yr + "/" + mth + "/" + dt + "/"
adb_activity_name = "notebook"
adb_bronze_zone = "bronze"
adb_silver_zone = "silver"
adb_gold_zone = "gold"
return_code_success = "success"
return_code_failure = "fail"
fact_tables_prefix = "fct_"
dimension_tables_prefix = "dim_"
lookup_tables_prefix = "lkp_"
file_format_csv = "csv"
file_format_delta = "delta"
file_format_parquet = "parquet"
file_format_json = "json"
elt_config_file_name = "halian_aiq_elt_config.csv"


# COMMAND ----------

# MAGIC %md
# MAGIC Declare path variables to be used in reading and writing data into Data Lake

# COMMAND ----------

path_inbound = "/mnt/" + storage_account_name + "/inbound/"
path_bronze = "/mnt/" + storage_account_name + "/bronze/"
path_silver = "/mnt/" + storage_account_name + "/silver/"
path_gold = 'abfss://'+container_name+'@'+storage_account_name+'.dfs.core.windows.net/gold/'
path_error = "/mnt/" + storage_account_name + "/error/"
path_archive = "/mnt/" + storage_account_name + "/archive/"
path_meta_data = "/mnt/" + storage_account_name + "/metadata/"
path_elt_config_file = "/mnt/" + storage_account_name + "/elt_pipeline_config/"
path_elt_log = "/mnt/" + storage_account_name + "/elt_pipeline_log/"


# COMMAND ----------

# MAGIC %md
# MAGIC Function to Read Dataframe 
# MAGIC <table>
# MAGIC <tr><td><b>Inputs</td><td></td> </tr>
# MAGIC <tr><td>file_path</td>:<td> Path of the file</td></tr>
# MAGIC <tr><td>file_format<td> format of the file which will be red</td> </tr>
# MAGIC <tr><td><b>Output</td><td></td> </tr>
# MAGIC <tr><td>File data will be red and returened as dataframe<td></tr>
# MAGIC </table>
# MAGIC
# MAGIC

# COMMAND ----------

def readDataframe(file_path, file_format):
    df_read = spark.read.load(
        file_path, format=f"{file_format}", header=True, InferSchema=True
    )
    return df_read


# COMMAND ----------

# MAGIC %md
# MAGIC Function to Append audit columns (created date, updated date) to a Dataframe 
# MAGIC <table>
# MAGIC <tr><td><b>Inputs:</td><td></td> </tr>
# MAGIC <tr><td>dataframe</td></tr>
# MAGIC <tr><td><b>Output</td><td></td> </tr>
# MAGIC <tr><td>Dataframes with 2 columns (lh_created_date, lh_updated_date) added in it</td><td></td> </tr>
# MAGIC </table>

# COMMAND ----------

def addAuditColumn(df_name):
    df_audit_date_added = df_name.withColumn(
        "lh_created_date", lit(current_timestamp())
    ).withColumn("lh_updated_date", lit(datetime(1900, 1, 1, 00, 00, 00)))
    return df_audit_date_added


# COMMAND ----------

# MAGIC %md
# MAGIC Function to replace special characters and convert the column names of an input data red as a dataframe 
# MAGIC <table>
# MAGIC <tr><td><b>Inputs:</td><td></td> </tr>
# MAGIC <tr><td>dataframe</td></tr>
# MAGIC <tr><td><b>Output</td><td></td> </tr>
# MAGIC <tr><td>Dataframes column names with special characters will be replaced with underscore and returned</td><td></td> </tr>
# MAGIC </table>

# COMMAND ----------

def replaceColumnNameSpecialCharacters(df_name):
    df_new = df_name.toDF(*[re.sub("[^\w]", " ", col) for col in df_name.columns])
    NewColumns = (
        column.strip()
        .replace(" ", "_")
        .replace("__", "_")
        .replace("___", "_")
        .replace("____", "_")
        .lower()
        for column in df_new.columns
    )
    df_replace_column_name_special_characters = df_new.toDF(*NewColumns)
    return df_replace_column_name_special_characters


# COMMAND ----------

# MAGIC %md
# MAGIC Function to write dataframe in overwrite mode
# MAGIC <table>
# MAGIC <tr><td><b>Inputs</td><td></td> </tr>
# MAGIC <tr><td>df_name<td>Dataframe to be written</td> </tr>
# MAGIC <tr><td>file_format<td>Target format which will be written</td> </tr>
# MAGIC <tr><td>path_tgt</td>:<td>Target path to write</td></tr>
# MAGIC </table>

# COMMAND ----------

def writeToTarget(df_name, file_format, path_tgt):
    df_name.write.format(f"{file_format}").mode("overwrite").option(
        "header", "True"
    ).save(f"{path_tgt}")


# COMMAND ----------

# MAGIC %md
# MAGIC Function to write dataframe in append mode with partition
# MAGIC <table>
# MAGIC <tr><td><b>Inputs</td><td></td> </tr>
# MAGIC <tr><td>df_name<td>Dataframe to be written</td> </tr>
# MAGIC <tr><td>file_format<td>Target format which will be written</td> </tr>
# MAGIC <tr><td>path_tgt</td>:<td>Target path to write</td></tr>
# MAGIC </table>

# COMMAND ----------

def appendToTargetPartition(df_name,file_format,path_tgt):
    df_name.write.format(f"{file_format}")\
              .mode('append')\
              .option('header','True')\
              .partitionBy("part_year","part_month") \
              .save(f"{path_tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC Function to write dataframe in overwrite mode with partition
# MAGIC <table>
# MAGIC <tr><td><b>Inputs</td><td></td> </tr>
# MAGIC <tr><td>df_name<td>Dataframe to be written</td> </tr>
# MAGIC <tr><td>file_format<td>Target format which will be written</td> </tr>
# MAGIC <tr><td>path_tgt</td>:<td>Target path to write</td></tr>
# MAGIC </table>

# COMMAND ----------

def writeToTargetPartition(df_name,file_format,path_tgt):
    df_name.write.format(f"{file_format}")\
              .mode('overwrite')\
              .option('header','True')\
              .partitionBy("part_year","part_month") \
              .save(f"{path_tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC Function to write dataframe in overwrite mode
# MAGIC <table>
# MAGIC <tr><td><b>Inputs</td><td></td> </tr>
# MAGIC <tr><td>df_meta_data<td>Metadata red as a dataframe</td> </tr>
# MAGIC <tr><td>file_format<td>Actual file data as a dataframe</td> </tr>
# MAGIC <tr><td>Output</td></tr>
# MAGIC <tr><td>Returns true if metadata schema and actual file schema matches otherwise false</td></tr>
# MAGIC </table>

# COMMAND ----------

def schemaValidation(df_metadata, df_actual):
    if df_metadata.schema == df_actual.schema:
        return True
    else:
        return False


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog halian_aiq
