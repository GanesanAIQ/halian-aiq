# Databricks notebook source
# MAGIC %md
# MAGIC Declare the varibales and assigne the seecre values stored in Databricks scope credentials

# COMMAND ----------

scope_name='aiq-key-vault-secret'
adls_secret = 'halian-aiq-adb-adls'
app_id_secret= 'halian-aiq-adb-adls-app-id'
dir_id_secret= 'halian-aiq-adb-adls-dir-id'
storage_account_name = 'halianaiq'
container_name ='aiq'
service_credential = dbutils.secrets.get(scope=scope_name,key=adls_secret)
app_id_credential = dbutils.secrets.get(scope=scope_name,key=app_id_secret)
dir_id_credential = dbutils.secrets.get(scope=scope_name,key=dir_id_secret)

# spark.conf.set("fs.azure.account.auth.type."+storage_account_name+".dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type."+storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id."+storage_account_name+".dfs.core.windows.net", app_id_credential)
# spark.conf.set("fs.azure.account.oauth2.client.secret."+storage_account_name+".dfs.core.windows.net", service_credential)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+dir_id_credential+"/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a config string to be used in mount

# COMMAND ----------

configs={"fs.azure.account.auth.type":"OAuth","fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider","fs.azure.account.oauth2.client.id":app_id_credential,"fs.azure.account.oauth2.client.secret": service_credential,"fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/"+dir_id_credential+"/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Mount the ADLS storage as a mountpint to be accessed by databricks

# COMMAND ----------


#dbutils.fs.mount(source="abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net",mount_point="/mnt/"+storage_account_name,extra_configs=configs)

# COMMAND ----------

# MAGIC %md
# MAGIC Use the below code if we want to unmount and remount again

# COMMAND ----------

#dbutils.fs.unmount("paste-mount-point-name-here")

# COMMAND ----------

# MAGIC %md
# MAGIC To verify the list of mounts

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC To check the scecret scopes

# COMMAND ----------

#display(dbutils.secrets.list(scope=scope_name))
