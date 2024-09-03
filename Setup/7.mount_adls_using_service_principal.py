# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting Azure datalake using Service Principal

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

clientid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-clientid')
tenantid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-sp-secretvalue')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": clientid,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://demo@formula1dl4019.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl4019/demo",
  extra_configs = configs)



# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl4019/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl4019/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl4019/demo")

# COMMAND ----------


