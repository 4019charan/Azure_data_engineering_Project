# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure datalake using Service Principal

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

clientid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-clientid')
tenantid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-sp-secretvalue')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl4019.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl4019.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl4019.dfs.core.windows.net",clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl4019.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl4019.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl4019.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl4019.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


