# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure datalake using Access keys

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'formula1-scope',key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl4019.dfs.core.windows.net",
    access_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl4019.dfs.core.windows.net/")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl4019.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


