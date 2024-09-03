# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure datalake using Access keys

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl4019.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl4019.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl4019.dfs.core.windows.net","sp=rl&st=2024-08-25T02:11:59Z&se=2024-08-25T10:11:59Z&spr=https&sv=2022-11-02&sr=c&sig=MmlmFNYIOeXKxlUTk5J8ty%2FYM7Y%2FsdX5HM6ZG6JV5Xk%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl4019.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl4019.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


