# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting Azure datalake using Service Principal

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

def mount_adls(storage_name, container_name):
    clientid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-clientid')
    tenantid = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenantid')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-sp-secretvalue')

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": clientid,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}
    
    if  any(mount.mountPoint==f"/mnt/{storage_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")
    
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1dl4019", "raw")
mount_adls("formula1dl4019", "processed")
mount_adls("formula1dl4019", "presentation")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl4019/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl4019/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl4019/demo")

# COMMAND ----------


