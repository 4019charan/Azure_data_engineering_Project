# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count,col,sum,countDistinct,rank,desc
from pyspark.sql import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter('race_year in (2020,2019)')

# COMMAND ----------

display(demo_df.select(countDistinct('race_name')))

# COMMAND ----------

grouped_demo_df = demo_df.groupBy('driver_name') \
    .agg(countDistinct('race_name').alias('no.of races'),sum('points').alias('total_points'))

# COMMAND ----------

grouped_demo_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practicing Window Functions

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy(["race_year","driver_name"]) \
    .agg(sum('points').alias("total_points"), \
    countDistinct('race_name').alias('no.of.races'))

# COMMAND ----------

demo_window = Window.partitionBy(['race_year']) \
    .orderBy(desc('total_points'))

# COMMAND ----------

demo_grouped_df.withColumn('rank',rank().over(demo_window)).show(100)

# COMMAND ----------


