# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import desc,rank,count,sum,when,col
from pyspark.sql.window import Window

# COMMAND ----------

race_window = Window.partitionBy('race_year').orderBy([desc('total_points'),desc('Wins')])

# COMMAND ----------

pre_final_df = race_results.groupBy("race_year",'driver_name','driver_nationality',"team") \
    .agg(sum(col('points')).alias('total_points'), \
         count(when(col('position') == 1,True)).alias('Wins'))

# COMMAND ----------

display(pre_final_df)

# COMMAND ----------

final_df = pre_final_df.withColumn('rank',rank().over(race_window))

# COMMAND ----------

final_df.write.parquet(path = f"{presentation_folder_path}/driver_standings",mode = "overwrite")

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')
