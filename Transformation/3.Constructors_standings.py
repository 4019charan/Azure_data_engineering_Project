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

race_grouped_df = race_results.groupBy('race_year','team') \
    .agg(
        sum(col('points')).alias('total_points'),
        count(when(col('position') == 1,True)).alias('Wins'))

# COMMAND ----------

race_grouped_df.orderBy(desc('total_points'),desc('Wins')).show()

# COMMAND ----------

race_window = Window.partitionBy('race_year').orderBy(desc('total_points'))

# COMMAND ----------

final_df = race_grouped_df.withColumn('rank',rank().over(race_window))

# COMMAND ----------

final_df.write.parquet(path = f'{presentation_folder_path}/constructor_standings',mode = 'overwrite')

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')
