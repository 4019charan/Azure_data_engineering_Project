# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.table("f1_processed.races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

drivers_df = spark.read.table("f1_processed.drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

circuits_df = spark.read.table("f1_processed.circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df = spark.read.table("f1_processed.constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.table("f1_processed.results") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

ra_ci_df = races_df.join(circuits_df,on = (races_df.circuit_id == circuits_df.circuit_id),how = "inner") \
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(ra_ci_df,on = results_df.result_race_id == ra_ci_df.race_id,how = "inner") \
    .join(drivers_df,on = results_df.driver_id == drivers_df.driver_id, how="inner") \
    .join(constructors_df,on = results_df.constructor_id == constructors_df.constructor_id, how="inner")

# COMMAND ----------

final_df = race_results_df.select( 'race_id','race_year','race_name','race_date','circuit_location','driver_number','driver_name','driver_nationality','team','grid','fastest_lap','race_time','points','position', "result_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter((final_df['race_year'] == '2020') & (final_df['race_name'] == 'Abu Dhabi Grand Prix')) \
    .orderBy(final_df['points'],ascending=False))

# COMMAND ----------

final_df.write.parquet(path = f"{presentation_folder_path}/race_results",mode = "overwrite")

# COMMAND ----------

write_output(final_df, 'f1_presentation', 'race_results', 'race_id')
