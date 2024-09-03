-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####SELECT DATABASE

-- COMMAND ----------

USE DATABASE f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DROP TABLES IF EXISTS

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Circuits table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuit_id INT,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formuladl4019/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### creating race table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/formula1dl4019/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables from JSON

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formula1dl/raw/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Creating tables using multi line JSON

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/lap_times")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------


