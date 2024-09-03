-- Databricks notebook source
show databases

-- COMMAND ----------

SHOW tables in f1_presentation

-- COMMAND ----------

select * from f1_presentation.race_results ORDER BY race_year DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Driver who played more than 100 races and has highest average

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_stats1 AS
SELECT 
    driver_name,
    count(1) as total_races,
    SUM(points) as total_points,
    AVG(11 - position) AS avg_points,
    row_number() over(order by  AVG(11 - position) DESC) as rank
  FROM
    f1_presentation.race_results
  GROUP BY
    driver_name
  HAVING
    total_races > 100
  ORDER BY
    avg_points DESC


-- COMMAND ----------

SELECT * FROM driver_stats1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_stats AS 
SELECT
    race_year,
    driver_name,
    COUNT(1) as total_races,
    SUM(points) as total_points,
    AVG(11 - position) AS avg_points
  FROM
    f1_presentation.race_results
  GROUP BY
    race_year,
    driver_name
  HAVING
    COUNT(1) > 10
  ORDER BY
    avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top-performing candidates of each year

-- COMMAND ----------

WITH ranked_results AS (
  SELECT  race_year,
        driver_name,
        avg_points,
        row_number() OVER(PARTITION BY race_year ORDER BY avg_points DESC) as rank
  FROM 
        driver_stats
  GROUP BY
        race_year,driver_name,avg_points
  ORDER BY
        race_year DESC
)
  SELECT  race_year,
          driver_name,
          avg_points
    FROM  ranked_results
  WHERE rank = 1

-- COMMAND ----------

SELECT driver_name FROM driver_stats1 WHERE rank <= 10

-- COMMAND ----------

SELECT 
    race_year,
    driver_name,
    count(1) as total_races,
    SUM(points) as total_points,
    AVG(11 - position) AS avg_points,
    row_number() over(order by  AVG(11 - position) DESC) as rank
  FROM
    f1_presentation.race_results
  WHERE driver_name IN ( SELECT driver_name FROM driver_stats1 WHERE rank <= 5 )
  GROUP BY
    race_year,driver_name
  ORDER BY
    (race_year,avg_points) DESC

-- COMMAND ----------


