# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dbacademy_benjamin;
# MAGIC USE dbacademy_benjamin;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dc_data_raw;
# MAGIC CREATE TABLE dc_data_raw
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   PATH "/FileStore/tables/data_centers_q2_q3_snappy-1.parquet"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from dc_data_raw limit 1
# MAGIC -- describe detail dc_data_raw
# MAGIC select explode(source) from dc_data_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WITH explode_source
# MAGIC -- AS
# MAGIC --   (
# MAGIC --   SELECT 
# MAGIC --   dc_id,
# MAGIC --   to_date(date) AS date,
# MAGIC --   EXPLODE (source)
# MAGIC --   FROM dc_data_raw
# MAGIC --   )
# MAGIC -- SELECT key,
# MAGIC --   dc_id,
# MAGIC --   date,
# MAGIC --   value.description,
# MAGIC --   value.ip,
# MAGIC --   value.temps,
# MAGIC --   value.co2_level
# MAGIC   
# MAGIC -- FROM explode_source;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS device_data;
# MAGIC 
# MAGIC CREATE TABLE device_data 
# MAGIC USING delta
# MAGIC PARTITIONED BY (device_type)
# MAGIC WITH explode_source
# MAGIC AS
# MAGIC   (
# MAGIC   SELECT 
# MAGIC   dc_id,
# MAGIC   to_date(date) AS date,
# MAGIC   EXPLODE (source)
# MAGIC   FROM dc_data_raw
# MAGIC   )
# MAGIC SELECT 
# MAGIC   dc_id,
# MAGIC   key `device_type`, 
# MAGIC   date,
# MAGIC   value.description,
# MAGIC   value.ip,
# MAGIC   value.temps,
# MAGIC   value.co2_level
# MAGIC   
# MAGIC FROM explode_source;
# MAGIC 
# MAGIC SELECT * FROM device_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe extended device_data
# MAGIC cache table device_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   dc_id,
# MAGIC   device_type, 
# MAGIC   temps,
# MAGIC   TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) AS `temps_F`
# MAGIC FROM device_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW co2_levels_temporary
# MAGIC AS
# MAGIC   SELECT
# MAGIC     dc_id, 
# MAGIC     device_type,
# MAGIC     co2_level,
# MAGIC     REDUCE(co2_level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_level))) as average_co2_level,
# MAGIC     REDUCE(co2_level, 0, (c, acc) -> c + acc) as total_co2_level
# MAGIC   FROM device_data
# MAGIC   SORT BY average_co2_level DESC;
# MAGIC   
# MAGIC SELECT * FROM co2_levels_temporary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT device_type, average_co2_level 
# MAGIC   FROM co2_levels_temporary
# MAGIC )
# MAGIC PIVOT (
# MAGIC   ROUND(AVG(average_co2_level), 2) AS avg_co2 
# MAGIC   FOR device_type IN ('sensor-ipad', 'sensor-inest', 
# MAGIC     'sensor-istick', 'sensor-igauge')
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   dc_id,
# MAGIC   device_type,
# MAGIC   ROUND(AVG(average_co2_level)) AS avg_co2_level 
# MAGIC FROM co2_levels_temporary
# MAGIC -- WHERE avg_co2_level < 1100
# MAGIC GROUP BY ROLLUP (dc_id, device_type)
# MAGIC -- HAVING avg_co2_level > 1100 AND device_type IS NOT NULL
# MAGIC HAVING device_type IS NOT NULL
# MAGIC ORDER BY dc_id, device_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from co2_levels_temporary where device_type = 'null';

# COMMAND ----------

# Create a view or table

temp_table_name = "data_centers_q2_q3_snappy-1_parquet"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

from pyspark.sql.functions import from_json
# File location and type
file_location = "/FileStore/tables/data_centers_q2_q3_snappy-1.parquet"
file_type = "parquet"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `data_centers_q2_q3_snappy-1_parquet`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "data_centers_q2_q3_snappy-1_parquet"

# df.write.format("parquet").saveAsTable(permanent_table_name)