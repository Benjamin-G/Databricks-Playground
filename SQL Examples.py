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

# Create a view or table

temp_table_name = "data_centers_q2_q3_snappy-1_parquet"

df.createOrReplaceTempView(temp_table_name)

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