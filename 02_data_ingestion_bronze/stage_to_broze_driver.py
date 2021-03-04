# Databricks notebook source
# MAGIC %md
# MAGIC ### Stage to Bronze Driver
# MAGIC ##### Generic Notebook to ingest data into Bronze Layer Tables from Staging Area
# MAGIC > Takes following Paramater
# MAGIC - stage_location_root
# MAGIC - stage_folder
# MAGIC - bronze_schema
# MAGIC - bronze_table
# MAGIC - spark_checkpoint_root
# MAGIC - trigger_mode ( defaults to Batch)

# COMMAND ----------

dbutils.widgets.text('stage_location_root', '')
dbutils.widgets.text('stage_folder', '')
dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('bronze_table', '')
dbutils.widgets.text('spark_checkpoint_root', '')
dbutils.widgets.dropdown("trigger_mode", "Batch", ['Batch', 'Streaming'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Setup required variables

# COMMAND ----------

# DBTITLE 0,Setup required variables
import os
source_folder = os.path.join( dbutils.widgets.get('stage_location_root'), dbutils.widgets.get('stage_folder') )
source_data_schema_name = '{}_json_data_schema'.format(dbutils.widgets.get('stage_folder'))
target_table = "{}.{}".format( dbutils.widgets.get('bronze_schema'), dbutils.widgets.get('bronze_table') )
spark_checkpoint_dir = os.path.join( dbutils.widgets.get('spark_checkpoint_root'), 'stage-bronze', '{}-{}'.format( dbutils.widgets.get('stage_folder'), dbutils.widgets.get('bronze_table') ))

print('source_folder: {}'.format(source_folder))
print('source_data_schema_name: {}'.format(source_data_schema_name))
print('target_table: {}'.format(target_table))
print('spark_checkpoint_dir: {}'.format(spark_checkpoint_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Get Source Data schema

# COMMAND ----------

# DBTITLE 1,Include Stage data schema definitions from common catalog notebook
# MAGIC %run ./staged_data_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Setup Source Dataframe

# COMMAND ----------

# DBTITLE 1,Read Staged Data with Stream
source_schema = stage_data_schema_catalog[source_data_schema_name]

source_df = (
  spark
  .readStream
  .format('json')
  .schema(source_schema)
  .option('maxFilesPerTrigger', 20)
  .load(source_folder)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Setup Write Query

# COMMAND ----------

triggerType = { 'once': True}
if dbutils.widgets.get('trigger_mode') == 'Streaming':
  triggerType = { 'processingTime': '15 seconds'}
print(triggerType)

# COMMAND ----------

# DBTITLE 1,Define Write Stream
write_query = (
  source_df
  .writeStream
  .format('delta')
  .outputMode("append")
  .option("checkpointLocation", spark_checkpoint_dir)
  .trigger(**triggerType)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write to Target

# COMMAND ----------

# DBTITLE 1,Write to Target Table
query = write_query.table(target_table)

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Log Status & Results

# COMMAND ----------

# DBTITLE 1,Print Write Query Status & progress to Job log
print('status: {}'.format(query.status))
query.lastProgress

# COMMAND ----------

# DBTITLE 1,Finally print target Delta Table History to Job Log
display(spark.sql('DESCRIBE HISTORY {}'.format(target_table)))
