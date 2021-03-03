# Databricks notebook source
# MAGIC %md
# MAGIC ### ATM_VISITS (Silver Layer) table load
# MAGIC 
# MAGIC > **This table is Fact table**
# MAGIC - So we need dedupping

# COMMAND ----------

dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('silver_schema', '')
dbutils.widgets.text('spark_checkpoint_root', '')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Setup required variables

# COMMAND ----------

import os

table_name = 'atm_visits'
source_table = "{}.{}".format( dbutils.widgets.get('bronze_schema'),  table_name)
target_table = "{}.{}".format( dbutils.widgets.get('silver_schema'),  table_name)
spark_checkpoint_dir = os.path.join( 
  dbutils.widgets.get('spark_checkpoint_root'), 
  'bronze-silver', 
  '{}-{}'.format(source_table, target_table)
)

print('source_table: {}'.format(source_table))
print('target_table: {}'.format(target_table))
print('spark_checkpoint_dir: {}'.format(spark_checkpoint_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read Source Table

# COMMAND ----------

source_df = (
  spark
  .readStream
  .format('delta')
  .table(source_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Prepare Data for target table

# COMMAND ----------

# DBTITLE 1,Check Target table Schema
spark.table(target_table).printSchema()

# COMMAND ----------

# DBTITLE 1,Add load_ts
from pyspark.sql.functions import current_timestamp

prepared_df = (
  source_df
  .withColumn('load_ts', current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Setup Write Query

# COMMAND ----------

# DBTITLE 1,upsertToTaget function
from delta.tables import *

def upsertToTaget(outputDF, batchId): 
  # Set the dataframe to view name
  outputDF.createOrReplaceTempView("updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` temp table dataframe
  outputDF._jdf.sparkSession().sql("""
    MERGE INTO {} t
    USING updates s
    ON s.visit_id = t.visit_id
    WHEN NOT MATCHED THEN INSERT *
  """.format(target_table))

# COMMAND ----------

# DBTITLE 1,Write Query
write_query = (
  prepared_df
  .writeStream
  .format('delta')
  .foreachBatch(upsertToTaget)
  .outputMode("update")
  .option("checkpointLocation", spark_checkpoint_dir)
  .trigger(once=True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write to Target

# COMMAND ----------

query = write_query.start()

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
