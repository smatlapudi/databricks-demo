# Databricks notebook source
# MAGIC %md
# MAGIC ### ATM_CUSTOMERS Table load (Silver Layer) 
# MAGIC 
# MAGIC > **This table is Type-1 Dimension**
# MAGIC - So we need to overwrite the old value (UPSERT/MERGE)
# MAGIC - [Delta Lake - Databricks Docuemntation](https://docs.databricks.com/delta/delta-update.html)
# MAGIC 
# MAGIC 
# MAGIC > Takes following Paramater
# MAGIC - bronze_schema
# MAGIC - silver_schema
# MAGIC - spark_checkpoint_root
# MAGIC - trigger_mode ( defaults to Batch)

# COMMAND ----------

dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('silver_schema', '')
dbutils.widgets.text('spark_checkpoint_root', '')
dbutils.widgets.dropdown("trigger_mode", "Batch", ['Batch', 'Streaming'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Setup required variables

# COMMAND ----------

import os

table_name = 'atm_customers'
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
# MAGIC 
# MAGIC > **Note:** Handle deduping within the input here ( Not implemented below)

# COMMAND ----------

# DBTITLE 1,Check Target table Schema
print(target_table);
spark.table(target_table).printSchema()

# COMMAND ----------

# DBTITLE 1,Transformations
from pyspark.sql.functions import current_timestamp

prepared_df = (
  source_df
  .selectExpr('*')
  .withColumn('load_ts', current_timestamp())
  .withColumn('update_ts', current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Setup Write Query

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MERGE INTO Statement (Delta Lake)
# MAGIC > **Syntax**
# MAGIC ```
# MAGIC MERGE INTO target_table_identifier [AS target_alias]
# MAGIC USING source_table_identifier [<time_travel_version>] [AS source_alias]
# MAGIC ON <merge_condition>
# MAGIC [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
# MAGIC [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
# MAGIC [ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]
# MAGIC ```
# MAGIC [MERGE INTO Documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html#merge-into-delta-lake-on-databricks)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Type-2 SCD Example
# MAGIC ```SQL
# MAGIC MERGE INTO atm_customers target
# MAGIC     USING (
# MAGIC        -- These rows will either UPDATE the current customer record or 
# MAGIC        -- INSERT the new customer record
# MAGIC       SELECT updates.customer_id as mergeKey, updates.*
# MAGIC       FROM updates
# MAGIC 
# MAGIC       UNION ALL
# MAGIC 
# MAGIC       -- These rows will INSERT new addresses of existing customers 
# MAGIC       -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
# MAGIC       SELECT NULL as mergeKey, updates.*
# MAGIC       FROM updates JOIN atm_customers as target
# MAGIC       ON updates.customer_id = target.customer_id 
# MAGIC       WHERE 
# MAGIC         target.current = true AND 
# MAGIC         (target.card_number <> updates.card_number OR target.preferred <> updates.preferred OR
# MAGIC          target.first_name <> updates.first_name OR target.last_name <> updates.last_name)
# MAGIC     ) staged_updates
# MAGIC     ON target.customer_id = mergeKey
# MAGIC     WHEN MATCHED AND
# MAGIC         target.current = true AND
# MAGIC         (target.card_number <> staged_updates.card_number OR target.preferred <> staged_updates.preferred OR
# MAGIC          target.first_name <> staged_updates.first_name OR target.last_name <> staged_updates.last_name)
# MAGIC       THEN  
# MAGIC         UPDATE SET -- Set current to false and endDate to source's effective date.
# MAGIC           current = false,
# MAGIC           end_date = current_date,
# MAGIC           update_ts = staged_updates.update_ts 
# MAGIC     WHEN NOT MATCHED 
# MAGIC       THEN 
# MAGIC         INSERT(
# MAGIC           customer_id,
# MAGIC           card_number,
# MAGIC           checking_savings,
# MAGIC           first_name,
# MAGIC           last_name,
# MAGIC           customer_since_date,
# MAGIC           preferred,
# MAGIC           current,
# MAGIC           effective_date,
# MAGIC           end_date,
# MAGIC           load_ts,
# MAGIC           update_ts) 
# MAGIC         VALUES(
# MAGIC           staged_updates.customer_id,
# MAGIC           staged_updates.card_number,
# MAGIC           staged_updates.checking_savings,
# MAGIC           staged_updates.first_name,
# MAGIC           staged_updates.last_name,
# MAGIC           -- this can be improved.. idealy we want to have 
# MAGIC           -- staged_updates.customer_since_date as effective_date for first time customer
# MAGIC           -- it can be done by customer_id count (windowing parition by customer_id) in staged_updates query
# MAGIC           CURRENT_DATE,  
# MAGIC           staged_updates.preferred,
# MAGIC           true, 
# MAGIC           staged_updates.customer_since_date, 
# MAGIC           null,
# MAGIC           staged_updates.load_ts ,
# MAGIC           staged_updates.update_ts 
# MAGIC          ) -- Set current to true along with the new address and its effective date.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### upsertToTaget function

# COMMAND ----------

# DBTITLE 0,upsertToTaget function
from delta.tables import *

def upsertToTaget(outputDF, batchId): 
  # Set the dataframe to view name
  outputDF.createOrReplaceTempView("updates")
  

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` temp table dataframe
  outputDF._jdf.sparkSession().sql(f"""
    MERGE INTO {target_table} target
    USING (
       -- These rows will either UPDATE the current customer record or 
       -- INSERT the new customer record
      SELECT updates.customer_id as mergeKey, updates.*
      FROM updates

      UNION ALL

      -- These rows will INSERT new addresses of existing customers 
      -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
      SELECT NULL as mergeKey, updates.*
      FROM updates JOIN {target_table} as target
      ON updates.customer_id = target.customer_id 
      WHERE 
        target.current = true AND 
        (target.card_number <> updates.card_number OR target.preferred <> updates.preferred OR
         target.first_name <> updates.first_name OR target.last_name <> updates.last_name)
    ) staged_updates
    ON target.customer_id = mergeKey
    WHEN MATCHED AND
        target.current = true AND
        (target.card_number <> staged_updates.card_number OR target.preferred <> staged_updates.preferred OR
         target.first_name <> staged_updates.first_name OR target.last_name <> staged_updates.last_name)
      THEN  
        UPDATE SET -- Set current to false and endDate to source's effective date.
          current = false,
          end_date = CURRENT_DATE,
          update_ts = staged_updates.update_ts 
    WHEN NOT MATCHED 
      THEN 
        INSERT(
          customer_id,
          card_number,
          checking_savings,
          first_name,
          last_name,
          customer_since_date,
          preferred,
          current,
          effective_date,
          end_date,
          load_ts,
          update_ts) 
        VALUES(
          staged_updates.customer_id,
          staged_updates.card_number,
          staged_updates.checking_savings,
          staged_updates.first_name,
          staged_updates.last_name,
          staged_updates.customer_since_date,
          staged_updates.preferred,
          true, 
          -- this can be improved.. idealy we want to have 
          -- staged_updates.customer_since_date as effective_date for first time customer
          -- it can be done by customer_id count (windowing parition by customer_id) in staged_updates query          
          CURRENT_DATE, 
          null,
          staged_updates.load_ts ,
          staged_updates.update_ts 
         ) -- Set current to true along with the new address and its effective date.
  """)

# COMMAND ----------

triggerType = { 'once': True}
if dbutils.widgets.get('trigger_mode') == 'Streaming':
  triggerType = { 'processingTime': '15 seconds'}
print(triggerType)

# COMMAND ----------

# DBTITLE 1,Write Query
write_query = (
  prepared_df
  .writeStream
  .format('delta')
  .foreachBatch(upsertToTaget)
  .outputMode("update")
  .option("checkpointLocation", spark_checkpoint_dir)
  .trigger(**triggerType)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write to Target

# COMMAND ----------

query = write_query.start()

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
