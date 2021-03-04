-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Gold Layer Tables

-- COMMAND ----------

CREATE WIDGET TEXT gold_schema  DEFAULT "";
CREATE WIDGET TEXT gold_location_root  DEFAULT "";

-- COMMAND ----------

-- DBTITLE 1,Create Schema for Silver layer tables
create database if not exists $gold_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### daily_transaction_summary_by_bank Table

-- COMMAND ----------

-- DBTITLE 1,daily_transaction_summary_by_bank
drop table if exists $gold_schema.daily_transaction_summary_by_bank
;
create table $gold_schema.daily_transaction_summary_by_bank
(
  transaction_dt DATE,
  bank STRING,
  total_withdrawls BIGINT,
  total_deposits BIGINT,
  create_ts TIMESTAMP,
  update_ts TIMESTAMP
)
using delta
location '$gold_location_root/daily_transaction_summary_by_bank'
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### monthly_customer_transaction_summary
-- MAGIC > Note:
-- MAGIC - Partition Key is **transaction_month**
-- MAGIC - **customer_id** is good for z-ordering (High Cardinality)

-- COMMAND ----------

-- DBTITLE 1,monthly_customer_transaction_summary
drop table if exists $gold_schema.monthly_customer_transaction_summary
;
create table $gold_schema.monthly_customer_transaction_summary
(
  transaction_month INT,
  customer_id BIGINT,
  total_withdrawls BIGINT,
  total_deposits BIGINT,
  create_ts TIMESTAMP,
  update_ts TIMESTAMP
)
using delta
partitioned by (transaction_month)
location '$gold_location_root/monthly_customer_transaction_summary'
;
