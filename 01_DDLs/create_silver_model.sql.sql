-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Silver Layer Tables

-- COMMAND ----------

CREATE WIDGET TEXT silver_schema  DEFAULT "";
CREATE WIDGET TEXT silver_location_root  DEFAULT "";

-- COMMAND ----------

-- DBTITLE 1,Create Schema for Silver layer tables
create database if not exists $silver_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 01. Dimension Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### atm_customers

-- COMMAND ----------

-- DBTITLE 1,atm_customers table
drop table if exists $silver_schema.atm_customers
;
create table $silver_schema.atm_customers
(
    customer_id BIGINT,
    card_number BIGINT,
    checking_savings STRING,
    first_name STRING,
    last_name STRING,
    customer_since_date DATE,
    preferred INT,
    current BOOLEAN,
    effective_date date,
    end_date date,
    load_ts TIMESTAMP,
    update_ts TIMESTAMP
)
using delta
location '$silver_location_root/atm_customers'
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### atm_locations

-- COMMAND ----------

-- DBTITLE 1,atm_locations table
drop table if exists $silver_schema.atm_locations
;
create table $silver_schema.atm_locations
(
    atm_id BIGINT,
    city STRING, 
    state STRING, 
    zip STRING,
    pos_capability STRING,
    offsite_or_onsite STRING,
    bank STRING,
    load_ts date
)
using delta
location '$silver_location_root/atm_locations'
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 01. Fact Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### atm_visits
-- MAGIC > Note:
-- MAGIC - Partition Key is **transaction_dt**
-- MAGIC - **customer_id** is good for z-ordering (High Cardinality)

-- COMMAND ----------

-- DBTITLE 1,atm_visits  table
drop table if exists $silver_schema.atm_visits
;
create table $silver_schema.atm_visits
(
    visit_id BIGINT,
    customer_id BIGINT,
    atm_id BIGINT,
    withdrawl_or_deposit STRING,
    amount BIGINT,
    fraud_report STRING,
    transaction_ts TIMESTAMP,
    transaction_dt DATE,    
    load_ts TIMESTAMP
)
using delta
PARTITIONED BY (transaction_dt)
location '$silver_location_root/atm_visits'
;
