-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Bronze Layer Tables

-- COMMAND ----------

CREATE WIDGET TEXT bronze_schema  DEFAULT "";
CREATE WIDGET TEXT bronze_location_root  DEFAULT "";

-- COMMAND ----------

-- DBTITLE 1,Create Schema for Bronze layer tables
create database if not exists $bronze_schema;

-- COMMAND ----------

-- DBTITLE 1,atm_visits  table
drop table if exists $bronze_schema.atm_visits
;
create table $bronze_schema.atm_visits
(
    visit_id BIGINT,
    customer_id BIGINT,
    atm_id BIGINT,
    withdrawl_or_deposit STRING,
    amount BIGINT,
    fraud_report STRING,
    transaction_ts TIMESTAMP,
    date DATE
)
using delta
PARTITIONED BY (date)
location '$bronze_location_root/atm_visits'
;

-- COMMAND ----------

-- DBTITLE 1,atm_customers table
drop table if exists $bronze_schema.atm_customers
;
create table $bronze_schema.atm_customers
(
    customer_id BIGINT,
    card_number BIGINT,
    checking_savings STRING,
    first_name STRING,
    last_name STRING,
    customer_since_date DATE,
    preferred INT
)
using delta
location '$bronze_location_root/atm_customers'
;

-- COMMAND ----------

-- DBTITLE 1,atm_locations table
drop table if exists $bronze_schema.atm_locations
;
create table $bronze_schema.atm_locations
(
    atm_id BIGINT,
    city_state_zip STRUCT <city: STRING, state: STRING, zip: STRING>,
    pos_capability STRING,
    offsite_or_onsite STRING,
    bank STRING
)
using delta
location '$bronze_location_root/atm_locations'
;
