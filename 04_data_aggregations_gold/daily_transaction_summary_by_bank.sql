-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### daily_transaction_summary_by_bank

-- COMMAND ----------

CREATE WIDGET TEXT silver_schema DEFAULT "" ;
CREATE WIDGET TEXT gold_schema DEFAULT "";
CREATE WIDGET TEXT start_date_yyyyMMdd default "CURRENT_DATE";
CREATE WIDGET TEXT num_days default "3";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 01. Daily Transaction Summary by ATM - Temp View

-- COMMAND ----------

-- DBTITLE 0,01. Daily Transaction Summary by ATM - Temp View
CREATE OR REPLACE TEMPORARY VIEW tv_daily_transaction_summary_by_atm
as 
SELECT
 transaction_dt, 
 atm_id, 
 SUM(withdrawl_amt) as total_withdrawls,
 SUM(withdrawl_amt) as total_deposits
FROM 
(
  SELECT
    transaction_dt, 
    atm_id,
    CASE 
      WHEN withdrawl_or_deposit = 'withdrawl' THEN amount
      ELSE NULL
    END AS withdrawl_amt,
    CASE 
      WHEN withdrawl_or_deposit = 'deposit' THEN amount
      ELSE NULL
    END AS deposit_amt 
  FROM 
    $silver_schema.atm_visits
  WHERE
   transaction_dt  BETWEEN 
     (
       CASE WHEN '$start_date_yyyyMMdd' = '' or '$start_date_yyyyMMdd' = 'CURRENT_DATE'
         THEN CURRENT_DATE ELSE to_date('$start_date_yyyyMMdd', 'yyyyMMdd')
       END - $num_days
     ) 
   AND
     (
       CASE WHEN '$start_date_yyyyMMdd' = '' or '$start_date_yyyyMMdd' = 'CURRENT_DATE'
         THEN CURRENT_DATE ELSE to_date('$start_date_yyyyMMdd', 'yyyyMMdd')
       END
     ) 
) AS atm_transactions
GROUP BY
 transaction_dt,
 atm_id
;

-- COMMAND ----------

describe tv_daily_transaction_summary_by_atm

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02. Daily Transaction Summary by Bank - Temp View

-- COMMAND ----------

-- DBTITLE 1,Daily Transaction Summary by Bank - Temp View
CREATE OR REPLACE TEMPORARY VIEW tv_daily_transaction_summary_by_bank
AS
SELECT
  transaction_dt,
  bank,
  sum(total_withdrawls) as total_withdrawls,
  sum(total_deposits) as total_deposits,
  CURRENT_TIMESTAMP as create_ts,
  CURRENT_TIMESTAMP as update_ts
FROM
(
  SELECT
    atm_trans.*,
    CASE WHEN atm.bank is NULL THEN 'UNKNOWN' ELSE atm.bank END bank
  FROM
    tv_daily_transaction_summary_by_atm atm_trans
  LEFT JOIN  (SELECT DISTINCT atm_id, bank from  $silver_schema.atm_locations) atm
  ON atm_trans.atm_id = atm.atm_id
) X
GROUP BY
 transaction_dt, bank
 ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03. Load daily_transaction_summary_by_bank Table

-- COMMAND ----------

MERGE INTO $gold_schema.daily_transaction_summary_by_bank target
USING tv_daily_transaction_summary_by_bank staged 
ON target.transaction_dt = staged.transaction_dt AND
   target.bank = staged.bank AND
   (target.total_withdrawls <> staged.total_withdrawls OR target.total_deposits <> staged.total_deposits ) 
WHEN MATCHED 
  THEN
    UPDATE SET 
      target.total_withdrawls = staged.total_withdrawls,
      target.total_deposits = staged.total_deposits,
      target.update_ts = staged.update_ts
WHEN NOT MATCHED
  THEN INSERT (transaction_dt, bank, total_withdrawls, total_deposits, create_ts, update_ts) 
  VALUES (transaction_dt, bank, total_withdrawls, total_deposits, create_ts, update_ts)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 04. Log Table History

-- COMMAND ----------

DESCRIBE HISTORY $gold_schema.daily_transaction_summary_by_bank
