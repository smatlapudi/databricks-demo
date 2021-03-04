-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### monthly_customer_transaction_summary

-- COMMAND ----------

CREATE WIDGET TEXT silver_schema DEFAULT "" ;
CREATE WIDGET TEXT gold_schema DEFAULT "";
CREATE WIDGET TEXT start_month_yyyyMM default "LAST_MONTH";
CREATE WIDGET TEXT num_months default "1";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 01. Monthly Transaction Summary by Customers - Temp View

-- COMMAND ----------

-- DBTITLE 0,01. Daily Transaction Summary by ATM - Temp View
CREATE OR REPLACE TEMPORARY VIEW tv_monthly_customer_transaction_summary
as 
SELECT
 transaction_month, 
 customer_id, 
 SUM(withdrawl_amt) as total_withdrawls,
 SUM(withdrawl_amt) as total_deposits
FROM 
(
  SELECT
    month(transaction_dt) as transaction_month, 
    customer_id,
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
   CAST(date_format(transaction_dt, 'yyyyMM') AS INT)  BETWEEN 
     CAST(date_format(
         add_months(
           CASE WHEN '$start_month_yyyyMM' = '' or '$start_month_yyyyMM' = 'LAST_MONTH'
             THEN CURRENT_DATE ELSE to_date('$start_month_yyyyMM', 'yyyyMM')
           END, - $num_months
         ), 'yyyyMM'
      ) AS INT) 
   AND
     CAST(date_format(
         add_months(
           CASE WHEN '$start_month_yyyyMM' = '' or '$start_month_yyyyMM' = 'LAST_MONTH'
             THEN CURRENT_DATE ELSE to_date('$start_month_yyyyMM', 'yyyyMM')
           END, - 1
         ), 'yyyyMM'
      ) AS INT) 
) AS atm_transactions
GROUP BY
 transaction_month,
 customer_id
;

-- COMMAND ----------

select * from tv_monthly_customer_transaction_summary

-- COMMAND ----------

describe tv_monthly_customer_transaction_summary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02. Load daily_transaction_summary_by_bank Table

-- COMMAND ----------

MERGE INTO $gold_schema.monthly_customer_transaction_summary target
USING ( SELECT *, CURRENT_TIMESTAMP as create_ts, CURRENT_TIMESTAMP as update_ts from tv_monthly_customer_transaction_summary) as staged 
ON target.transaction_month = staged.transaction_month AND
   target.customer_id = staged.customer_id AND
   (target.total_withdrawls <> staged.total_withdrawls OR target.total_deposits <> staged.total_deposits ) 
WHEN MATCHED 
  THEN
    UPDATE SET 
      target.total_withdrawls = staged.total_withdrawls,
      target.total_deposits = staged.total_deposits,
      target.update_ts = staged.update_ts
WHEN NOT MATCHED
  THEN INSERT (transaction_month, customer_id, total_withdrawls, total_deposits, create_ts, update_ts) 
  VALUES (transaction_month, customer_id, total_withdrawls, total_deposits, create_ts, update_ts)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 04. Log Table History

-- COMMAND ----------

DESCRIBE HISTORY $gold_schema.monthly_customer_transaction_summary
