# Databricks notebook source
# MAGIC %md
# MAGIC #### Common notebook to define json data schema for all staged data
# MAGIC > This can be included (by %run) in other notebooks

# COMMAND ----------

from pyspark.sql.types import StructType
import json

# COMMAND ----------

# DBTITLE 1,atm_customers_json_data_schema
atm_customers_json_data_schema =StructType.fromJson(json.loads('''
{
  "type": "struct",
  "fields": [
    {
      "metadata": {},
      "name": "customer_id",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "card_number",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "checking_savings",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "first_name",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "last_name",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "customer_since_date",
      "nullable": true,
      "type": "date"
    },
    {
      "metadata": {},
      "name": "preferred",
      "nullable": true,
      "type": "integer"
    }
  ]
}
'''))

# COMMAND ----------

# DBTITLE 1,atm_locations_json_data_schema
atm_locations_json_data_schema =StructType.fromJson(json.loads('''
{
  "type": "struct",
  "fields": [
    {
      "metadata": {},
      "name": "atm_id",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "city_state_zip",
      "nullable": true,
      "type": {
        "fields": [
          {
            "metadata": {},
            "name": "city",
            "nullable": true,
            "type": "string"
          },
          {
            "metadata": {},
            "name": "state",
            "nullable": true,
            "type": "string"
          },
          {
            "metadata": {},
            "name": "zip",
            "nullable": true,
            "type": "string"
          }
        ],
        "type": "struct"
      }
    },
    {
      "metadata": {},
      "name": "pos_capability",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "offsite_or_onsite",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "bank",
      "nullable": true,
      "type": "string"
    }
  ]
}
'''))

# COMMAND ----------

# DBTITLE 1,atm_visits_json_data_schema
atm_visits_json_data_schema =StructType.fromJson(json.loads('''
{
  "fields": [
    {
      "metadata": {},
      "name": "visit_id",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "customer_id",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "atm_id",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "withdrawl_or_deposit",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "amount",
      "nullable": true,
      "type": "long"
    },
    {
      "metadata": {},
      "name": "fraud_report",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "transaction_ts",
      "nullable": true,
      "type": "timestamp"
    },
    {
      "metadata": {},
      "name": "date",
      "nullable": true,
      "type": "date"
    }
  ],
  "type": "struct"
}
'''))

# COMMAND ----------

stage_data_schema_catalog = {
  'atm_customers_json_data_schema': atm_customers_json_data_schema,
  'atm_locations_json_data_schema': atm_locations_json_data_schema,
  'atm_visits_json_data_schema': atm_visits_json_data_schema,
}
