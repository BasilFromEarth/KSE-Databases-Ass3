-- =================================================================
-- CONFIGURATION
-- =================================================================
CREATE SCHEMA IF NOT EXISTS `strong-harbor-474616-p4.dyba_stg_banking`;

-- =================================================================
-- STAGING LAYER (SILVER) TABLE CREATION
-- Reads from Raw Layer, performs cleaning, casting, and structuring.
-- =================================================================

-- 1. Create Staging Table for Customers
-- This query cleans names, standardizes statuses, and deduplicates records,
-- keeping only the most recent version of each customer from the raw logs.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_customers` AS
WITH
  source AS (
    SELECT * FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers`
  ),
  cleaned_and_casted AS (
    SELECT
      id AS customer_id,
      INITCAP(TRIM(first_name)) AS first_name, -- Fixes casing like 'olena' -> 'Olena'
      INITCAP(TRIM(last_name)) AS last_name,
      REGEXP_REPLACE(phone_number, r'[^0-9]', '') AS phone_number, -- Removes non-numeric chars
      UPPER(status) AS status,
      CAST(registration_date AS DATE) AS registration_date,
      _ingested_at,
      _source_system
    FROM source
  ),
  deduplicated AS (
    -- Using a window function to find the latest record for each customer_id
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS rn
    FROM cleaned_and_casted
  )
SELECT
  customer_id,
  first_name,
  last_name,
  phone_number,
  status,
  registration_date,
  _ingested_at,
  _source_system
FROM deduplicated
WHERE rn = 1; -- Select only the most recent record


-- 2. Create Staging Table for Accounts
-- Similar to customers, this cleans, casts, and deduplicates to get the current state of each account.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_accounts` AS
WITH
  source AS (
    SELECT * FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts`
  ),
  cleaned_and_casted AS (
    SELECT
      id AS account_id,
      customer_id,
      account_number,
      UPPER(account_type) AS account_type,
      UPPER(currency) AS currency_code,
      CAST(credit_limit AS NUMERIC) AS credit_limit,
      CAST(balance AS NUMERIC) AS balance,
      CAST(created_at AS TIMESTAMP) AS created_at,
      _ingested_at,
      _source_system
    FROM source
  ),
  deduplicated AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _ingested_at DESC) AS rn
    FROM cleaned_and_casted
  )
SELECT
  account_id,
  customer_id,
  account_number,
  account_type,
  currency_code,
  credit_limit,
  balance,
  created_at,
  _ingested_at,
  _source_system
FROM deduplicated
WHERE rn = 1;


-- 3. Create Staging Table for Transactions
-- This table is not deduplicated as each transaction is a unique event.
-- It focuses on casting types and cleaning descriptions.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_transactions` AS
SELECT
  transaction_id,
  account_id,
  amount,
  UPPER(currency) AS currency_code,
  TRIM(description) AS description,
  -- Handle NULL MCCs by replacing them with a placeholder. This is crucial for joins.
  COALESCE(mcc, 'UNKNOWN') AS mcc,
  CAST(transaction_date AS TIMESTAMP) AS transaction_timestamp,
  _ingested_at,
  _source_system
FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions`;


-- 4. Create Staging Table for NBU Currency Rates
-- This query unnests and parses the raw JSON payload into a structured table.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_currency_rates` AS
SELECT
  _request_date AS rate_date,
  -- Parse the date string inside the JSON to a proper DATE type
  PARSE_DATE('%d.%m.%Y', JSON_VALUE(rate_info, '$.exchangedate')) AS exchange_date,
  UPPER(JSON_VALUE(rate_info, '$.cc')) AS currency_code,
  CAST(JSON_VALUE(rate_info, '$.rate') AS NUMERIC) AS rate_to_uah,
  _ingested_at
FROM
  `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates`,
  -- Unnest the JSON array into individual rows
  UNNEST(JSON_QUERY_ARRAY(rates_payload)) AS rate_info;


-- 5. Create Staging Table for Mobile App Events
-- Parses the raw JSON event payload into a clean, structured format.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_app_events` AS
SELECT
  JSON_VALUE(event_payload, '$.eventId') AS event_id,
  JSON_VALUE(event_payload, '$.customerId') AS customer_id,
  JSON_VALUE(event_payload, '$.eventType') AS event_type,
  CAST(JSON_VALUE(event_payload, '$.timestamp') AS TIMESTAMP) AS event_timestamp,
  -- Extract some common nested details for easier querying
  JSON_VALUE(event_payload, '$.details.device') AS device,
  JSON_VALUE(event_payload, '$.details.ipAddress') AS ip_address,
  JSON_VALUE(event_payload, '$.details.featureName') AS feature_name,
  JSON_QUERY(event_payload, '$.details') AS details_payload, -- Keep the full details object
  _ingested_at,
  _source_system
FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events`;


-- 6. Create Staging Table for MCC Directory
-- A simple transformation to standardize column names.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_stg_banking.stg_mcc_directory` AS
SELECT
  mcc_code AS mcc,
  category_description AS mcc_category_description,
  _ingested_at
FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes`;