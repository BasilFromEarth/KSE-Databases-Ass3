-- =================================================================
-- CONFIGURATION
-- =================================================================
CREATE SCHEMA IF NOT EXISTS `strong-harbor-474616-p4.dyba_mart_banking`;

-- =================================================================
-- MART LAYER (GOLD) - DIMENSION TABLES
-- These tables provide the "who, what, where, when" context for our facts.
-- =================================================================

-- 1. DIM_DATETIME
-- A generated table that provides rich time-based attributes for any given day.
-- This is often pre-populated for several years.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.DIM_DATETIME` AS
SELECT
  d AS full_date,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(QUARTER FROM d) AS quarter,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE('%B', d) AS month_name,
  EXTRACT(DAY FROM d) AS day,
  FORMAT_DATE('%A', d) AS day_of_week
FROM
  UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY)) AS d;


-- 2. DIM_CUSTOMER (SCD Type 2)
-- This is the most complex dimension. It reconstructs the full history of each customer.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.DIM_CUSTOMER` AS
WITH
  raw_customer_history AS (
    -- 1. Select all historical records from the raw layer.
    --    We use the raw layer because it contains every single change event.
    SELECT
      id AS customer_id,
      INITCAP(TRIM(first_name)) AS first_name,
      INITCAP(TRIM(last_name)) AS last_name,
      UPPER(status) AS status,
      CAST(registration_date AS DATE) AS registration_date,
      _ingested_at,
      -- Create a unique hash of the tracked attributes. If this hash is the same as the
      -- previous row's hash, it means no meaningful change occurred, and it's a duplicate.
      FARM_FINGERPRINT(CONCAT(
        IFNULL(first_name, ''),
        IFNULL(last_name, ''),
        IFNULL(status, '')
      )) AS row_hash
    FROM `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers`
  ),

  non_duplicate_history AS (
    -- 2. Filter out consecutive rows where the tracked attributes did not change.
    SELECT
      *,
      LAG(row_hash) OVER (PARTITION BY customer_id ORDER BY _ingested_at) AS prev_row_hash
    FROM raw_customer_history
  ),

  effective_dates AS (
    -- 3. Use LEAD to find the start date of the NEXT record. This becomes the end date for the CURRENT record.
    SELECT
      customer_id,
      first_name,
      last_name,
      status,
      registration_date,
      _ingested_at AS valid_from, -- The record is valid starting from its ingestion time
      LEAD(_ingested_at, 1) OVER (PARTITION BY customer_id ORDER BY _ingested_at) AS valid_to
    FROM non_duplicate_history
    WHERE row_hash != prev_row_hash OR prev_row_hash IS NULL -- Keep the row if it's the first one or if it changed
  )

-- 4. Final selection and generation of surrogate keys and the 'is_current' flag.
SELECT
  -- Generate a unique surrogate key for each historical version of the customer
  FARM_FINGERPRINT(CONCAT(customer_id, CAST(valid_from AS STRING))) AS customer_key,
  customer_id,
  first_name,
  last_name,
  status,
  registration_date,
  valid_from,
  -- A NULL valid_to means this is the current, active version of the record
  valid_to,
  -- The 'is_current' flag is TRUE only if valid_to is NULL
  (valid_to IS NULL) AS is_current
FROM effective_dates;


-- 3. DIM_ACCOUNT
-- This is a Type 1 Dimension, showing only the current state of each account.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.DIM_ACCOUNT` AS
SELECT
  FARM_FINGERPRINT(account_id) AS account_key, -- Surrogate Key
  account_id,
  customer_id,
  account_type,
  currency_code,
  created_at
FROM `strong-harbor-474616-p4.dyba_stg_banking.stg_accounts`;


-- 4. DIM_TRANSACTION_TYPE
-- Inferred from the transaction description. A more robust solution might use regex or a rules engine.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.DIM_TRANSACTION_TYPE` AS
SELECT
  -- Surrogate Key
  FARM_FINGERPRINT(transaction_type) AS transaction_type_key,
  transaction_type,
  transaction_category
FROM (
  SELECT DISTINCT
    CASE
      WHEN LOWER(description) LIKE 'pokupka:%' THEN 'POS Purchase'
      WHEN LOWER(description) LIKE 'oplata:%' THEN 'Bill Payment'
      WHEN LOWER(description) LIKE '%apple.com/bill%' THEN 'Online Subscription'
      WHEN LOWER(description) LIKE '%spotify ab%' THEN 'Online Subscription'
      WHEN LOWER(description) LIKE 'znyattya gotivky:%' THEN 'ATM Withdrawal'
      WHEN LOWER(description) LIKE 'perekaz vid%' OR LOWER(description) LIKE 'perekaz na kartu%' OR LOWER(description) LIKE 'p2p popovnennya%' THEN 'P2P Transfer'
      WHEN LOWER(description) LIKE 'popovnennya rahunku fop%' THEN 'Business Account Funding'
      WHEN LOWER(description) LIKE 'povernennya:%' THEN 'Refund'
      WHEN LOWER(description) LIKE 'interest_charge%' THEN 'System Charge'
      ELSE 'Other'
    END AS transaction_type,
    CASE
      WHEN LOWER(description) LIKE '%perekaz%' OR LOWER(description) LIKE '%popovnennya%' OR LOWER(description) LIKE '%povernennya%' THEN 'Credit'
      ELSE 'Debit'
    END AS transaction_category
  FROM `strong-harbor-474616-p4.dyba_stg_banking.stg_transactions`
);


-- 5. DIM_MERCHANT (Corrected)
-- Created by cleaning up descriptions and enriching with MCC data.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.DIM_MERCHANT` AS
WITH
  extracted_names AS (
    SELECT
      t.mcc,
      t.description,
      -- This CASE statement attempts several patterns to find a clean merchant name.
      CASE
        -- Pattern 1: Look for 'Pokupka: [Merchant Name], ...' or 'Oplata: [Merchant Name], ...'
        WHEN REGEXP_CONTAINS(LOWER(t.description), r'^(pokupka|oplata): ')
          THEN REGEXP_EXTRACT(t.description, r'^(?:Pokupka|Oplata): (.*?)(?:,|$| #)')
        -- If no prefix, just take the first part of the description before a comma.
        ELSE
          REGEXP_EXTRACT(t.description, r'^(.*?)(?:,|$| #)')
      END AS extracted_name
    FROM
      `strong-harbor-474616-p4.dyba_stg_banking.stg_transactions` t
    WHERE
      t.mcc != 'UNKNOWN' AND t.mcc != '4829' -- Exclude P2P transfers from merchants
  )
SELECT
  -- Surrogate Key based on the MCC and the cleaned merchant name
  FARM_FINGERPRINT(CONCAT(COALESCE(e.mcc, 'UNKNOWN'), COALESCE(TRIM(e.extracted_name), ''))) AS merchant_key,
  COALESCE(e.mcc, 'UNKNOWN') AS mcc,
  -- Clean up the extracted name by trimming whitespace
  TRIM(e.extracted_name) AS merchant_name,
  mcc.mcc_category_description AS category
FROM
  extracted_names e
LEFT JOIN
  `strong-harbor-474616-p4.dyba_stg_banking.stg_mcc_directory` mcc ON e.mcc = mcc.mcc
-- Group by all columns to get a distinct list of merchants
GROUP BY
  merchant_key, mcc, merchant_name, category;


-- =================================================================
-- MART LAYER (GOLD) - FACT TABLES
-- These tables store the business events and metrics.
-- =================================================================

-- 6. FACT_TRANSACTIONS
-- The central fact table, bringing together keys and measures.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_mart_banking.FACT_TRANSACTIONS` AS
SELECT
  -- Surrogate Primary Key
  FARM_FINGERPRINT(t.transaction_id) AS transaction_key,
  -- Foreign Keys to Dimensions
  dc.customer_key,
  da.account_key,
  dm.merchant_key,
  dtt.transaction_type_key,
  CAST(t.transaction_timestamp AS DATE) AS transaction_date_key, -- For joining to DIM_DATETIME
  -- Measures
  t.amount AS transaction_amount,
  t.currency_code,
  -- The crucial currency conversion to create a consistent measure
  CASE
    WHEN t.currency_code = 'UAH' THEN t.amount
    ELSE t.amount * rates.rate_to_uah
  END AS transaction_amount_uah,
  -- Degenerate Dimension
  t.transaction_id
FROM
  `strong-harbor-474616-p4.dyba_stg_banking.stg_transactions` AS t
-- Join to get Account and Customer IDs
JOIN
  `strong-harbor-474616-p4.dyba_stg_banking.stg_accounts` AS sa ON t.account_id = sa.account_id
-- Join to DIM_ACCOUNT to get its surrogate key
JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_ACCOUNT` AS da ON sa.account_id = da.account_id
-- Join to DIM_CUSTOMER to get the correct historical key (SCD Type 2)
JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_CUSTOMER` AS dc ON sa.customer_id = dc.customer_id
  AND t.transaction_timestamp >= dc.valid_from
  AND (t.transaction_timestamp < dc.valid_to OR dc.valid_to IS NULL)
-- Join to our inferred DIM_TRANSACTION_TYPE
JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_TRANSACTION_TYPE` AS dtt
  ON CASE
      WHEN LOWER(t.description) LIKE 'pokupka:%' THEN 'POS Purchase'
      WHEN LOWER(t.description) LIKE 'oplata:%' THEN 'Bill Payment'
      WHEN LOWER(t.description) LIKE '%apple.com/bill%' THEN 'Online Subscription'
      WHEN LOWER(t.description) LIKE '%spotify ab%' THEN 'Online Subscription'
      WHEN LOWER(t.description) LIKE 'znyattya gotivky:%' THEN 'ATM Withdrawal'
      WHEN LOWER(t.description) LIKE 'perekaz vid%' OR LOWER(t.description) LIKE 'perekaz na kartu%' OR LOWER(t.description) LIKE 'p2p popovnennya%' THEN 'P2P Transfer'
      WHEN LOWER(t.description) LIKE 'popovnennya rahunku fop%' THEN 'Business Account Funding'
      WHEN LOWER(t.description) LIKE 'povernennya:%' THEN 'Refund'
      WHEN LOWER(t.description) LIKE 'interest_charge%' THEN 'System Charge'
      ELSE 'Other'
    END = dtt.transaction_type
-- Join to DIM_MERCHANT
LEFT JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_MERCHANT` AS dm ON t.mcc = dm.mcc
-- Join to get currency rates for conversion
LEFT JOIN
  `strong-harbor-474616-p4.dyba_stg_banking.stg_currency_rates` AS rates
  ON t.currency_code = rates.currency_code AND CAST(t.transaction_timestamp AS DATE) = rates.rate_date;

