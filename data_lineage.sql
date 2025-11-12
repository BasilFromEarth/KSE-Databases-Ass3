-- =================================================================
-- SCRIPT TO TRACE DATA LINEAGE: RAW → STAGE → MART
-- We will follow two specific examples through the DWH layers.
-- =================================================================

-- =================================================================
-- CASE STUDY 1: The Evolving Customer (cust_101 - Ivan Petrenko)
-- =================================================================

-- STEP 1: RAW LAYER - The Full, Unfiltered History
-- We query the raw customer table for 'cust_101'.
-- Notice: We find TWO records, ingested at different times. The status changes from 'ACTIVE' to 'UNDER_REVIEW'.
-- This is the immutable log of what the source system told us and when.
SELECT
  id AS customer_id,
  status,
  _ingested_at
FROM
  `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers`
WHERE
  id = 'cust_101'
ORDER BY
  _ingested_at;

-- Result Insight: The Raw layer keeps every change as a separate, timestamped record.


-- STEP 2: STAGING LAYER - The Clean, Current Snapshot
-- Now, we query the staging customer table for 'cust_101'.
-- Notice: We only find ONE record. The status is 'UNDER_REVIEW'.
-- The staging process has deduplicated the raw log to show only the most recent, current state of the customer.
SELECT
  customer_id,
  first_name,
  status,
  registration_date,
  _ingested_at
FROM
  `strong-harbor-474616-p4.dyba_stg_banking.stg_customers`
WHERE
  customer_id = 'cust_101';

-- Result Insight: The Staging layer cleans the data and represents the LATEST known state of an entity.


-- STEP 3: MART LAYER - The Powerful, Historical Dimension (SCD Type 2)
-- Finally, we query our dimensional model, DIM_CUSTOMER, for 'cust_101'.
-- Notice: We see TWO records again! But now, they are beautifully structured with validity periods.
-- We have preserved the full history in a queryable format. This is the power of SCD Type 2.
SELECT
  customer_key,
  customer_id,
  status,
  valid_from,
  valid_to,
  is_current
FROM
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_CUSTOMER`
WHERE
  customer_id = 'cust_101'
ORDER BY
  valid_from;

-- Result Insight: The Mart layer transforms the data into a model that preserves historical context,
-- allowing us to analyze data "as it was" at any point in time.


-- =================================================================
-- CASE STUDY 2: The Foreign Currency Transaction (txn_9 - Amazon DE)
-- =================================================================

-- STEP 1: RAW LAYER - The Original Transaction Record
-- We query the raw transactions table for 'txn_9'.
-- Notice: The amount is -50.00, currency is EUR, and the transaction_date is a STRING.
SELECT
  transaction_id,
  amount,
  currency,
  description,
  transaction_date
FROM
  `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions`
WHERE
  transaction_id = 'txn_9';

-- Result Insight: The Raw layer shows the data exactly as it arrived, with original types and formats.


-- STEP 2: STAGING LAYER - Cleaned and Standardized
-- We query the staging transactions table for 'txn_9'.
-- Notice: The transaction_timestamp is now a proper TIMESTAMP data type. The currency_code is standardized (UPPER).
-- The amount is still -50.00 because staging does not apply business logic like currency conversion.
SELECT
  transaction_id,
  amount,
  currency_code,
  description,
  transaction_timestamp
FROM
  `strong-harbor-474616-p4.dyba_stg_banking.stg_transactions`
WHERE
  transaction_id = 'txn_9';

-- Result Insight: The Staging layer ensures data is clean, typed, and consistent, preparing it for analysis.


-- STEP 3: MART LAYER - Enriched with Business Value
-- We query the final FACT_TRANSACTIONS table for 'txn_9'.
-- Notice: This is where the magic happens! We still have the original amount and currency,
-- but we now have a NEW column: `transaction_amount_uah`.
-- This value was calculated by joining the transaction with the currency rates for that specific day.
SELECT
  f.transaction_id,
  f.transaction_amount,         -- The original amount
  f.currency_code,              -- The original currency
  f.transaction_amount_uah,     -- The NEW, calculated amount in our standard currency
  d.first_name,                 -- We can now easily join to get customer context
  dt.month_name                 -- ... or date context
FROM
  `strong-harbor-474616-p4.dyba_mart_banking.FACT_TRANSACTIONS` f
JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_CUSTOMER` d ON f.customer_key = d.customer_key
JOIN
  `strong-harbor-474616-p4.dyba_mart_banking.DIM_DATETIME` dt ON f.transaction_date_key = dt.full_date
WHERE
  f.transaction_id = 'txn_9'
  AND d.is_current = TRUE; -- Filter for the customer's current profile for simplicity

-- Result Insight: The Mart layer applies complex business logic (like currency conversion)
-- and models the data in a way that makes joining and analysis simple and intuitive.
-- We transformed a raw number into a valuable, comparable business metric.