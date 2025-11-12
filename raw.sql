-- =================================================================
-- CONFIGURATION
-- =================================================================
-- Define your project and dataset variables.
-- In a real project, these would be managed by your deployment tool.
CREATE SCHEMA IF NOT EXISTS `strong-harbor-474616-p4.dyba_raw_banking`;

-- =================================================================
-- RAW LAYER (BRONZE) TABLE CREATION
-- Data is landed here "as-is" from the source systems.
-- =================================================================

-- 1. Raw data from the Core Banking PostgreSQL Database
-- These tables represent a direct dump or CDC stream.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers` (
  id STRING,                  -- Original primary key from source
  first_name STRING,
  last_name STRING,
  phone_number STRING,        -- May contain formatting characters
  status STRING,              -- e.g., 'ACTIVE', 'CLOSED'
  registration_date STRING,   -- Note: Landed as a STRING to simulate raw data
  _ingested_at TIMESTAMP,     -- Audit column: When this record was loaded
  _source_system STRING       -- Audit column: Where the data came from
);

CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts` (
  id STRING,
  customer_id STRING,
  account_number STRING,      -- Could be masked in a real system
  account_type STRING,        -- e.g., 'BLACK', 'FOP', 'WHITE'
  currency STRING,            -- e.g., 'UAH', 'USD', 'EUR'
  credit_limit STRING,        -- Note: Landed as a STRING
  balance STRING,             -- Note: Landed as a STRING
  created_at STRING,
  _ingested_at TIMESTAMP,
  _source_system STRING
);

CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions` (
  transaction_id STRING,
  account_id STRING,
  amount NUMERIC,             -- Amount in the original currency
  currency STRING,
  description STRING,         -- The raw, often messy, transaction description
  mcc STRING,                 -- Merchant Category Code, if available
  transaction_date STRING,    -- e.g., '2025-11-10T14:35:10Z'
  _ingested_at TIMESTAMP,
  _source_system STRING
);

-- 2. Raw data from the Mobile App's Kafka Stream
-- We land the entire JSON payload into a single column.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events` (
  event_payload JSON,         -- The entire raw JSON event from Kafka
  _ingested_at TIMESTAMP,
  _source_system STRING
);

-- 3. Raw data from the NBU API for currency rates
-- The daily JSON response is stored directly.
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates` (
  rates_payload JSON,         -- The entire raw JSON response from the NBU API
  _request_date DATE,         -- The date for which the rates were requested
  _ingested_at TIMESTAMP,
  _source_system STRING
);

-- 4. Raw data from a static CSV file for MCC codes
CREATE OR REPLACE TABLE `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes` (
  mcc_code STRING,
  category_description STRING,
  _ingested_at TIMESTAMP,
  _source_system STRING
);


-- =================================================================
-- DATA POPULATION
-- Inserting sample data that mimics real-world source data.
-- =================================================================

-- Insert Customer Data
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers` VALUES
('cust_101', 'Ivan', 'Petrenko', '+380501234567', 'ACTIVE', '2022-08-15', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('cust_102', 'Olena', 'Kovalchuk', '380679876543', 'ACTIVE', '2023-01-20', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('cust_103', 'Andriy', 'Shevchenko', '+380995554433', 'CLOSED', '2021-11-30', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- Insert Account Data
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts` VALUES
('acc_201', 'cust_101', '26201234567890', 'BLACK', 'UAH', '50000.00', '12540.50', '2022-08-15T10:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('acc_202', 'cust_101', '26209876543211', 'BLACK', 'USD', '1000.00', '850.25', '2022-09-01T12:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('acc_203', 'cust_102', '26001122334455', 'FOP', 'UAH', '0.00', '250000.00', '2023-01-20T18:30:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('acc_204', 'cust_103', '26204455667788', 'WHITE', 'UAH', '1000.00', '-250.75', '2021-11-30T09:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- Insert Transaction Data (Note the messy 'description' field)
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions` VALUES
('txn_1', 'acc_201', -150.50, 'UAH', 'Pokupka: SILPO, KYIV', '5411', '2025-11-10T14:35:10Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_2', 'acc_201', -2500.00, 'UAH', 'Apple.com/bill', '5734', '2025-11-10T18:05:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_3', 'acc_203', 120000.00, 'UAH', 'Popovnennya rahunku FOP', '6012', '2025-11-11T09:12:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_4', 'acc_202', -100.00, 'USD', 'Znyattya gotivky: Raiffeisen Bank ATM', '6011', '2025-11-11T11:45:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_5', 'acc_201', 5000.00, 'UAH', 'Perekaz vid Petrenko I.A.', '4829', '2025-11-11T15:20:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- Insert App Event Data (as JSON)
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events` VALUES
(JSON '{"eventId": "evt_abc", "customerId": "cust_101", "eventType": "user_login", "timestamp": "2025-11-10T14:30:00Z", "details": {"device": "iPhone 15 Pro", "ipAddress": "192.168.1.1"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_def", "customerId": "cust_102", "eventType": "feature_click", "timestamp": "2025-11-11T09:10:00Z", "details": {"featureName": "Shake_to_Pay", "screen": "MainDashboard"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_ghi", "customerId": "cust_101", "eventType": "password_change_attempt", "timestamp": "2025-11-11T20:00:00Z", "details": {"success": false, "reason": "Old password incorrect"}}', CURRENT_TIMESTAMP(), 'MobileAppStream');

-- Insert NBU Currency Rate Data (as JSON)
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates` VALUES
(JSON '[{"r030":840,"txt":"Долар США","rate":38.55,"cc":"USD","exchangedate":"11.11.2025"},{"r030":978,"txt":"Євро","rate":41.20,"cc":"EUR","exchangedate":"11.11.2025"},{"r030":985,"txt":"Злотий","rate":9.55,"cc":"PLN","exchangedate":"11.11.2025"}]', '2025-11-11', CURRENT_TIMESTAMP(), 'NBU_API');

-- Insert MCC Code Data (from a static file)
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes` VALUES
('5411', 'Grocery Stores, Supermarkets', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('5734', 'Computer Software Stores', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('6011', 'Automated Cash Disbursements', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('6012', 'Financial Institutions - Merchandise and Services', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('4829', 'Money Transfer', CURRENT_TIMESTAMP(), 'StaticMCCFile');


-- =================================================================
-- BATCH 2: Simulating data arrival for the next day (2025-11-12)
-- This represents a new ingestion run.
-- =================================================================

-- =================================================================
-- DATA POPULATION (NEW BATCH)
-- =================================================================

-- 1. Insert Customer Data
-- Note: A new customer (cust_104) and an updated record for an existing customer (cust_101).
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers` VALUES
('cust_104', 'Maria', 'Zadorozhna', '+380951112233', 'ACTIVE', '2025-11-12', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
-- This is an UPDATE to Ivan Petrenko. In a CDC/dump, we get the full new row.
('cust_101', 'Ivan', 'Petrenko', '+380501234567', 'UNDER_REVIEW', '2022-08-15', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 2. Insert Account Data
-- A new EUR account for Olena (cust_102) and a new card for our new customer (cust_104).
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts` VALUES
('acc_205', 'cust_102', '26208877665544', 'BLACK', 'EUR', '500.00', '450.10', '2025-11-12T10:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('acc_206', 'cust_104', '26201111222233', 'BLACK', 'UAH', '20000.00', '19850.00', '2025-11-12T14:20:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 3. Insert Transaction Data
-- New transactions for the next day, including a cashback-eligible one (Restaurant).
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions` VALUES
('txn_6', 'acc_201', -850.75, 'UAH', 'Pokupka: OKKO 7, KYIV', '5541', '2025-11-12T08:30:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_7', 'acc_201', -450.00, 'UAH', 'Oplata: VERY WELL CAFE', '5812', '2025-11-12T13:05:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_8', 'acc_203', -75000.00, 'UAH', 'Oplata po rahunku #12345', '6012', '2025-11-12T15:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_9', 'acc_205', -50.00, 'EUR', 'Pokupka: Amazon DE', '5734', '2025-11-12T18:45:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_10', 'acc_206', -150.00, 'UAH', 'Pokupka: Aroma Kava', '5814', '2025-11-12T16:10:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_11', 'acc_201', -120.00, 'UAH', 'Oplata: BARBERSHOP', '7230', '2025-11-12T19:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 4. Insert App Event Data (as JSON)
-- More events, including a successful account creation and a P2P transfer.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events` VALUES
(JSON '{"eventId": "evt_jkl", "customerId": "cust_104", "eventType": "account_creation_success", "timestamp": "2025-11-12T14:18:00Z", "details": {"accountType": "BLACK", "initialCreditLimit": "20000.00"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
 (JSON '{"eventId": "evt_mno", "customerId": "cust_102", "eventType": "p2p_transfer_success", "timestamp": "2025-11-12T17:00:00Z", "details": {"fromAccount": "acc_203", "toCard": "5168********1234", "amount": "10000.00", "currency": "UAH"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_pqr", "customerId": "cust_101", "eventType": "user_login", "timestamp": "2025-11-12T21:00:00Z", "details": {"device": "Pixel 8 Pro", "ipAddress": "10.0.0.5"}}', CURRENT_TIMESTAMP(), 'MobileAppStream');

-- 5. Insert NBU Currency Rate Data (as JSON)
-- The rates for the next day.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates` VALUES
(JSON '[{"r030":840,"txt":"Долар США","rate":38.62,"cc":"USD","exchangedate":"12.11.2025"},{"r030":978,"txt":"Євро","rate":41.15,"cc":"EUR","exchangedate":"12.11.2025"},{"r030":985,"txt":"Злотий","rate":9.58,"cc":"PLN","exchangedate":"12.11.2025"}]', '2025-11-12', CURRENT_TIMESTAMP(), 'NBU_API');

-- 6. Insert MCC Code Data (from a static file)
-- Adding new codes that appeared in the new transactions.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes` VALUES
('5541', 'Service Stations (With or without Ancillary Services)', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('5812', 'Eating Places, Restaurants', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('5814', 'Fast Food Restaurants', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('7230', 'Barber and Beauty Shops', CURRENT_TIMESTAMP(), 'StaticMCCFile');


-- =================================================================
-- BATCH 3: Simulating data arrival for the next day (2025-11-13)
-- This represents a third, distinct ingestion run.
-- =================================================================

-- =================================================================
-- DATA POPULATION (NEW BATCH 3)
-- =================================================================

-- 1. Insert Customer Data
-- No new customers in this batch, but an update to Maria's phone number.
-- This again results in a full new row for the customer in the raw log.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers` VALUES
('cust_104', 'Maria', 'Zadorozhna', '+380951112234', 'ACTIVE', '2025-11-12', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 2. Insert Account Data
-- A credit limit was increased for Ivan Petrenko's UAH account (acc_201).
-- This is reflected as a new snapshot of his account.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts` VALUES
('acc_201', 'cust_101', '26201234567890', 'BLACK', 'UAH', '75000.00', '11839.75', '2022-08-15T10:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 3. Insert Transaction Data
-- Includes a refund (positive amount) and more daily spending.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions` VALUES
('txn_12', 'acc_201', 2500.00, 'UAH', 'Povernennya: Apple.com/bill', '5734', '2025-11-13T10:15:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_13', 'acc_205', -15.50, 'EUR', 'Oplata: Spotify AB', '5815', '2025-11-13T11:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_14', 'acc_206', -1250.00, 'UAH', 'Pokupka: Rozetka.ua', '5311', '2025-11-13T14:20:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_15', 'acc_203', -15000.00, 'UAH', 'Perekaz na kartu', '4829', '2025-11-13T17:55:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_16', 'acc_201', -320.00, 'UAH', 'Oplata: Kino teatr MULTIPLEX', '7832', '2025-11-13T20:30:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');

-- 4. Insert App Event Data (as JSON)
-- Correct JSON syntax is used.
-- Includes a support chat initiation event.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events` VALUES
(JSON '{"eventId": "evt_stu", "customerId": "cust_102", "eventType": "support_chat_initiated", "timestamp": "2025-11-13T10:45:00Z", "details": {"topic": "Dispute Transaction", "transactionId": "txn_15"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_vwx", "customerId": "cust_104", "eventType": "feature_click", "timestamp": "2025-11-13T14:18:00Z", "details": {"featureName": "Cashback_Selection", "screen": "CashbackSettings"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_yza", "customerId": "cust_101", "eventType": "credit_limit_change_success", "timestamp": "2025-11-13T18:00:00Z", "details": {"fromAccount": "acc_201", "oldLimit": "50000.00", "newLimit": "75000.00"}}', CURRENT_TIMESTAMP(), 'MobileAppStream');

-- 5. Insert NBU Currency Rate Data (as JSON)
-- Rates for November 13th.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates` VALUES
(JSON '[{"r030":840,"txt":"Долар США","rate":38.60,"cc":"USD","exchangedate":"13.11.2025"},{"r030":978,"txt":"Євро","rate":41.25,"cc":"EUR","exchangedate":"13.11.2025"},{"r030":985,"txt":"Злотий","rate":9.60,"cc":"PLN","exchangedate":"13.11.2025"}]', '2025-11-13', CURRENT_TIMESTAMP(), 'NBU_API');

-- 6. Insert MCC Code Data (from a static file)
-- New codes from today's transactions.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes` VALUES
('5815', 'Audio Visual Sales, Rentals', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('5311', 'Department Stores', CURRENT_TIMESTAMP(), 'StaticMCCFile'),
('7832', 'Motion Picture Theaters', CURRENT_TIMESTAMP(), 'StaticMCCFile');


-- =================================================================
-- DATA POPULATION (NEW BATCH 4)
-- =================================================================

-- 1. Insert Customer Data
-- Simulating a data entry error: Olena's name is now in lowercase.
-- The staging layer will need to handle such inconsistencies.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_customers` VALUES
('cust_102', 'olena', 'Kovalchuk', '380679876543', 'ACTIVE', '2023-01-20', CURRENT_TIMESTAMP(), 'CoreBankingDB');


-- 2. Insert Account Data
-- The old, closed account for Andriy is now marked as FROZEN.
-- This is a new account status our DWH must recognize.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_accounts` VALUES
('acc_204', 'cust_103', '26204455667788', 'WHITE', 'UAH', '0.00', '-280.75', '2021-11-30T09:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');


-- 3. Insert Transaction Data
-- Includes a transaction with a NULL MCC and a system-generated interest charge.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_postgres_transactions` VALUES
('txn_17', 'acc_201', -550.00, 'UAH', 'Oplata: Lvivski Kruasany', NULL, '2025-11-15T11:25:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'), -- Missing MCC
('txn_18', 'acc_203', -100000.00, 'UAH', 'Znyattya FOP', '6011', '2025-11-15T12:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_19', 'acc_204', -30.00, 'UAH', 'INTEREST_CHARGE_NOV25', '9999', '2025-11-15T23:50:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'), -- System transaction
('txn_20', 'acc_206', 2000.00, 'UAH', 'P2P Popovnennya vid Kovalchuk O.', '4829', '2025-11-15T15:00:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB'),
('txn_21', 'acc_201', -480.00, 'UAH', 'Pokupka: Planeta Kino', '7832', '2025-11-15T19:10:00Z', CURRENT_TIMESTAMP(), 'CoreBankingDB');


-- 4. Insert App Event Data (as JSON)
-- Includes a failed login attempt and a cashback category selection event.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_kafka_app_events` VALUES
(JSON '{"eventId": "evt_bcd", "customerId": "cust_103", "eventType": "login_failed", "timestamp": "2025-11-15T09:00:00Z", "details": {"reason": "Account frozen", "ipAddress": "8.8.8.8"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_efg", "customerId": "cust_104", "eventType": "cashback_selection_updated", "timestamp": "2025-11-15T14:25:00Z", "details": {"selectedCategories": ["Groceries", "Cinema"], "month": "November"}}', CURRENT_TIMESTAMP(), 'MobileAppStream'),
(JSON '{"eventId": "evt_hij", "customerId": "cust_104", "eventType": "p2p_transfer_received", "timestamp": "2025-11-15T15:01:00Z", "details": {"fromName": "Olena K.", "amount": "2000.00", "currency": "UAH"}}', CURRENT_TIMESTAMP(), 'MobileAppStream');


-- 5. Insert NBU Currency Rate Data (as JSON)
-- Rates for Saturday, November 15th.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_api_nbu_rates` VALUES
(JSON '[{"r030":840,"txt":"Долар США","rate":38.65,"cc":"USD","exchangedate":"15.11.2025"},{"r030":978,"txt":"Євро","rate":41.30,"cc":"EUR","exchangedate":"15.11.2025"},{"r030":985,"txt":"Злотий","rate":9.62,"cc":"PLN","exchangedate":"15.11.2025"}]', '2025-11-15', CURRENT_TIMESTAMP(), 'NBU_API');

-- 6. Insert MCC Code Data (from a static file)
-- Adding a system/internal code category.
INSERT INTO `strong-harbor-474616-p4.dyba_raw_banking.raw_mcc_codes` VALUES
('9999', 'System & Interest Charges', CURRENT_TIMESTAMP(), 'StaticMCCFile');


