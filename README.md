**Monobank Analytical Data Warehouse (DWH)**

Overview
This repository contains the design, schema, and implementation scripts for an analytical Data Warehouse (DWH) for Monobank, a digital bank. The primary goal of this DWH is to transform raw operational data into clean, reliable, and business-ready insights.
The architecture is designed to support a wide range of analytical use cases, from daily operational reporting to complex behavioral analysis and strategic decision-making.

Core Design Principles
Layered "Medallion" Architecture (Bronze → Silver → Gold)
The DWH is structured into three distinct layers. This separation of concerns is a core design choice that ensures data quality, traceability, and scalability.

Raw Layer (Bronze):
- Purpose: An immutable, long-term archive of all source data in its original, untouched format.
- Design Choice: Data is ingested with no transformations (schema-on-read). JSON payloads from streams and APIs are stored in their native format.
- Why? This guarantees we never lose source information. It allows for complete reprocessing of the entire DWH if business logic changes, ensuring maximum flexibility and data lineage.

Staging Layer (Silver):
- Purpose: To provide a clean, structured, and unified version of all source data.
- Design Choice: This is where data is cleaned, de-duplicated, standardized, and cast into proper data types. It mirrors the source schemas but in a reliable state.
- Why? This layer serves as the "single source of truth" for all downstream analytics. It isolates the messy work of data cleaning from the complexities of business modeling, making the system easier to manage and debug.

Mart Layer (Gold):
- Purpose: A curated, business-facing layer optimized for analytics and Business Intelligence (BI).
- Design Choice: Data is modeled using Dimensional Modeling principles, primarily a Star Schema. This layer contains our central fact tables (e.g., FACT_TRANSACTIONS) and their surrounding descriptive dimensions (e.g., DIM_CUSTOMER, DIM_MERCHANT).
- Why? The star schema is highly intuitive for business users and analysts. It is also extremely performant for the types of large-scale aggregations and filtering common in analytical queries. It simplifies the data into business-centric concepts like "customers," "accounts," and "transactions."
