## Overview

## Pre Processing
- Convert Excel file of Customer.xlsx to CSV format in Customers.csv
  - Used scripts.converter.py to convert Excel to CSV
  - Removed /r, /n, /r/n from the CSV file for Line Break
- Execute make excel_to_csv in local to generate the CSV File

## Assumptions
- This is a Full Snapshot Processing and Not Incremental Processing
- The input data schema is consistent and well-formed and based on shared format
- Order Date last 4 digits are always in YYYY format and considered as year

## Cleansing
- Cleaning source columns compatible SQL Column Names
  - A column mapping is created to standardize column names 
    - src/main/cleaner/cleansed.py

## Enrichment
- Observed Duplicates in Customer & Product data are handled by using Row_Number()
- No Timestamp Column in Customer & Product thus random record based on 
  - Customer Id for Customers
  - Product Id for Products
- Customer Id NULL and Product Id NULL are removed from Master datasets
- 2 Digit Rounding of Profit from Orders
- src/main/transformer/enriched.py

## Code Structure
- `src/` - Contains the main code for data processing and transformation
  - main.flow.py - The main entry point for the data processing pipeline
  - reader - Contains All the Reader Class for Ingestion using Common method
    - CSV Reader
    - JSON Reader
  - cleaner - Contains All the Cleaner Class for Data Cleaning
    - Renames Columns to Standard Format
  - transformer - Contains All the Transformer Class for Data Transformation
    - Enricher -
      - Handles Duplicates
      - Remove Null Value Records in Primary Key
    - Aggregator - 
      - Aggregates Profit Data based on Year and Category, Sub Category
      - src/main/transformer/aggregate.py
  - writer - Contains All the Writer Class for Output
    - Raw Writer - Writes Raw Data to Delta Table
    - Enriched Writer - Writes Enriched Data to Delta Table
    - Aggregated Writer - Writes Aggregated Data to Delta Table
  - utils - Contains utility functions and classes used across the pipeline
- `scripts/` - Contains SQL scripts for reference and execution
- `tst/` - Contains unit tests for the code
  - In Each GitHub Action, workflow execute to ensure Test Cases are passed

## Pre Processing
- Convert Excel file of Customers.xlsx to CSV format in Customers.csv using scripts/converter.py

## Local Execution Requirements
- Setup PEI_ENV environment variable to local
- Run make e2e_run to see output in terminal
- Create a folder source_data under root
  - Place 3 files
  - Make sure to update the Excel to CSV using scripts/converter.py
- Modify Flow Command to add some dataframe.show() to see in local
- Databricks Delta Write and SQL Statements are skipped in local

## Databricks Execution Requirements
- Go to Databricks Workspace
- Create a folder named codebase under /Workspace/Shared
- Checkout the main branch from the repository inside the codebase folder
- Create a catalog named pipeline
  - create a schema named pei
  - create a volume named artifacts
  - create a folder under artifacts named input
  - Make Sure the input data base path is "/Volumes/pipeline/pei/artifacts/input"
  - Upload the input data files into the input folder
    - Products.csv
    - Orders.json
    - Customers.csv
  - create a folder under artifacts named sql_files
    - Upload SQL files from scripts over there
- Navigate to src.main.flow.py
- Run the flow.py with the All Purpose Cluster / Serverless
- Observe the logs in console for completion of steps with message
- Display Top 50 records for each SQL statement in terminal
- Once Completed Open the Delta Tables in Databricks SQL
  - Raw Tables
    - pipeline.pei.orders_raw
    - pipeline.pei.customers_raw
    - pipeline.pei.products_raw
  - Enriched Tables
    - pipeline.pei.orders_custom_enriched
    - pipeline.pei.customers_enriched
    - pipeline.pei.products_enriched
  - Aggregated Table
    - pei.year_cat_sub_cat_cust_aggregate 
