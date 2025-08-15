## Overview

## Assumptions

- The input data schema is consistent and well-formed and based on shared format.
- Order Date last 4 digits are always in YYYY format and considered as year.
- Observed Duplicates in Customer & Product data are handled by using Row_Number().
- No Timestamp Column in Customer & Product thus random record based on 
  - Customer Id for Customers
  - Product Id for Products
- Customer Id NULL and Product Id NULL are removed from Master datasets.
- SQL Queries have been provided under scripts directory for reference and execution.
  - These should be executed against Aggregated Table

## Code Structure
- `src/` - Contains the main code for data processing and transformation.
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
  - writer - Contains All the Writer Class for Output
    - Raw Writer - Writes Raw Data to Delta Table
    - Enriched Writer - Writes Enriched Data to Delta Table
    - Aggregated Writer - Writes Aggregated Data to Delta Table
  - utils - Contains utility functions and classes used across the pipeline
- `scripts/` - Contains SQL scripts for reference and execution.
- `tst/` - Contains unit tests for the code.
  - In Each GitHub Action, workflow execute to ensure Test Cases are passed

## Pre Processing
- Convert Excel file of Customers.xlsx to CSV format in Customers.csv

## Execution Requirements
- Create a folder named codebase under /Workspace/Shared
- Checkout the main branch from the repository into the codebase folder
- Create a catalog named pipeline
  - create a schema named pei
  - create a volume named artifacts
  - create a folder under artifacts named input
  - Upload the input data files into the input folder
    - Products.csv
    - Orders.json
    - Customers.csv
  - Make Sure the input data base path is "/Volumes/pipeline/pei/artifacts/input"
- Navigate to src.main.flow.py
- Run the flow.py with the All Purpose Cluster and Observe Logs
- Once Completed Open the Delta Tables in Databricks SQL
  - Raw Tables
    - pei.orders_raw
    - pei.customers_raw
    - pei.products_raw
  - Enriched Tables
    - pei.orders_custom_enriched
    - pei.customers_enriched
    - pei.products_enriched
  - Aggregated Table
    - pei.year_cat_sub_cat_cust_aggregate 
- Open the SQL Scripts in scripts directory and execute them against the Aggregated Table
  - Observe the Results