## Overview

## Assumptions

- The input data schema is consistent and well-formed and based on shared format.
- Order Date last 4 digits are always in YYYY format and considered as year.
- Observed Duplicates in Customer & Product data are handled by using Row_Number().
- No Timestamp Column in Customer & Product thus random record based on 
  - Customer Id for Customers
  - Product Id for Products
- Customer Id NULL and Product Id NULL are not considered for any processing and removed from the dataset.
- SQL Queries have been provided under scripts directory for reference and execution.
  - These should be executed against Aggregated Table

## Code Structure
- `src/` - Contains the main code for data processing and transformation.
  - main.flow.py - The main entry point for the data processing pipeline
  - reader - Contains All the Reader Class for Ingestion
    - CSV Reader
    - JSON Reader
    - Excel Reader
  - transformer - Contains All the Transformer Class for Data Transformation
    - Enricher - 
      - Renames Columns in standard format
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

## Requirements
- Install below pypi packages in Databricks Cluster
  - pandas~=2.3.1 
  - openpyxl~=3.1.5 
  - pyarrow~=21.0.0
