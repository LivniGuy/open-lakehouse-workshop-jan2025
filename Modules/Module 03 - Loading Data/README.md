#  MODULE 03 - Loading Data

## Loading Data into Iceberg Tables

This module focuses on loading data into Iceberg tables created on Cloudera Data Platform (CDP).

The provided example demonstrates loading data from a CSV file (`flights_csv`) into the previously created Iceberg table named `flights`. The `INSERT INTO` statement with a `SELECT` clause achieves this. It also filters the data to include only records for years up to 2006.

Following the load, a sample query is provided to verify the data. It retrieves a count of flights grouped by year, ordered by year in descending order.

This module demonstrates a basic approach to loading data into Iceberg tables.

Remember to replace `${user_id}` with your actual user ID throughout the process.

To begin, select the sub-module below:

## Submodules

`01` [Load Iceberg Tables Using SQL](load_iceberg_tbl_SQL.md)