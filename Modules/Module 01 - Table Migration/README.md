#  MODULE 01 - Table Migration Overview

## Migrating Tables to Iceberg and Querying Multiple Table Formats (Summary)

This module covers migrating tables to Iceberg format on the Cloudera Data Platform (CDP) and combining Iceberg and Hive tables in queries. Iceberg offers advantages like efficient data management and faster queries.

There are two approaches to migration:

- **In-place migration:** Modifies an existing Hive table to use Iceberg for managing metadata.
- **Create Table as Select (CTAS):** Creates a new Iceberg table by reading data from an existing table.

We've already seen examples of migrating tables to Iceberg format:

- `flights` table created directly in Iceberg format.
- `planes` table migrated using the `ALTER TABLE` utility.
- `airports` table created as a new Iceberg table using CTAS.

This guide will focus on combining tables. We'll use the `unique_tickets` table, which remains in Hive format (identified by the `ParquetHiveSerDe` library).

The provided example demonstrates querying the `unique_tickets` table (Hive format) along with the `flights` table (Iceberg format). This highlights a key benefit of Iceberg - you can migrate tables gradually and still leverage them in queries alongside existing Hive tables.

In summary, Iceberg allows you to:

- Improve performance and data management for specific tables.
- Migrate tables to Iceberg over time, enabling a smooth transition.
- Combine Iceberg and Hive tables within queries for comprehensive data analysis.

Note: Remember to replace `${user_id}` with your actual user ID throughout the process.

To begin, select one of the sub-modules below:

## Submodules

`01` [Migrate Tables to Iceberg Using SQL](migrate_tbl_to_iceberg_SQL.md)

`02` [Migrate Tables to Iceberg Using Spark SQL](migrate_tbl_to_iceberg_SparkSQL.md)

`03` [Migrate Tables to Iceberg Using Spark DataFrames](migrate_tbl_to_iceberg_SparkDataFrame.md)

`04` [Query Iceberg and Hive Tables](query_iceberg_and_hive_tables_single_query_SQL.md)

