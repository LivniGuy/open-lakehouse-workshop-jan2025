# MODULE 08 - Schema Evolution (in-place)

## Overview

This module explores schema evolution for Iceberg tables on the Cloudera Data Platform (CDP). The key concepts of Iceberg Schema Evolution are:

1) Backward and forward compatibility
   * Backward compatibility - meaning that data is does not need to be rewritten
   * Forward compatibility - meaning it allows applications to smoothly handle schema changes, even when they are not immediately aware of new/changed columns

2) Iceberg ensures that schema changes are handled safely. For instance, column names and types are tracked and referenced by IDs under the hood. This allows the system to handle cases where columns are renamed or reordered without ambiguity.

3) Schema changes in Iceberg are tracked in the metadata layer, which keeps versions of the table schema and data. This means schema evolution can occur independently of the data files themselves, ensuring efficient and reliable changes.


### Why In-place Schema Evolution is important?

One of Iceberg's core strengths is the ability to handle schema evolution without requiring costly table rewrites or disrupting queries on existing data. Apache Iceberg allows for table schemas to be changed without having to rewrite data within the table and it doesn't break existing queries.

It provides several advantages over traditional Hive tables and RDBMS systems, including: 
- Avoids costly data migrations
- No need for rewrites as modifying a table schema does not break existing data or queries
- Allows you to adapt to changing business needs - including new data sources and changing business processes

### Methods Covered in This Module

#### 1. In-place Schema Evolution Using SQL

This example demonstrates how to modify an existing Iceberg table (`airlines`) to add a new columns to the schema. The `ALTER TABLE` statement is used to achieve this, showcasing Iceberg's ability to evolve schema in-place.

#### 2. Schema Evolution Using Spark SQL

In this method, you’ll explore in-place schema evolution using Spark SQL. The example demonstrates how to modify the `airlines` table to add new columns: status, and updated. The `ALTER TABLE` command in Spark SQL allows you to adjust the schema without moving data, retaining the original schema for existing data.

After evolving the schema, you’ll use Spark SQL to add a new record to take advantage of the new columns.

#### 3. Schema Evolution Using Spark DataFrames

This method involves using Spark SQL and Spark DataFrames to evolve schemas within an Iceberg table programmatically.

### Key Takeaways

- Does not require rewrite of exiting data, as all changes are applied at the metadata level
- Older data and schema are still queryable post evolution
- Schema Evolution allos you to: add columns, drop columns, rename columns, change column types, and reorder columns

> **Note:** Remember to replace `${prefix}` with your chosen value (e.g., your User ID) throughout the process.

### Submodules

`01` [Evolve Iceberg Table Schema Using SQL](SchemaEvolution_SQL.md)

`02` [Evolve Iceberg Table Schema Using Spark SQL](SchemaEvolution_SparkSQL.md)

`03` [Evolve Iceberg Table Schema Using Spark DataFrames](SchemaEvolution_SparkDF.md)



