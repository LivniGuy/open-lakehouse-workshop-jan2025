# MODULE 08 - Schema Evolution (in-place)

## Overview

This module explores schema evolution for Iceberg tables on the Cloudera Data Platform (CDP). Schema .

### Why In-place Schema Evolution is important?

Apache Iceberg allows for table schemas to be changed without having to rewrite data within the table. It provides several advantages over traditional Hive tables and RDBMS systems, including: 
- Avoids costly data migrations
- No need for rewrites
- Meet changing business needs - include new data sources and business processes

### Methods Covered in This Module

#### 1. In-place Schema Evolution Using SQL

This example demonstrates how to modify an existing Iceberg table (`flights`) to add a new partitioning level by month within the existing year partition. The `ALTER TABLE` statement is used to achieve this, showcasing Iceberg's ability to evolve partitions in-place.

#### 2. Schema Evolution Using Spark SQL

In this method, you’ll explore in-place schema evolution using Spark SQL. The example demonstrates how to modify the `airlines` table to add new columns: status, and . The `ALTER TABLE` command in Spark SQL allows you to adjust the partitioning strategy without moving data, retaining the original indexing by year.

After evolving the partition, you’ll use Impala to query the Iceberg table and analyze the performance benefits of the new partitioning strategy. The module demonstrates how Iceberg’s advanced partitioning capabilities can significantly boost query performance when partitioning is aligned with query patterns.

#### 3. Schema Evolution Using Spark DataFrames

This method involves using Spark DataFrames to evolve schemas within an Iceberg table programmatically.

### Key Takeaways

- Iceberg tables suport in-place schema evolution, allowing you to optimize evolving business needs and requirements.
- Adding/removing columns from a schema.
- You can still query the old data as well as the new data without any code changes.

> **Note:** Remember to replace `${prefix}` with your chosen value (e.g., your User ID) throughout the process.

### Submodules

`01` [Evolve Iceberg Table Schema Using SQL](SchemaEvolution_SQL.md)

`02` [Evolve Iceberg Table Schema Using Spark SQL](SchemaEvolution_SparkSQL.md)

`03` [Evolve Iceberg Table Schema Using Spark DataFrames](SchemaEvolution_SparkDF.md)



