# Module 04 - Partition Evolution

## Partition Evolution Using SQL and Spark SQL

This module explores partition evolution for Iceberg tables on the Cloudera Data Platform (CDP). Partitioning allows for efficient data organization and retrieval based on specific columns.

**In-place Table Evolution:**

This example demonstrates modifying an existing Iceberg table (`flights`) to add a new partitioning level by month within the existing year partition. The `ALTER TABLE` statement achieves this with minimal data movement, as the existing data remains indexed by year.

**Impala Querying and Partitioning Benefits:**

The example highlights the advantage of querying Iceberg tables with Impala, leveraging its performance capabilities. It showcases two queries, one targeting data partitioned by year only (2006) and another targeting data partitioned by year and month (2007).

- **EXPLAIN PLAN** is used to analyze the query execution strategy.
- The query targeting the year-month partition (2007) benefits from efficient scanning of a smaller data partition (month), leading to a significant performance improvement compared to the year-only partitioned data (2006).

**Key Takeaways:**

- Iceberg tables support in-place partition evolution.
- Partitioning by relevant columns optimizes query performance, especially when using Impala.

Remember to replace `${user_id}` with your actual user ID throughout the process.

To begin, select the sub-module below:

## Submodules

`01` [Partition Evolution Using SQL](partition_evolution_SQL.md)

`02` [Partition Evolution Using Spark SQL](partition_evolution_SparkSQL.md)
