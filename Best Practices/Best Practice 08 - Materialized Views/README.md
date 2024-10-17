# Best Practice 08 - Materialized Views

```sql
CREATE MATERIALIZED VIEW transaction_aggregation 
PARTITIONED ON (col4)
STORED BY ICEBERG STORED AS PARQUET
AS
SELECT tbl1.col1, tbl2.col4, count(tbl1.amount) as total_amt 
FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.id 
GROUP BY tbl1.col1, tbl2.col4;
```

## IceTip

Leverage Materialized Views to pre-compute most common aggregation for end-user and BI projects. With MV, creating, maintaining, and evolving those business views will be much easier from the data management perspective.

Only INSERT transactions will perform an incremental rebuild. Under other conditions, will force a full rebuild.
