#  Best Practice 02 # - Partitioning

## For Data Management

- There is no need to create a specific column for the partition strategy.

```sql
CREATE TABLE tbl1 (i int, s string, ts timestamp, d date)
PARTITIONED BY SPEC (YEAR(ts))
STORED by ICEBERG; 
```

- Insert can leverage built-in transforms to allocate data into partitions.

```sql
PARTITIONED BY SPEC (TRUNCATE(10, i), BUCKET(11, s), YEAR(ts))
```

- Storage efficiencies, as the additional columns for partitions are not required anymore. 

## For Data Consumption

- End-users do not need to specify the filter corresponding to the partion column. 

```sql
SELECT * FROM tbl1
WHERE ts BETWEEN '2022-07-01 00:00:00' AND '2022-07-31 00:00:00'
AND month = ?;
```

- More intuitive and natural data access, using the original value of the partition columns.

- Minimize the risk of full scans, as Iceberg statistics are leveraged during query execution. 

## IceTip
Whenever possible, leverage the Hidden Partitioning capabilities Iceberg tables deliver. In most cases, tables are partitioned by an attribute that represents time. 
Using the transforms to allocate data rows to different partitions will make data ingestion and data consumption easier.

## Transforms

| Transformation | Spec | Supported by SQL Engine |
| --------------- | ---- | ----------------------- |
| Partition by year | years(time_stamp) | year(time_stamp) | Hive and Impala |

