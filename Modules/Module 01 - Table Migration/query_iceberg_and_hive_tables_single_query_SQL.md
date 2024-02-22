# Query Iceberg Table Format and Hive Table Format in same query (in CDW HUE)

- Execute the following in HUE for Impala VW

  - Remember that we have already seen the following tables in Iceberg Table format

    - flights (via Create Table in Iceberg format)

    - planes (migrated via Alter Table utility)

    - airports (CTAS to Iceberg table)

  - So, let’s see about the Unique Tickets table.  We’ll see that this table is still in Hive Table Format - see SerDe Library = ParquetHiveSerDe (not the Iceberg SerDe).  So we have a Dashboard that is combining both table formats in a single Dashboard

```
    --
    -- [optional] SINGLE QUERY USING ICEBERG & HIVE TABLE FORMAT
    DESCRIBE FORMATTED ${user_id}_airlines.flights;
    DESCRIBE FORMATTED ${user_id}_airlines.unique_tickets;
```

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/65.png)

- Now let’s run a query that combines this Hive Table with an Iceberg Table

```
    -- Query combining Hive Table Format (unique_tickets) and Iceberg Table Format (flights)
    SELECT 
    t.leg1origin,
    f.dest,
    count(*) as num_passengers
    FROM ${user_id}_airlines.unique_tickets t
    LEFT OUTER JOIN ${user_id}_airlines.flights f ON
      t.leg1origin = f.origin
      AND t.leg1dest = f.dest
    GROUP BY t.leg1origin, f.dest;
```

- **VALUE**: combine Iceberg with Hive table formats, means you can convert to use Iceberg where it makes sense and also can migrate over time vs. having to migrate at the same time

