# Load Data into Iceberg Table

**Insert data feature**

- Execute the following lines in HUE for the Hive VW (this may take a few minutes)

```
    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
     WHERE year <= 2006;
```

- Once the load completes, execute the following query

```
    SELECT year, count(*) 
    FROM ${user_id}_airlines.flights
    GROUP BY year
    ORDER BY year desc;
```
