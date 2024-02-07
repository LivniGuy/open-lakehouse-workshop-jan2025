# Create Iceberg Table Feature

**Create table (partitioned) feature**

- Execute the following lines in HUE for the Hive VW

<!---->

    drop table if exists ${user_id}_airlines.flights;

    CREATE EXTERNAL TABLE ${user_id}_airlines.flights (
     month int, dayofmonth int, 
     dayofweek int, deptime int, crsdeptime int, arrtime int, 
     crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
     actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
     depdelay int, origin string, dest string, distance int, taxiin int, 
     taxiout int, cancelled int, cancellationcode string, diverted string, 
     carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
     lateaircraftdelay int
    ) 
    PARTITIONED BY (year int)
    STORED BY ICEBERG 
    STORED AS PARQUET;

    SHOW CREATE TABLE ${user_id}_airlines.flights;

- In the output - look for the following (see highlighted fields)

![](../images/50.png)


# Load Data into Iceberg Table<a id="load-data-into-iceberg-table"></a>

**Insert data feature**

- Execute the following lines in HUE for the Hive VW (this may take a few minutes)

<!---->

    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
     WHERE year <= 2006;

- Once the load completes, execute the following query

<!---->

    SELECT year, count(*) 
    FROM ${user_id}_airlines.flights
    GROUP BY year
    ORDER BY year desc;

