![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/1.png)



2. 

## \[OPTIONAL] Cloudera Data Flow (CDF) Setup \[UNDER CONSTRUCTION - WORK IN PROGRESS]<a id="optional-cloudera-data-flow-cdf-setup-under-construction---work-in-progress"></a>

- **NOTE:** be sure to run the optional section of the Hive SQL script to create the necessary tables for this section.  The tables are - weather, weather\_clouds, & weather\_conditions

- Signup for free Weather CheckWX License Key - this API was chosen as this is a good API for Weather data related to airline travel

  - Website - <https://www.checkwxapi.com/>

  - Click “Sign Up” button or “Get Started Free” button

    - Fill out form & save API key for later use

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/14.png)

- Documentation: <https://www.checkwxapi.com/documentation/metar>

-

* Create DataHub Cluster for Streams Messaging - default settings is fine

  - In this DataHub open Streams Messaging Manager (SMM)

    - Click on Topics

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/15.png)

When finished it should look like this when you search on “weather”

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/16.png)

- Create 3 Kafka Topics - each with 3 partitions; Delete cleanup policy; & Maximum availability

  - “weather”

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/17.png)

- “weather\_clouds”

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/18.png)

- “weather\_conditions”

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/19.png)

- in this DataHub open Schema Registry

  - Create 3 Schema Registry avro schemas - found here - <https://drive.google.com/drive/folders/1eioXyxSNvucbgJ_QTs-2HkphcEMtBr7n?usp=share_link>

When completed it should look like the following

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/20.png)

- “weather”

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/21.png)

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/22.png)

- “weather\_clouds”

- “weather\_conditions”

* Deploy Data Flow .json file - if not in catalog, upload the following file to the CDF Catalog - found here - <https://drive.google.com/file/d/1bBdT4kKMScZyyY8giTC4zzGxa_W-HwFF/view?usp=share_link>

![](/Users/jingalls/Documents/GitHub/iceberg-demo-runbook/images/23.png)

- x


## CDF Setup<a id="cdf-setup"></a>

- Sign-up for free API Key from Weather WX

  - Go to - ?????

  - Get API Key

  - Save API Key

- Enable Data Flow for the Environment you are planning to use

  - Import the Weather Data Flow to ingest data from the Weather API to Kafka

    - ????.json

    - Name it \<user\_id>\_Weather Data to Kafka

  - Deploy the ReadyFlow for Kafka to Iceberg

    - Use the following Parameter values

    - ?????

- In CDW Hive run the following DDL

  - ??????

- In the Environment you are planning to use create a DataHub cluster for Streams Messaging

  - In Schema Registry create the following schemas

    - ?????

  - In Streams Messaging Manager create the following Kafka Topics

    - ??????

  -

-






3. 

# DEMO >>><a id="demo-"></a>









# \[OPTIONAL] Load Iceberg Table using Cloudera DataFlow \[UNDER CONSTRUCTION - WORK IN PROGRESS]<a id="optional-load-iceberg-table-using-cloudera-dataflow-under-construction---work-in-progress"></a>

This section shows how you can use Cloudera Data Flow (CDF) to stream data into an Iceberg table.  This example will append new weather data into the Weather tables.  There are 2 steps involved in this process: 1) Call API to get weather data & send data to Kafka; and 2) Use ReadyFlow “Kafka To Iceberg” to append the Kafka weather data to the Iceberg tables (this is performed 3 times for each weather table)

**\[NOTE:]** - Be careful when using this as to not treat this like a RTDM (Kudu) equivalent.  If there are frequent updates requirements with large volumes of data, then Iceberg may not be the best choice - you should consider using Kudu instead.

**CDF - Load Iceberg Table using PutIceberg Processor**

-

- Click on the Dashboard named “**A**”



# Iceberg Table Format Table Maintenance Tasks 

Perform necessary clean up of Iceberg tables.  The following steps will build off of each other - ie. the first section will load data into the flights table in the \<user-id>\_airlines\_maint database and the next sections will use this data load to perform the table maintenance tasks


-



 __

4. # Appendix <a id="appendix"></a>


## Coming Soon (planned)<a id="coming-soon-planned"></a>

In no particular order:


- Iceberg v2 Features (some added already to Runbook, a few are missing, but will be added over time)
   acid merge in cml (done)
   *add* acid merge in Hive
   insert / update / delete - shown in DF CDC (done)
   *add* insert / update / delete in Hive (not available for Impala)


  - Materialized View
     Hive
     Impala only from Unified Analytics


  - Upload in HUE directly to Iceberg Table Format

- \[WORK IN PROGRESS] \[adding 2023-03-30] CDF writing

  - Publish data to Kafka Topic(s)

  - From Kafka to Iceberg (leverage a Ready Flow in DFx)


  - Weather data to be used to augment Delay data for ML Predictions

- Add table maintenance

<!---->

    --

    -- DETERMINE LOCATION OF METADATA AND DATA FILES
    DESCRIBE FORMATTED ${user_id}_airlines_maint.flights;

    -- above this comment will be included in setup steps

    -- following section will be in spark code or in the execution steps

    -- LOAD DATA VIA SPARK

    SELECT * FROM ${user_id}_airlines_maint.flights.history;

    SELECT * FROM ${user_id}_airlines_maint.flights.files;
    SELECT * FROM ${user_id}_airlines_maint.flights.manifests;

- Rollback

  - First insert some bad records into flights table (with year = 9999)

  - Query data to see bad rows

  - Query to see where bad rows came from (ie. which Snapshot)

  - Perform Rollback (using either method):

    - ALTER TABLE ${user\_id}\_airlines.flights EXECUTE ROLLBACK('${create\_ts}');

    - ALTER TABLE ${user\_id}\_airlines.flights EXECUTE ROLLBACK(${snapshot\_id});

<!---->

    --
    -- [OPTIONAL] TABLE MAINTENANCE FEATURES

    -- INSERT "BAD" RECORD INTO FLIGHTS TABLE
    INSERT INTO ${user_id}_airlines_maint.flights VALUES(99,99,99,9999,9999,9999,9999,'DL',947,'N981DL',95,86,70,113,104,'XXX','ATL',356,11,14,0,'n/a','0',0,0,113,0,0,9999);

    -- Check data that was loaded - will see year=9999 (invalid)
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
    GROUP BY year
    ORDER BY year desc;

    -- See Snapshot to determine when this data was loaded
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

    -- SELECT DATA USING TIMESTAMP FOR SNAPSHOT
    --      Using the previous Snapshot will see that this is where the records were loaded (Rollback needed)
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
      FOR SYSTEM_VERSION AS OF ${snapshot_id}
    GROUP BY year
    ORDER BY year desc;

    -- ROLLBACK TO LAST KNOWN "GOOD" STATE FOR THE TABLE
    ALTER TABLE ${user_id}_airlines_maint.flights EXECUTE ROLLBACK(${snapshot_id});

    -- Check data has been restored to last known "GOOD" state - data to year 2006
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
    GROUP BY year
    ORDER BY year desc;

- Expire Snapshot(s) manually

  - x

<!---->

    -- EXPIRE SNAPSHOT(S)
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

    -- Expire Snapshots up to the specified timestamp
    --      BE CAREFUL: Once you run this you will not be able to Time Travel for any Snapshots that you Expire
    ALTER TABLE ${user_id}_airlines_maint.flights EXECUTE expire_snapshots('${create_ts}');

    -- Ensure Snapshots have been removed
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

- On-demand Compaction (PySpark Code) - run in CML or now CDE Session or scheduled Job

<!---->

    import cml.data_v1 as cmldata

    CONNECTION_NAME = "jing-cdp-datalake"
    conn = cmldata.get_connection(CONNECTION_NAME)
    spark = conn.get_spark_session()

    # Sample usage to run query through spark
    EXAMPLE_SQL_QUERY = "show databases"
    spark.sql(EXAMPLE_SQL_QUERY).show()

    # Replace <user-id> below with your user id

    user_id = "<user-id>"

    table_name = user_id + "_airlines_maint.flights"
    insert_stmt = "INSERT INTO " + table_name + " SELECT * FROM " + user_id + "_airlines_csv.flights_csv"

    # Load Data Files, to create larger number of files written for table
    for i in range(1,32):
       spark.sql(insert_stmt + " WHERE year = 2006 AND month = 1 AND dayofmonth = " + str(i))
       print("Data Loaded for Day of Month: ", i)

    # INSERT "BAD" RECORD INTO FLIGHTS TABLE
    insert_stmt = "INSERT INTO " + table_name
    spark.sql(insert_stmt + " VALUES(99,99,99,9999,9999,9999,9999,'DL',947,'N981DL',95,86,70,113,104,'XXX','ATL',356,11,14,0,'n/a','0',0,0,113,0,0,9999)")

    print("Data Load Complete")

    # Query Raw Data Table, to see that 1 month of data is loaded for 2006 ~50k records
    spark.sql("SELECT year, count(*) FROM " + table_name + " GROUP BY year ORDER BY year desc").show()

    # The 2006 partition data contains 60+ files, about 200KB each (for just 1 month loaded for each day)
    #    When reading this data it will cause overhead in opening many smaller files instead of fewer 

    #    appropriately sized files

    spark.sql("SELECT file_path, file_size_in_bytes FROM " + table_name + ".files").show(100)

    # Table Maintenance to Manually Compact files to size of 500MB
    #    Compact flights table files into fewer files

    spark.sql("CALL spark_catalog.system.rewrite_data_files(table => '" + table_name + "', options => map('target-file-size-bytes','52428800'))").show()

    # After Compaction, see that there is only 1 file which is about 11MB that needs to be read in the same directory
    spark.sql("SELECT file_path, file_size_in_bytes FROM " + table_name + ".files").show(1000)

    ## Also should clean up Manifest files
    spark.sql("SELECT path, length, added_snapshot_id, added_data_files_count FROM jing_airlines_maint.flights.manifests").show(100)

    # Table Maintenance to Manually Compact files to size of 500MB
    #    Compact flights table files into fewer files
    spark.sql("CALL spark_catalog.system.rewrite_manifests(table => 'jing_airlines_maint.flights')").show()

    # After Compaction, see that there is only 1 file which is about 11MB that needs to be read in the same directory
    spark.sql("SELECT path, length, added_snapshot_id, added_data_files_count FROM jing_airlines_maint.flights.manifests").show(100)

    # Now let's investigate the "Bad" record we saw previously


- Incorporate Data Catalog instructions (feel free to show on your own)

- Advance Table definitions - compression (Snappy/Gzip) and sorting for a given table

- Concurrency/scalability (using JMeter)

- In HUE upload file directly into Iceberg Table Format

- Iceberg in Ares CDP PvC 1.5.1 

  - Links to documentation that will help

    - <https://cloudera.atlassian.net/wiki/spaces/SE/pages/10114662491/Project+Ares+-+CDP+Private+Cloud+Demo+Environment>

    - If you haven’t installed the required certificates - <https://cloudera.atlassian.net/wiki/spaces/SE/pages/10121150465/Importing+Certificates+for+Ares+MAC+OS>

    - cde Foundry AWX - self service job that you have to run

    - Ozone documentation - <https://ozone.apache.org/docs/1.0.0/interface/ofs.html>

    -

  - Ozone commands

<!---->

    ozone sh bucket create /lake/jingalls
    ozone fs -mkdir -p ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/flights
    ozone fs -mkdir -p ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/airlines
    ozone fs -mkdir -p ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/airports
    ozone fs -mkdir -p ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/planes
    ozone fs -mkdir -p ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/unique_tickets

- SQL for Ares Private Cloud CDW + Ozone as the storage layer

<!---->

    -- CREATE DATABASES
    CREATE DATABASE ${user_id}_airlines;

    --
    -- CTAS to create Iceberg Table format
    drop table if exists ${user_id}_airlines.airports;

    CREATE TABLE ${user_id}_airlines.airports
       STORED AS ICEBERG 
       LOCATION 'ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/airports'
    AS
       SELECT * FROM airlines_csv.airports_csv;

    DESCRIBE FORMATTED ${user_id}_airlines.airports;

    --
    -- Create Partitioned Iceberg Table
    drop table if exists ${user_id}_airlines.flights;

    CREATE TABLE ${user_id}_airlines.flights (
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
    STORED AS ICEBERG
    LOCATION 'ofs://o3service1/lake/jingalls/data/warehouse/tablespace/external/hive/jing_airlines.db/flights';

    SHOW CREATE TABLE ${user_id}_airlines.flights;

    --
    -- Load Data into Partitioned Iceberg Table
    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM airlines_csv.flights_csv
     WHERE year <= 2006;

    --
    -- Query Table
    SELECT year, count(*)
    FROM ${user_id}_airlines.flights
    GROUP BY year
    ORDER BY year desc;

    --
    -- Partition Evolution
    ALTER TABLE ${user_id}_airlines.flights
    SET PARTITION spec ( year, month );

    SHOW CREATE TABLE ${user_id}_airlines.flights;

    --
    -- Load Data into Iceberg Table using NEW Partition
    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM airlines_csv.flights_csv
     WHERE year = 2007;

    --
    -- Typical analytic query patterns that need to be run

    -- RUN EXPLAIN PLAN ON THIS QUERY
    SELECT year, month, count(*)
    FROM ${user_id}_airlines.flights
    WHERE year = 2006 AND month = 12
    GROUP BY year, month
    ORDER BY year desc, month asc;

    -- RUN EXPLAIN PLAN ON THIS QUERY; AND COMPARE RESULTS
    SELECT year, month, count(*)
    FROM ${user_id}_airlines.flights
    WHERE year = 2007 AND month = 12
    GROUP BY year, month
    ORDER BY year desc, month asc;

    --
    -- SELECT SNAPSHOSTS THAT HAVE BEEN CREATED
    DESCRIBE HISTORY ${user_id}_airlines.flights;

    -- SELECT DATA USING TIMESTAMP FOR SNAPSHOT
    SELECT year, count(*)
    FROM ${user_id}_airlines.flights
      FOR SYSTEM_TIME AS OF '${create_ts}'
    GROUP BY year
    ORDER BY year desc;

    -- SELECT DATA USING TIMESTAMP FOR SNAPSHOT
    SELECT year, count(*)
    FROM ${user_id}_airlines.flights
      FOR SYSTEM_VERSION AS OF ${snapshot_id}
    GROUP BY year
    ORDER BY year desc;
