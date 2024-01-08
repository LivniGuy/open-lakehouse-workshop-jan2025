![](https://lh7-us.googleusercontent.com/K2Z0WUSi67bHPce1f8JlYeWBRbXCrtqT6bknjOefaTjkVgxMjzENARcWeMwg7P2bT5PicaFud4tWUVYjuakXFcQ2qQ3ASmTy0RDjjybKCDGRO2C5xWyjXlriSeZpq-T55igf4SqtW5E67e2kMeK5JSw)

TABLE OF CONTENTS

[**1. Summary ****3**](#summary)

[**2. Demo Install/Setup ****4**](#demo-installsetup)

[**CDP Environment ****4**](#cdp-environment)

[**Getting Airline Data to Cloud Storage ****4**](#getting-airline-data-to-cloud-storage)

[**Create CDW Virtual Warehouses ****6**](#create-cdw-virtual-warehouses)

[**Setting up the Databases for the Demo ****7**](#setting-up-the-databases-for-the-demo)

[\[OPTIONAL\] Cloudera Data Flow (CDF) Setup \[UNDER CONSTRUCTION - WORK IN PROGRESS\] 11](#optional-cloudera-data-flow-cdf-setup-under-construction---work-in-progress)

[CDF Setup 16](#cdf-setup)

[**\[OPTIONAL\] Data Visualization (CDV) Setup ****16**](#optional-data-visualization-cdv-setup)

[**CDE Setup ****20**](#cde-setup)

[**CML Setup ****20**](#cml-setup)

[**Ranger Policy Setup ****21**](#ranger-policy-setup)

[**3. Demo Steps ****23**](#demo-steps)

[**Prior to Demo - SQL that will be run in Hive VW HUE ****23**](#prior-to-demo---sql-that-will-be-run-in-hive-vw-hue)

[**Prior to Demo - SQL that will be run in Impala VW HUE ****25**](#prior-to-demo---sql-that-will-be-run-in-impala-vw-hue)

[**Prior to Demo - Prep Data Services that you’ll demo ****28**](#prior-to-demo---prep-data-services-that-youll-demo)

[**DEMO >>> ****28**](#demo-)

[**Migrate Existing Tables to Iceberg Tables ****28**](#migrate-existing-tables-to-iceberg-tables)

[**Create Iceberg Table Feature ****29**](#create-iceberg-table-feature)

[**Load Data into Iceberg Table ****30**](#load-data-into-iceberg-table)

[**Evolution (Partition & Schema) ****31**](#evolution-partition--schema)

[**Time Travel ****33**](#time-travel)

[**CDE - Multi-function Analytics ****35**](#cde---multi-function-analytics)

[**ACID feature - shown in CML ****39**](#acid-feature---shown-in-cml)

[**SDX Integration (Fine Grained Access Control) ****43**](#sdx-integration-fine-grained-access-control)

[**\[OPTIONAL\] Load Iceberg Table using Cloudera DataFlow \[UNDER CONSTRUCTION - WORK IN PROGRESS\] ****44**](#optional-load-iceberg-table-using-cloudera-dataflow-under-construction---work-in-progress)

[**\[OPTIONAL\] Iceberg Table Format Table(s) joined with Hive Table Format Table(s) using Data Viz ****45**](#optional-iceberg-table-format-tables-joined-with-hive-table-format-tables-using-data-viz)

[**\[OPTIONAL\] Iceberg Table Format Table Maintenance Tasks ****46**](#optional-iceberg-table-format-table-maintenance-tasks)

[**4. Appendix ****51**](#appendix)

[**Coming Soon (planned) ****51**](#coming-soon-planned)

1. # Summary<a id="summary"></a>

This demo is a “How to build a watch” demo - this means that you will be completely responsible for setup and care of the demo.  These instructions include: 1) setup of the Demo; and 2) What you will execute as part of the demo and some instructions on how to deliver the demo.

Use Case: Airline specific, focused on a more technical tilt of starting with the premise of an already existing Data Warehouse where everything is in a Hive table format and you will work through the demo to convert the tables to Iceberg Table Format.

Value Propositions: Take advantage of Iceberg - CDP’s Open Data Lakehouse, to experience better performance, lower maintenance responsibilities, and greater control.

![](https://lh7-us.googleusercontent.com/J18TygugNk0ViztrHbV-VbHZiSJn6p_5LTHhABogXxK3OsuZMOn1w_Rt3aBK5euTdiN51BHUg1a95dV9A65P2AAuaXzVEpVDfJgWrxDOSPTu-lwLBFHFE2f7fJ9WJCG1H-Jyz4Vdvwx2ZaX81-sJIQ)

Schema used for this Demo

2. # Demo Install/Setup<a id="demo-installsetup"></a>

**Sandbox Tenant**

**Environment:** se-sandboxx-aws \<OR> create your own


## CDP Environment<a id="cdp-environment"></a>

- **REQUIRED:** you must have access to the S3 bucket for whatever CDP Environment you plan to use

- You have two (2) options

1. Use an existing CDP Environment in the Sandbox Tenant, such as se-sandboxx-aws

2. Create your own CDP Environment \[for screenshots in this Runbook, this is the route that was used]


## Getting Airline Data to Cloud Storage<a id="getting-airline-data-to-cloud-storage"></a>

- Download files from Google Drive in this [folder](https://drive.google.com/drive/folders/1y0bHTrco4qiFBozEbnoCsD2vG19htRWS?usp=share_link)

  - Click on the drop down next to folder, and select Download

![](https://lh7-us.googleusercontent.com/1rwzdYjqfmNN3QROqUoT-nay0dNFqx6zb_uITM5O931cIhw6oon8sU7-_JWJv9U_ngH3UOkGXNBgPXbGCvfC4NOClzDxsGNcKixYKeW6Df1XmNu53kq82Ji1SeOnAybu2hdVkCqHNTp8fgDxgqRFDbo)

- This will create a zip file and downloads 2 files

![](https://lh7-us.googleusercontent.com/XraDjQ9z_HZbaBeNUFxyKgkBntlKrGLO6dSrXwImW_qCpVorKBU450_UJdMMJEyUVchlMEUxobQ3TGhdJH8UokB_FRDO-68-bM7pi725ASUgB1ZUMGNZw7uiQ8kSXxFoSMniRLjZ51dwagTjGc8a4KU)

- Unzip the downloaded zip file

![](https://lh7-us.googleusercontent.com/w1JDDAGiPrhOap7pWAyO6vTIvbF7tT5K9WATxfxyALUpagDwMe87rLSPDotvw_Wpdl3laEPZG-zkULzlH6eI9ewgB0nqUz3HXK_zcHgKaEaHTUvyl1ZP7WtBu6qLKDxlBPjUrB5ck8olXy2nx3i_BOk)

- In the airlines-csv directory, create a New Directory named “flights”

![](https://lh7-us.googleusercontent.com/h0ejQDhV0oRch4P4GIXod5g6Zsd1oIfARQjcb4AIX7dd-OaVuVcyHJWpTemyf4FJIlD3ZcuHYbWPxJIE5eG7r-Eal_ZZ1g7eXp8zwBJfdhfAAjbzkEFPpNgr1z2QjR4Zqiv1r_H2dnVVS4yIWdBmRHQ)

- Rename the flights-002.csv file to “flights.csv”

![](https://lh7-us.googleusercontent.com/l2WCOSEUW8aXIc6Q3Chvmc1rFWg3yfhjBEMQVZkQfx-RrJgkELgXHVKM3Cg00V6zwiMuf-h7_jnf0nHAgJoC2mjUROpEnC_tY493OM_R83l3gQQ1DyVAXx1W6sAa8l_T53gqMG0ZR01MQlNbaRcSTVA)

- Move flights.csv file to the airlines-csv/flights directory

![](https://lh7-us.googleusercontent.com/lLCR9IPkL_FchkWL5O3CUUTWq__FB3EGbD92VdM5Y6yV2xeUhhRMdHiS5L_SPJugKCa7i-apfridUNICoC6iu3mfK5-UwiVcuAlg7N9Yey4zWyQyLqv9CCICBujqoSzaTpDVx7zTkDsaF_YbLbnC-WE)

- In the AWS Console for s3 

  - Navigate to the bucket used for creating the environment

![](https://lh7-us.googleusercontent.com/uXUDSVf5ID8-wszzpRS6sHwii-XLEK9-t7j5Ex-7w88Wb9diSHJ5YIAa_yror1yQVG1XpAYdv5NIk7b0M4FjNFOheatHaeOdl3-p3SsCBEtN23BsQbhUCdQfAY02Zh2wXFuydmIic4yVy1kdMMmBMPc)

- Create a folder named - iceberg-hott

  - This new folder location should be something like “s3a://\<cdp-bucket>/iceberg-hott”

- \[In AWS S3, Upload] With the folder you just created selected, click on Upload, on the Upload screen click “Add folder” button and browse your computer to the  airlines-csv directory (download from Google Drive)

![](https://lh7-us.googleusercontent.com/sTlZU4U8IWp7AL69O3xwJovzgk238B0Cv5KVPuKdaJoWoJqeVs-d3SLt_FGfbkOX7ExMIXifapNsRZn8hQ_uriuFeJgEb16xzH5P26kLs18QupcDnAQ6ZABK5bYc1lCSjNjSfwZb3qqS66Z5bag7gMo)

- Browse each of the folder to make sure the CSV file in the folders - flights, planes, airlines, and airports

![](https://lh7-us.googleusercontent.com/MmB811jKzeqxKv7ki9L_hb87ExEUDupp6LBb2bRzWqQxPaNd33RtUZRODxbS9ktTPvAGbG-eL4PeECQ3EA21my4PWmArXsk8I-4OxrXwOXdLLc26lRYv0clGBFxzcV7eka88afbH-GzpPWVpRbnYceg) >>![](https://lh7-us.googleusercontent.com/iRiv0abnlfk-XzWomKJb_ymzI_wQK_konN7jSGHUh0TPfn9Y9V3JPe1l5FaxqZHrVLMBLMN0yn_XqGaWdsnPsyOzoCzDvRZ9g5qbRu2yRtkr5OjoevkLjkazGuSEpDviXoFXo7n_TR_95ppBakbpelE)


## Create CDW Virtual Warehouses<a id="create-cdw-virtual-warehouses"></a>

- In CDW ensure that the Environment you are using is enabled for use with CDW

  - For this Runbook you can use the Default Database Catalog that is automatically created with enabling CDW for the Environment

- Create two CDW Virtual Warehouses attached to the Default Database Catalog, replace \<user-id> with your user id

  - Create a Hive VW

    - Name: “**\<user-id>**-iceberg-hive-vw”

    - Type: Hive

    - Database Catalog: select the Default DBC for the Environment you are using

    - Size: x-small

    - Concurrency Autoscaling > Executors: make sure the max is > (# Attendees \* 2)

    - Leave remaining options as default (these can be changed but for this Runbook there are no specific settings that are required)

  - Create an Impala VW

    - Name: “**\<user-id>**-iceberg-impala-vw”

    - Type: Impala

    - Database Catalog: select the Default DBC for the Environment you are using

    - Size: x-small

    - Unified Analytics: enabled

    - Executors: make sure the max is > (# Attendees \* 2)

    - Leave remaining options as default (these can be changed but for this Runbook there are no specific settings that are required)


## Setting up the Databases for the Demo<a id="setting-up-the-databases-for-the-demo"></a>

- Execute the following in HUE using the Hive VW named **\<user-id>-iceberg-hive-vw**

  - Copy & paste the SQL (`in gray`) below

    - HUE has determined that there are 2 parameters that need to be entered, in the “user\_id” box enter your user id; and in the “cdp\_env\_bucket” box enter the CDP Environment bucket name

![](https://lh7-us.googleusercontent.com/ORh5JX6mRCmt9QzZMOxrMVw4hCmFET-yRIFa5nKOjPR-yIAdHFNWLBPNJh2869QW_P9ysii9EZljaR-es8xEoRBAX5WXua4n82vInMhVJ_CgixKHDO6o7aycC-DUdhpdgINpzE2Cd5wBb5cmd_A1LJs)

    -- CREATE DATABASES
    CREATE DATABASE ${user_id}_airlines_csv;
    CREATE DATABASE ${user_id}_airlines;
    CREATE DATABASE ${user_id}_airlines_maint;

    -- CREATE CSV TABLES
    drop table if exists ${user_id}_airlines_csv.flights_csv;

    CREATE EXTERNAL TABLE ${user_id}_airlines_csv.flights_csv (month int, dayofmonth int, dayofweek int, deptime int, crsdeptime int, arrtime int, crsarrtime int, uniquecarrier string, flightnum int, tailnum string, actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, depdelay int, origin string, dest string, distance int, taxiin int, taxiout int, cancelled int, cancellationcode string, diverted string, carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int, year int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE LOCATION 's3a://${cdp_env_bucket}/airlines-csv/flights' tblproperties("skip.header.line.count"="1");

    drop table if exists ${user_id}_airlines_csv.planes_csv;

    CREATE EXTERNAL TABLE ${user_id}_airlines_csv.planes_csv (tailnum string, owner_type string, manufacturer string, issue_date string, model string, status string, aircraft_type string, engine_type string, year int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE LOCATION 's3a://${cdp_env_bucket}/airlines-csv/planes' tblproperties("skip.header.line.count"="1");

    drop table if exists ${user_id}_airlines_csv.airlines_csv;

    CREATE EXTERNAL TABLE ${user_id}_airlines_csv.airlines_csv (code string, description string) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE LOCATION 's3a://${cdp_env_bucket}/airlines-csv/airlines/' tblproperties("skip.header.line.count"="1");

    drop table if exists ${user_id}_airlines_csv. airports_csv;

    CREATE EXTERNAL TABLE ${user_id}_airlines_csv.airports_csv (iata string, airport string, city string, state DOUBLE, country string, lat DOUBLE, lon DOUBLE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE LOCATION 's3a://${cdp_env_bucket}/airlines-csv/airports' tblproperties("skip.header.line.count"="1");

    drop table if exists ${user_id}_airlines_csv.unique_tickets_csv;

    CREATE external TABLE ${user_id}_airlines_csv.unique_tickets_csv (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING, leg1origin STRING,   leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,   
     leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,   
     leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,   
     leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,   leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT ) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
    STORED AS TEXTFILE LOCATION 's3a://${cdp_env_bucket}/airlines-csv/unique_tickets' 
    tblproperties("skip.header.line.count"="1");

    -- CREATE HIVE TABLE FORMAT STORED AS PARQUET
    drop table if exists ${user_id}_airlines.planes;

    CREATE EXTERNAL TABLE ${user_id}_airlines.planes (
      tailnum STRING, owner_type STRING, manufacturer STRING, issue_date STRING,
      model STRING, status STRING, aircraft_type STRING,  engine_type STRING, year INT 
    ) 
    STORED AS PARQUET
    ;

    INSERT INTO ${user_id}_airlines.planes
      SELECT * FROM ${user_id}_airlines_csv.planes_csv;

    drop table if exists ${user_id}_airlines.unique_tickets;

    CREATE EXTERNAL TABLE ${user_id}_airlines.unique_tickets (
      ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING,
      leg1origin STRING,   leg1dest STRING, leg1month BIGINT,
      leg1dayofmonth BIGINT, leg1dayofweek BIGINT, leg1deptime BIGINT,
      leg1arrtime BIGINT, leg2flightnum BIGINT, leg2uniquecarrier STRING,
      leg2origin STRING, leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,
      leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT 
    ) 
    STORED AS PARQUET
    TBLPROPERTIES ('external.table.purge'='true');

    INSERT INTO ${user_id}_airlines.unique_tickets
      SELECT * FROM ${user_id}_airlines_csv.unique_tickets_csv;

    -- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
    drop table if exists ${user_id}_airlines.flights_iceberg;

    CREATE EXTERNAL TABLE ${user_id}_airlines.flights_iceberg (
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
    STORED AS PARQUET
    ;

    -- LOAD DATA INTO ICEBERG TABLE FORMAT STORED AS PARQUET
    INSERT INTO ${user_id}_airlines.flights_iceberg
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
     WHERE year <= 2006;

    -- [TABLE MAINTENANCE] CREATE FLIGHTS TABLE IN ICEBERG TABLE FORMAT STORED AS PARQUET
    drop table if exists ${user_id}_airlines_maint.flights;

    CREATE TABLE ${user_id}_airlines_maint.flights (
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
    STORED AS PARQUET
    ;

- Execute the following in HUE using the Hive VW to test that data has loaded correctly

  - Copy & paste the SQL below into HUE, in the “user\_id” parameter box enter your user id

<!---->

    -- TEST FLIGHTS CSV TABLE
    SELECT COUNT(*) FROM ${user_id}_airlines_csv.flights_csv;

Ensure that correct count is being returned

- Copy & paste the SQL below into HUE, in the “user\_id” parameter box enter your user id

<!---->

    -- TEST PLANES CSV TABLE
    SELECT COUNT(*) FROM ${user_id}_airlines_csv.planes_csv;

- Copy & paste the SQL below into HUE, in the “user\_id” parameter box enter your user id

<!---->

    -- TEST PLANES PROPERTIES
    DESCRIBE FORMATTED ${user_id}_airlines.planes;

Pay attention to the following properties: Table Type, SerDe Library, and Location

- Copy & paste the SQL below into HUE, in the “user\_id” parameter box enter your user id

<!---->

    -- TEST PLANES PARQUET TABLE
    SELECT * FROM ${user_id}_airlines.planes LIMIT 10;

Ensure records are being returned, tailnum should not be null, but other columns are empty or null


## \[OPTIONAL] Cloudera Data Flow (CDF) Setup \[UNDER CONSTRUCTION - WORK IN PROGRESS]<a id="optional-cloudera-data-flow-cdf-setup-under-construction---work-in-progress"></a>

- **NOTE:** be sure to run the optional section of the Hive SQL script to create the necessary tables for this section.  The tables are - weather, weather\_clouds, & weather\_conditions

- Signup for free Weather CheckWX License Key - this API was chosen as this is a good API for Weather data related to airline travel

  - Website - <https://www.checkwxapi.com/>

  - Click “Sign Up” button or “Get Started Free” button

    - Fill out form & save API key for later use

![](https://lh7-us.googleusercontent.com/hhmEopoJQSOFE3Gs2x9QdVof73L97Quk5dqWr22-4fI3WOOzN5OL2hwxuyBvPDybpjBQFkvVxCyV0q0BSYORQIBcsiMRrdjzIsZTPB_JDVxDb2oiKC4kM7u8IAhlW-EhhiyK4hA9Q3JjazlHT_-NrA)

- Documentation: <https://www.checkwxapi.com/documentation/metar>

-

* Create DataHub Cluster for Streams Messaging - default settings is fine

  - In this DataHub open Streams Messaging Manager (SMM)

    - Click on Topics

![](https://lh7-us.googleusercontent.com/SyabNFzkt4wzjBs8SM3nnGOn19VMMAG_04mfuxdyvazRU0T_UK38N6RGEUEgxyeL3cqwqGuH0C9zasitRLO0KcfiAhUk6z2NvaqqfWyD_g8FZISDyLtfrcKXItEp3g_B2y0s9LEtkyhLfcUF8fN7Atc)

When finished it should look like this when you search on “weather”

![](https://lh7-us.googleusercontent.com/eXgFvs8fXhcyk-WdvTXDZsDM9atoMCGTrCH8phCYuSkKJk-ygo66V9NhO6xxvlMFZmVR7BNwtSVnc7lWQXNnu4NgVUjzWV7ecWfnK_fkW7mbFhULksCJ0YvGWMOswBqYrMx1ypjyUDoy_jwSTH4lsFM)

- Create 3 Kafka Topics - each with 3 partitions; Delete cleanup policy; & Maximum availability

  - “weather”

![](https://lh7-us.googleusercontent.com/cKwfvdUC5tOztAbALPF_EWWQjp6V5DcpFAok0fVG2BkrApP5rLbWA4O13endqc79GMP-WNOyCxeSnaeyJgiqk_3qgCMIPT4OumC1GQ8Y3XSFOirc6lsRPwO-ak08ylH7LnG7j2NoOnnL2LDWr0Hpt7U)

- “weather\_clouds”

![](https://lh7-us.googleusercontent.com/hzETAV_cuS6er5OIbAqkVu3FTMVDxx6rmlOrWgOV4wZxEbtfYWvQ028O66gAYrv8V0amPQbp1GwefTM7zAxWPcKN400YNpopTHa8mLi720CvQHgjo6etDQ7YFo-tN5-3TSCv8Wi1A7i9a_KBcBnKtNc)

- “weather\_conditions”

![](https://lh7-us.googleusercontent.com/kysiHsUTa--iH0qTNDvIRaoO4QK5P1KV0UVjwGY9qPn5amJ7GFNkNXl_v43SIONjvGyT2w8-cqKlbJW8wbob30HliUG6iChOCa1PfYbfj9LhzLBOh3rpLKEtPtJSWhtwH1V5vBYg05D3j6TbiM6__A)

- in this DataHub open Schema Registry

  - Create 3 Schema Registry avro schemas - found here - <https://drive.google.com/drive/folders/1eioXyxSNvucbgJ_QTs-2HkphcEMtBr7n?usp=share_link>

When completed it should look like the following

![](https://lh7-us.googleusercontent.com/tn8eL0CNT9tb0mBHrI4I0bjEAmQmpRtUcoFkY1eQYSUWyJ4B2CwteL3mQrUkTDkb5Pc1uzxVfZTiQgQ7fSg4hQbqR4otkPnD0k1kcDRHZY7VzxEe8EgYILJ0oprt0NYZ5CmIb8BWjdgrTrjFk7WBOVM)

- “weather”

![](https://lh7-us.googleusercontent.com/q7lXgYzeX8YlEM2Rc_J6su2uKcAkGbl_oJxD9hzReALrkgeJi85KjT6j5V5yqOT6Pn22zGy6W5FXTyaZArjG3x3PvQu7VoqNEd6aUktM2ZVnsBRglhS7txG8GoxxEs6BC_ZERbppDfoPWjXA5OflvEE)

![](https://lh7-us.googleusercontent.com/TePJOhZaq3Q2thaKev-DSI4x9k_VUw2G4r_fxaHnLDUElZEWCt1lXMxwNUZA0VW1WYlAsLl4J9XRk7Q9iq8KuAuko82t_XCAREmHxHznX02OsK5a2C_pJmS0yJl5cNxxedxZPu54dRE_YrAkD7j9kCg)

- “weather\_clouds”

- “weather\_conditions”

* Deploy Data Flow .json file - if not in catalog, upload the following file to the CDF Catalog - found here - <https://drive.google.com/file/d/1bBdT4kKMScZyyY8giTC4zzGxa_W-HwFF/view?usp=share_link>

![](https://lh7-us.googleusercontent.com/s3SPcYc7jy5yPNxnPFGzielHtbdUDkDaZdIOIfcOFut3REzyCEM69vKeUv2hIMtsMQmX9FEtiYmGBlCYYCy8SfHkc7XN_P-msM_0V6gM6BbMBSsInNAtbDIA6bPZ_wENK4GC9EkuvR7-fWb79vy1r68)

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


## \[OPTIONAL] Data Visualization (CDV) Setup<a id="optional-data-visualization-cdv-setup"></a>

- Create or add your user id to a User Group, this will be needed to provide Admin access to CDV

  - These instructions will use a Group named “sales\_process\_demo-admins”

  - To be able to follow these steps you will need to have Admin access in CDV

- Create CDW DataViz instance 

  - In CDW, create a Data Visualization instance (replace \<user-id> with your user id) named **\<user-id>-iceberg-cdv**

    - Size: small

    - Environment: select the environment you have been using for CDW steps

    - Admin Groups: \<sales\_process\_demo-admins> (replace with the group you use, if you choose to use another Group)

![](https://lh7-us.googleusercontent.com/04Z5cfK-uGST-ao39ELZbHSxnj74opihoGYXKfcD1C6bZzeDwCYqCzXgCmIjQsDou7I5AyPQ5AuNS96fsdxa18EFEfcDOLOSUlaBIK0PSGeosSHPbygpmc22ZzNlBNG7lufBm6_UAYPl_jwAfnE6kTA)

- Create a Connection in CDV to the  

  - Open CDV

  - Click on the DATA tab

  - Click on NEW CONNECTION button

  - In CDV, create a Connection named **Airlines Impala VW**

![](https://lh7-us.googleusercontent.com/uElbuJyAkLRyK138gz044Rfccc9yNIqfrqq4h-lpoVoEOGr9SLa5LFJsI1mTgVaPhebeXajC1nk2adHOIaogHa-RUlCAgIEiLSp49AS-XoR4AFK1tZqc1T69KSq8M3q5ZCLPNm2ZeomWPRDkBeyIfDc)

- In CDW Warehouse select the Impala CDW VW named “**\<user\_id>**-iceberg-impala-vw”

- This should fill in details on the Basic tab

- Click on the CONNECT button, there is no need to change anything further including entering a Password (the connection will use your SSO Id to connect to the Impala DB)

* Import CDV Artifacts - Dataset & Dashboard

  - Download the [iceberg\_runbook\_cdv\_v1.json](https://drive.google.com/file/d/1MP9PKmxoh7qpDpoX90Dp6EVSngwhYf2j/view?usp=share_link) file and save it to a place you can remember

  - Click on the “Import Visual Artifacts”

![](https://lh7-us.googleusercontent.com/vDTDJZWyJhYKZ3HF9PLTFQcnhSgREDhgLaxW-a7T34JrbWhjQnKd4KfxWa_ic5OJkPzcVvKlgqzMYizaVUiqEBoTSAcTNZFN9IzpL4QBxdgEsG6_9f-tsO0_njeJVtQNeI3knAIxZv_N-OabknDKgac)

- Select the JSON file - **iceberg\_runbook\_cdv\_v1.json** 

  - Click the checkbox next to “Check data table compatibility” to turn it off

  - Click on the IMPORT button

![](https://lh7-us.googleusercontent.com/GSXX_tujCLyWZ-72e56Pt8OFjm_3EKjYnG1jr8Jcbusp-R6d4-QpjYgC8f96SEuNTP_DEhNVRAD9H1zXgibDAMKq7rWsQCVSynecHi6sCMmwvAq1t4uqzZYv_-uASLW_n9ZRLS9AWD2gZwgKr-UZBlo)

- Click the ACCEPT AND IMPORT BUTTON

![](https://lh7-us.googleusercontent.com/Mq07UcpBKU7A0j2WWyFKYaPG9Erzw5_pvuHtEUs1u9q2E0fa5xcayx2ul5sYtGfOuP2h_L9rCtpMA1zSlivOWstiDzWZyoonFkpd0LWpmb90Hqx9Z5lxXy2tsnnxKgotMT32zjAe-dhWuOYWIdgyxKY)  >>>  ![](https://lh7-us.googleusercontent.com/ih5k2WlCjI8t5vw8c03RI95HJSxrXFrn6lJyR9TQSKf8FK_W_W47JoZdHMOiwt2ZIS1h8yrCxwQvjNbgIEFGj9IvztKOFvAjYOgUnQJkbPojlltLiVmy1RG2zyMq6nVIFVPrtiYH-TtiMKJiXU4Mmfw)

- Click on the DATA tab, you will now have a Dataset named “Airlines Lakehouse”

![](https://lh7-us.googleusercontent.com/o_nZFAh7xF_X12NyTmgNJ8qJ42wTQkIUsi8SCmXkuaR1e8Gqqiio76SbahJr5mWoF2YPOwu1s3Rdo-6MBcsmkkx51erL8NdR3GUSlwo-1anSzzHKUfHjsQr0kmvfMa4GKPtuwuYJVETeqj1icOK2zOg)

- Click on the “Airlines Lakehouse” to open the Dataset

  - On the left navigation menu select Data Model

  - To use this Dataset with the Database **\<user\_id>**\_airlines, we need to modify the data model so that all tables point to the tables that you created previously in HUE

    - Click on the ![](https://lh7-us.googleusercontent.com/WFeeS7lE1aKJNO6HscACLjAzAkmflmihH7-JB68WVX12vlNvHDJDxRsocS5M2nYEE6wmazWRCQrIeXxyMp-VYn2cvRyxif5ncR6BQ12t38Ck6uO_xarB0A83sAjnB6iEwaqmtRmOm0lm-TT8m90lZpY)button

    - For each of the 4 tables in the Data Model, perform the following

      - Click on the name of each of the tables

      - Database: select the drop down and select **\<user\_id>**\_airlines

      - Table: select the name of the table you are editing

    - Click on the SHOW DATA button, to preview the data. You should see data populated for each of the tables, like the following

![](https://lh7-us.googleusercontent.com/QfqZa3pjsYsC0OWf1X2G4uqZTZgn9P6UDo9Hk8Ng6UvthY8tQqtijaIiw26RTVXnRbcWBFYHVeaxbeP8HU0lae4swaqBMHzU_vjbcFgcuk3L8OiXMBdxMXBRh-D5DxWjagm1VFa7Bbr_nniAC4Ny98g)

- Click on the SAVE button to save changes to the Data Model

* Click on the VISUALS tab

  - Make sure to select the Private WORKSPACE

  - Click on the Dashboard named “Airlines Dashboard”

![](https://lh7-us.googleusercontent.com/3ZQ3WawD9mXA23t3QbDXY_JMcdIar2c8Q7ofarxpXVHeAsC5OLPr4n4XU5lvk32bUQIqbTyV3zzS_h8o4DXX3kT0fmwoJCrg1RCov8NG21jCYtvgql9X77Wq7fXjv7oW_hn6g_xF4ltwgddAcOtZKQ0)

![](https://lh7-us.googleusercontent.com/rWMtxX9CgpmCQlhrpkvpnRWYgH0e4836k_B5kJqJfXqwSTFLPkHu98Nd5Vf2Hx2tnQUU9gWULd9qkKM_Pn2_IntlDvHWVcT5qme4uVWYlB72Lb1Zj5xb-6PGUpRaD0PxJ1JdGycBzUqnB6Gt9S9TrO4)

- If the Dashboard is populated then the setup is complete


## CDE Setup<a id="cde-setup"></a>

- Enable a New CDE Service, replace \<user-id> with your user id

  - Name: **\<user-id>**-iceberg-de

  - Workload Type: General - Small

  - Enable Public Load balancer: checked

![](https://lh7-us.googleusercontent.com/uXX6X9SYNwwtTbLB5FGHg4lgYGST801XlSLGeTHmD5veF4RXmSmaGsWortiPuS5hxKy24g84q1xYZzmhjSpT1Nig3E8bO1qPqh3yMJJcwS5tzTyQL9oXc0DWOF2V3cjy-pdJIZ60EFjTIM3BS7nFBw)

- Add a New Virtual Cluster, replace \<user-id> with your user id

  - Name: **\<user-id>**-iceberg-vc

  - Spark Version: Spark 3.2.0 \[required for Iceberg]

  - Enable Iceberg analytic tables: checked \[required]

  - Other settings can remain default

![](https://lh7-us.googleusercontent.com/ek5s9Uo4jb5CneFvnXcNzugasFeeGbGbUtyXCLp_33x5GjQSUcsX8Q7z4x4b1aE00Ek5gsFmv8OSc1E3KXb9wwuOWf_iJLDgNJmuC0hFN8s2ZDL1uPLeOt9bCP5Qaz2HAvqpU1J-6iI0BsIxocLOUw)


## CML Setup<a id="cml-setup"></a>

- Create a CML Workspace, replace \<user-id> with your user id

  - Name: **\<user-id>**\_iceberg\_cml

  - Advanced Options

    - Instance Type: m5.4xlarge

    - GPU instances: Off

    - Enable Public IP Address for Load Balancer: checked

    - All other options can stay default

![](https://lh7-us.googleusercontent.com/_RoAAJkvqIZhIpRwnwnOlLWXX4k4w90PKcGKw_hEc5gNRRNQTG7FVPx2zrbnoOl2q0o8mKeZLHRf2S0J0CSjebcEHQL4BQkb68uwfenenN71o7QTgMkK7OJ05o2VizsYkwX5jN2UqU8xIABsp_h9Tg)


## Ranger Policy Setup<a id="ranger-policy-setup"></a>

- Open Ranger UI for the Environment you are using.

- Open Hadoop SQL

![](https://lh7-us.googleusercontent.com/jeHnfDVt0aW1Jd2eD1lXiw41ceEAvipV8wgH88mfOoRNuCltttRXrTC9sLeOvU6WbOfmdzkuKnVS-lQTmW3YcZcGuY2i93wIbcQkRg5p_LnpbSUhb_f958ntv22NMEfbxsNqrngz94MjyAvQMn1QbA)

- On the Masking tab, Add a New Policy as follows, replacing \<user-id> with your user id and then clicking on the Add button.

  - Policy Name:  **\<user-id>**-iceberg-fgac

  - Hive Database:  **\<user-id>**\_airlines

  - HIve Table: planes

  - Hive Column: tailnum

  - Mask Conditions:

    - Select User:  **\<user-id>**

    - Select Access Types: select

    - Select Masking Option: Hash

![](https://lh7-us.googleusercontent.com/vOpN1BLCQWwMj_74Q5vEDmCmqQAlxpWs7I__Hwvol8pHPY5EJcoNsch--B-R5Fn7DIh_AXhI75ufYllpPV5rf2mtywgEL3azVDoa4EmdxXvtPN3c5FV8BzTPowpUOOT9smqBAuctDBVAQ4Fr-KQjcQ)      ![](https://lh7-us.googleusercontent.com/vOpN1BLCQWwMj_74Q5vEDmCmqQAlxpWs7I__Hwvol8pHPY5EJcoNsch--B-R5Fn7DIh_AXhI75ufYllpPV5rf2mtywgEL3azVDoa4EmdxXvtPN3c5FV8BzTPowpUOOT9smqBAuctDBVAQ4Fr-KQjcQ)

- To test the Policy works properly.  Execute the following in CDW, by opening HUE for the Impala VW named **\<user-id>-iceberg-impala-vw**

  - Execute the following SQL, enter your user id in the parameter box

<!---->

    SELECT * FROM ${user_id}_airlines.planes;

- In results you see that the Tailnum column has now been HASHed

![](https://lh7-us.googleusercontent.com/N3BW3pmdyRVJJtBBpLgVi0B89BNc3IQeq7hom0wdwUhaNY0qwgzbYZ12y1xGWdw89Wmqj596F_KBsNSfQoTqI9kDItyxlJ71RYqloVTPrBzrIHk45FLRy4Sz83v6xzTq6pAg6AziSYGYUYnBsR0B_Q)

- Disable the Ranger Policy (during the demo all you’ll have to do is enable the policy)

  - Return to Ranger and disable the masking policy you just created, and click Save button

![](https://lh7-us.googleusercontent.com/kKkWrJvWHOzPBeLtDHPy_AThqb56YKrdrraS1bxn32o3QM7N10vsKES8wxC1LOzDKu4XJD8qs7Zd0TZgYIzmXJ46XWkCzxXPqMqkWgnIHwV5Djq0lXw-C05CEpIG58LTXiOdAZyHrEDswfsxqAIAKA)

3. # Demo Steps<a id="demo-steps"></a>

The following describes the steps to take and how to deliver this demo.


# Prior to Demo - SQL that will be run in Hive VW HUE<a id="prior-to-demo---sql-that-will-be-run-in-hive-vw-hue"></a>

- Copy & paste the following to the HUE Editor for the Hive VW named **\<user-id>-iceberg-hive-vw**.  It will be Executed later

<!---->

    -- Migrate Table Feature
    DESCRIBE FORMATTED ${user_id}_airlines.planes;

    ALTER TABLE ${user_id}_airlines.planes
    SET TBLPROPERTIES ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');

    DESCRIBE FORMATTED ${user_id}_airlines.planes;

    --
    -- CTAS to create Iceberg Table format
    drop table if exists ${user_id}_airlines.airports;

    CREATE EXTERNAL TABLE ${user_id}_airlines.airports
    STORED BY ICEBERG AS
      SELECT * FROM ${user_id}_airlines_csv.airports_csv;

    DESCRIBE FORMATTED ${user_id}_airlines.airports;

    --
    -- Create Partitioned Iceberg Table
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

    --
    -- Load Data into Partitioned Iceberg Table
    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
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
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
     WHERE year = 2007;

    --
    -- [OPTIONAL] TABLE MAINTENANCE FEATURES
    --     1. [CML] Table Compaction
    --     2. Rollback
    --     3. Expire Snapshot(s)

    -- 1. Go to run CML code first, as this will load the data

    -- 2. Check data that was loaded - will see year=9999 (invalid)
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

    -- 3. EXPIRE SNAPSHOT(S)
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

    -- Expire Snapshots up to the specified timestamp
    --      BE CAREFUL: Once you run this you will not be able to Time Travel for any Snapshots that you Expire
    ALTER TABLE ${user_id}_airlines_maint.flights EXECUTE expire_snapshots('${create_ts}');

    -- Ensure Snapshots have been removed
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;


# Prior to Demo - SQL that will be run in Impala VW HUE<a id="prior-to-demo---sql-that-will-be-run-in-impala-vw-hue"></a>

- In HUE Editor for the Impala VW named **\<user-id>-iceberg-impala-vw**, make sure to select the IMPALA Editor (for now do not use Unified Analytics, hasn’t been test for this Runbook)

  - To switch to the Impala Editor - on the left navigation click on the ![](https://lh7-us.googleusercontent.com/kHx-lYoympCvygXLHFb34Nze4kE30-Kdehi8GcN_uUCOXalRnI_8wZfI6OqfJGk3Uq0Xi-Esf99fZDye4b_p1qSZZp_aFnmk7N86ppvSk5UmeDIMub9Mxr-cPDEgU_5a7aWC41auo_-ghIYnVMgKtHY) and select ![](https://lh7-us.googleusercontent.com/PG8absn_38jV9GZ1evW-JyauXMrF1tNQIgCA4UFbnV5XZc7ByuFGpXaCAbI1bjBwE0U6lDuzgF2Lj-_at2L7HgYHNhAktSyQPlH168QkE7ERtMJDcJe1HP0WrqhZfqtkDzr1JfeKXgmVD52whKxq1gk)

\
![](https://lh7-us.googleusercontent.com/kHx-lYoympCvygXLHFb34Nze4kE30-Kdehi8GcN_uUCOXalRnI_8wZfI6OqfJGk3Uq0Xi-Esf99fZDye4b_p1qSZZp_aFnmk7N86ppvSk5UmeDIMub9Mxr-cPDEgU_5a7aWC41auo_-ghIYnVMgKtHY)

 Notice in the gray bar that this is using the Unified Analytics as the Editor

![](https://lh7-us.googleusercontent.com/ldcwYfkLATYkPNJVqWrzc9wu0sTy3OMEEL9KHeNPkAj51kc0i84tvKNsA7J9ZjLmuzr1byp2xp6XDnQbswvrdLSY4VaTFZtqr9avsz9wFpwygnMt4iz0UGIXdZw-Su6JA2mezlJjIxpcXAv6_KzPsXY)

After that you will see “Impala” in the gray bar above the SQL Editor.

- Copy & paste the following to the HUE Editor.  It will be Executed later 

<!---->

    --
    -- QUERY TO SEE DATA LOADED FROM CDE
    SELECT year, count(*) 
    FROM ${user_id}_airlines.flights
    GROUP BY year
    ORDER BY year desc;

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
    --
    -- [optional] SINGLE QUERY USING ICEBERG & HIVE TABLE FORMAT
    --            Uses CDV Dashboard, could also just query in HUE
    -- DESCRIBE FORMATTED ${user_id}_airlines.flights;
    DESCRIBE FORMATTED ${user_id}_airlines.unique_tickets;

    -- [optional] Query combining Hive Table Format (unique_tickets) and Iceberg Table Format (flights)
    SELECT 
    t.leg1origin,
    f.dest,
    count(*) as num_passengers
    FROM ${user_id}_airlines.unique_tickets t
    LEFT OUTER JOIN ${user_id}_airlines.flights f ON
      t.leg1origin = f.origin
      AND t.leg1dest = f.dest
      AND t.leg1flightnum = f.flightnum
      AND t.leg1month = f.month
      AND t.leg1dayofmonth = f.dayofmonth
    GROUP BY t.leg1origin, f.dest;

    --
    -- SDX FINE GRAINED ACCESS CONTROL
    --     1. Run query to see Tailnum in plain text
    --     2. Ranger - enable policy
    --     3. Run query again to see Tailnum is Hashed
    SELECT * FROM ${user_id}_airlines.planes;


# Prior to Demo - Prep Data Services that you’ll demo<a id="prior-to-demo---prep-data-services-that-youll-demo"></a>

- Open Ranger in a browser window tab

- Open CDE in a browser window tab

- Open CML Workspace in a browser window tab


# DEMO >>><a id="demo-"></a>

# Migrate Existing Tables to Iceberg Tables<a id="migrate-existing-tables-to-iceberg-tables"></a>

**Migrate table feature (in-place)**

- Execute the following in HUE for Hive VW, In the “user\_id” parameter box enter your user id 

<!---->

    DESCRIBE FORMATTED ${user_id}_airlines.planes;

![](https://lh7-us.googleusercontent.com/S2wQ2x7Z93770BlJAIFCMdpp09nlmBHfL75VE-86RG9C3Yc0dJqfHftcekyTuh8IZhcr6lnv2gNIIrXQwXPJDTgqPL48TUQg1OSV0Ut6u6rV3SPnQTkKyrvU1qZWucV5TGImZMcDn1ebPl4K2MmY1g)

- In the output - look for the following properties Table Type, and SerDe Library

![](https://lh7-us.googleusercontent.com/_dOv2Kl--_-sLLR7Q4ql1tEV1C2rwsHHZZy4Z2Iep6xZKhMAupCDFqYoMdmPNxo6QJpuXMGtqVwaraYF8ZgfTrC7z0Re0NUZ1UIn-EW-ZecO_dP_02QwuVfPX0BpsheQf0c9yfPjRwLqEQ2lgK7ijg)

- Execute the following

<!---->

    ALTER TABLE ${user_id}_airlines.planes
    SET TBLPROPERTIES ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');

    DESCRIBE FORMATTED ${user_id}_airlines.planes;

- In the output - look for the following (see image with highlighted fields) key values: Table Type, Location (location of where table data is stored), SerDe Library, and in Table Parameters look for properties MIGRATE\_TO\_ICEBERG, storage\_handler, metadata\_location, and table\_type

  - Location - Data is stored in cloud storage in this case S3 in the same location as the Hive Table Format

  - Metadata\_location - Since there is no need to regenerate data files with in-place table migration, you save time generating Iceberg tables. Only metadata is regenerated, which points to source data files.  Removes Hive Metastore as bottleneck.

  - Table\_type - indicates “ICEBERG” table format

  - Storage\_handler & SerDe Library - indicate what Serializer/Desearializer to use when reading/writing data in this case the “HiveIcebergSerDe”

![](https://lh7-us.googleusercontent.com/MNT_1IJHivbSpx3HX6q8MLnGy7dJGu6Od2FKmi7QkbWgEWCeOZwq2pkxQY2WJd6w0xwd3Et7dMeoU3f3f0oQETC3eQDhTgQYxjs89vgIaRpjoE8OgENve8NOi4Obv4XojpeU1LQA9ASE2d6daxF7bg)

**Create Table as Select (CTAS) - create new table**

- Execute the following lines in HUE for the Hive VW

<!---->

    drop table if exists ${user_id}_airlines.airports;

    CREATE EXTERNAL TABLE ${user_id}_airlines.airports
    STORED BY ICEBERG AS
      SELECT * FROM ${user_id}_airlines_csv.airports_csv;

    DESCRIBE FORMATTED ${user_id}_airlines.airports;

- In the output - look for table\_type in the Table Parameters section to ensure that the table is in “ICEBERG” format


# Create Iceberg Table Feature<a id="create-iceberg-table-feature"></a>

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

![](https://lh7-us.googleusercontent.com/piH4gptXfM6WniscddPpHGtiPfxHXUKI8f1AuW22AQe1vgpIGgsCn3P5mw_gLZozfAQQVYnDt9viJoI5Bc902Vbor9QGMuVi84Q9GUX7rr7UQ2b_BWqyq5gzl0LTcL1o6yhz8PV4VNgNkhA50w6zNw)


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


# Evolution (Partition & Schema)<a id="evolution-partition--schema"></a>

In this Demo, we’ll just be exploring Partition Evolution.

**In-place Table Evolution feature**

- Execute the following lines in HUE for the Hive VW 

<!---->

    ALTER TABLE ${user_id}_airlines.flights
    SET PARTITION spec ( year, month );

- **Note**: this ALTER TABLE happens in-place and no data is manipulated existing data remains indexed by Year

* Execute the following

<!---->

    SHOW CREATE TABLE ${user_id}_airlines.flights;

- In the output - look for the Partition Spec to see the table is now partitioned by Year & Month

![](https://lh7-us.googleusercontent.com/_dMv27tLq0hfKHhFbj6mzQuRgpGcbBcRCdSafEhDZ70zukj81WAdZgPXpaSDidtVTdfsPae7Hn7r6KZvqyQHPl793zkwXkdtv04pD6coufqSzWYo9s1IC6extMJMwqGgiRKbRC2YwgM7wgC9vdp30g)

- Execute the following lines in HUE for the Hive VW to load additional data into the flights table to leverage the new partitioning strategy

<!---->

    INSERT INTO ${user_id}_airlines.flights
     SELECT * FROM ${user_id}_airlines_csv.flights_csv
     WHERE year = 2007;

**Impala Query Iceberg Tables - engine agnostic**

- First of all let’s switch to take advantage of the performance capabilities of Impala to query data for this part of the Runbook.

- Execute the following in HUE for Impala VW, In the “user\_id” parameter box enter your user id

<!---->

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

- Highlight the first “SELECT” statement to the “;” and click on the![](https://lh7-us.googleusercontent.com/cOHSc1ONj6QC_0tfBZtUsxFadAtrjVmFAvBa2mKLoyhccflriUmZgQXjmUD2Xl1nlKNSMjVPsWqKoFLyNV6K2GAYyIUgu0mtiMtnbkU6zTMwW5tG_wMI2aDLCo2VDkkqGJ4cUGrlCdA4HqTRJ23ADA)and select EXPLAIN.  Scroll down to the bottom of the Explain tab

![](https://lh7-us.googleusercontent.com/cOHSc1ONj6QC_0tfBZtUsxFadAtrjVmFAvBa2mKLoyhccflriUmZgQXjmUD2Xl1nlKNSMjVPsWqKoFLyNV6K2GAYyIUgu0mtiMtnbkU6zTMwW5tG_wMI2aDLCo2VDkkqGJ4cUGrlCdA4HqTRJ23ADA)               ![](https://lh7-us.googleusercontent.com/E-9pcl0uJsgGQoFZoNKtYRQaO3YRPxWtjp3qOVU5wwxu4CW4ADK_KjzZ8-PYUoYMzqfMsm6Uj5oFZEcVzRWVeDfZKxF3_auIAZ1YUEGuAZqVHjLU51wZg1vcQrMX33jl1a8tuy8T3j_iHkEx6ZYkEw)

- Highlight the second “SELECT” statement to the “;” and click on the![](https://lh7-us.googleusercontent.com/cOHSc1ONj6QC_0tfBZtUsxFadAtrjVmFAvBa2mKLoyhccflriUmZgQXjmUD2Xl1nlKNSMjVPsWqKoFLyNV6K2GAYyIUgu0mtiMtnbkU6zTMwW5tG_wMI2aDLCo2VDkkqGJ4cUGrlCdA4HqTRJ23ADA)and select EXPLAIN.  Scroll down to the bottom of the Explain tab

![](https://lh7-us.googleusercontent.com/yOFYGYCnBQXF4YGBm0xdA6Y_7hmPu1q5V7SqQRGEmCXc6mKcEuy5VV2Rngpq0yvIFc07hICCdlrV6A89UPmBOk7EqeUk1pLsTiLDBwMfswCMap9LWemrVccvutzBUKpiTAiZKPqgpLN3EkP0TnwjkA)              ![](https://lh7-us.googleusercontent.com/-KJhcvesjdLuO2D6E9w9J0XQOMbY-c_0qRIcLw6Oplo_rgJKNA-kFvzUoQmRIe3kYsew17boETUAGDb-ktB6X71gh_FWEmbXGDla4OSkb_JZ0HJOjAcfgufVP59weq0VgrUYEmlomoZw3Jt2X5ugkg)

- If you remember from an early query we ran, you saw that the counts for each year are fairly uniform.  But since we had 2 different partitioning strategies when we compare the Explain Plans we can see that the Query for year=2006 (uses the year partition still), so we see that this query scans 1 partition that is \~120MB even though we also are filtering on month.  However, the Query for year=2007 (uses the year, month partition) only has to scan 1 month of data so it also only scans 1 partition, but the month partition in this case is only \~9MB.  Major Performance Boost.

**Query for year=2006**                                 **Query for year=2007**

![](https://lh7-us.googleusercontent.com/eTa_LKZONckBe7GeoJY4GM1OSwfGrG7N0Cz-AjNTucGss7R-cGbyulfryeIHVD5UWCW9JZlBv3fiU08N4OiO2P9F0eVZ6ckIoaEobDMh-Ll_UgQP2HsSXqVLl3SEJqtLxY9hGd9Iq5vm_7tkLsgV3w)


# Time Travel<a id="time-travel"></a>

In the previous steps we have been loading data into the flights Iceberg table.  Each time we add (delete or update) data a Snapshot is captured.  This is important for many reasons but the main point of the Snapshot is to ensure eventual consistency and allow for multiple reads/writes concurrently (from various engines or the same engine).  In this Runbook you take advantage of one of the features, Time Travel.  We will use this to query data from a specific point in time, a good use case where this is important is regulatory compliance.

**Time Travel feature**

- In the next few steps execute the following in HUE for Impala VW

<!---->

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

- Highlight and Execute the “DESCRIBE HISTORY” line.  This returns all of the available Snapshots for the flight Iceberg table.  There were 2 Snapshots automatically captured each time we loaded data

![](https://lh7-us.googleusercontent.com/lfGf-CrwKngaetLW0s5YAUPmnNJT-23LYt5Kth2nCRB8oSiIVNsNLpgZAjMMhnqIKKW8YyLZm_x3HFHAFEERVS0cTOSlSZIC0uO0ltmxjSwD6oYLMidnQeLZH2-JLBFweIRq45Abtnx_sXnEVXu9OA)

- In the create\_ts parameter box enter a date/time (this can be relative or specific timestamp) in the orange box under creation\_time, in this example I picked a time between the 2 snapshots of 2022-11-15 21:50:00

- In the snapshot\_id parameter box enter the number in the blue box under snapshot\_id, in this example it is 7116728088845144567

![](https://lh7-us.googleusercontent.com/2zNBMmCOBZ99vr_f0gOPG4z_D6hJvX915m_Wu3RFNuzPAR7QQMQQtL9dzd6ABCvO6aoqdtbhvFMe8mtm2u-KLTiaoXiFi2VjH5hAIH_mDDPMRlORWDWRu9TZCgTnHaeNgU66gebVKIoFLtdKBkFXTg)

- Highlight and Execute the Query with the “FOR SYSTEM\_TIME AS OF”.  You can use a specific timestamp or you can use a relative timestamp and Iceberg will take the Snapshot that was in effect as of that time specified

![](https://lh7-us.googleusercontent.com/tvnMIRyk4GaOrvqMdOpd7cS0tAa4XV8t67rLf7maVuVCz6Mdu5dWgDxeQvNcSouLqR8WGfShN_H4qbJIb7Ig2brna1OE6W-KqK8R6fsIreF4K61JT5Ll3os5r08Unba66aBrxRXZmR0uHb3UHyKQPw)

- Highlight and Execute the Query with the “FOR SYSTEM\_VERSION AS OF”.  This uses the specific snapshot\_id specified.

* In the examples used in this Runbook you should get the exact same output.  Both statements will use the same Snapshot which was our first data load that was for all data <= 2006.  If you specify the other Snapshot and run the same queries, you will see the same data plus you will see the year 2007 because this snapshot is for the last load we ran.


# CDE - Multi-function Analytics<a id="cde---multi-function-analytics"></a>

Since Iceberg is Engine Agnostic, we are not locked into using only one engine to interact with Iceberg, in this part of the demo we will use Spark in CDE to add additional data to the Flights table we created in the previous demo steps using CDW (HUE Hive).

**CDE Insert data into Iceberg Table feature**

- Open a Text Editor or IDE (I used VS Code from Microsoft).  Copy and paste the following code replacing \<user-id> with your user id.

<!---->

    #****************************************************************************
    # 
    #  ICEBERG (Multi-function Analytics) - LOAD DATA  into table created in CDW
    #
    #***************************************************************************/
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    import pyspark.sql.functions as F

    #---------------------------------------------------
    #               CREATE SPARK SESSION
    #---------------------------------------------------
    spark = SparkSession.builder.appName('Ingest').getOrCreate()

    #-----------------------------------------------------------------------------------
    # LOAD DATA 1 YEAR (2008) FROM RAW DATA CSV FILES ON AWS S3 CLOUD STORAGE
    #          A  TABLE IS ALREADY CREATED ON TOP OF THE CSV FILE
    #          RUN INSERT INTO ICEBERG TABLE FROM THE RAW CSV TABLE
    #
    #-----------------------------------------------------------------------------------
    print("JOB STARTED...")
    spark.sql("INSERT INTO <user-id>_airlines.flights SELECT * FROM <user-id>_airlines_csv.flights_csv WHERE year = 2008 ")

    print("JOB COMPLETED.\n\n")

- Save file as “IcebergAdd2008.py”, in a location that you remember

- In CDE, click on View Jobs for the Virtual Cluster named **\<user-id>-iceberg-vc**, replacing \<user-id> with your user id

- Create a new Job in CDE, using the following, replacing \<user-id> with your user id.  Once completed click on “Create and Run” button

Job Type: Spark 3.2.0

Name: **\<user-id>**-IcebergAdd2008 

Application File: File

Upload File: IcebergAdd2008.py (click on upload file, and browse to find your file)

Select a Resource: (select) Create a Resource

Resource Name: **\<user-id>**-IcebergAdd2008

Configurations - 

spark.kerberos.access.hadoopFileSystems = s3a://**\<cdp-bucket>**

Select Spark 3.2.0

Leave Advanced Options and Scheduling alone (default settings)

![](https://lh7-us.googleusercontent.com/RV14KO1ZcdvlTrBxSKM_RQE2C3TqIHWB9-4OYKIb9Kz3vNPbvpVusk3FWEndi9x0OXTWPKJ6jQnzJZ2LrVXF1ruQaz7dOqQx_ucmi6wNNvdnVvvpx8Lqh1fMlzdmiTu5Z8tjNYWmffC1zmi2OEoiMg)            ![](https://lh7-us.googleusercontent.com/p5oW-0cnZXITxF2hia9aAeusGNVIrpbVbkaQEioPVwLbsO1aNKJrz0sa5jovO9JyLTy1HJ3Ubh8t2JZEjRdaR_sJWz_dWglxva0EES3DkUGOrZWywJP3ggtGGX5mc8ZPZuOWWQqLdNIdEzDcyeiZ_g)

                                                                                                                 Upload File (Python)

- In CDE, go to Job Runs, click on the ID for the row of the Job that you just ran.  Explore the Logs, the run history, etc. if you’d like to see more details.  As long as the Job runs successfully, you can continue on.

**Query the updated data (in CDW HUE)**

- Execute the following in HUE for Impala VW

<!---->

    SELECT year, count(*) 
    FROM ${user_id}_airlines.flights
    GROUP BY year
    ORDER BY year desc;

- In the Results, you should see Year 2008 and a number of records along with the data for our previous years that have been loaded

![](https://lh7-us.googleusercontent.com/BtWM4F84zVfknTRs13awi-aUHAlejbztTFu6iPUKwxVxq4Kpk7ClmmPayWAvQhd99bRIDSlBEW49Usn8aPBNThEgMyQbbgMraL1fa3TUy7Vmzosmx4vV50Dgiv7tXz5GSnashEy3WKwE4W3illNFNw)

**\[OPTIONAL] Query Iceberg Table Format and Hive Table Format in same query (in CDW HUE)**

- Execute the following in HUE for Impala VW

  - Remember that we have already seen the following tables in Iceberg Table format

    - flights (via Create Table in Iceberg format)

    - planes (migrated via Alter Table utility)

    - airports (CTAS to Iceberg table)

  - So, let’s see about the Unique Tickets table.  We’ll see that this table is still in Hive Table Format - see SerDe Library = ParquetHiveSerDe (not the Iceberg SerDe).  So we have a Dashboard that is combining both table formats in a single Dashboard

<!---->

    --
    -- [optional] SINGLE QUERY USING ICEBERG & HIVE TABLE FORMAT
    --            Uses CDV Dashboard, could also just query in HUE
    -- DESCRIBE FORMATTED ${user_id}_airlines.flights;
    DESCRIBE FORMATTED ${user_id}_airlines.unique_tickets;

![](https://lh7-us.googleusercontent.com/Pi-XHBa9TNjDNj-IonSAQsDj5ALM_WL7FbRRH9eCeNgOYI9UP9oQtARSxN4xG9VzJWMlD1i2SbmiIkS4bbd7CK7oh9G_7h0f6DuKUWrQMpg5oeyDUV6imOcgk6Zh6W6LYkAoHJzJPbd2Oj-d0B2e_s8)

- Now let’s run a query that combines this Hive Table with an Iceberg Table

<!---->

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

- **VALUE**: combine Iceberg with Hive table formats, means you can convert to use Iceberg where it makes sense and also can migrate over time vs. having to migrate at the same time


# ACID feature - shown in CML<a id="acid-feature---shown-in-cml"></a>

In this section use CML to run though some of the Iceberg features using Spark again, by running some PySpark code.  Here we will primarily concentrate on taking advantage of the ACID capabilities of Iceberg.  Since we are using CML, this section also shows off the multi-function analytic feature of Iceberg.

**CML** 

- In CML, open the Workspace named (replace \<user-id> with your user id) **\<user-id>-iceberg-ml**

- Create a new Project

  - Project Name: **\<user-id>**-iceberg-project (replace \<user-id> with your user id)

  - Initial Setup: Template tab, select Python from the dropdown

  - Runtime Setup: Basic

    - Kernel: Python 3.7

- Left nav, select Files

  - Create New File (+ New; then select New File)

    - File Name: **iceberg\_acid.py**

    - Check Open in Editor

![](https://lh7-us.googleusercontent.com/lnjlBqVoG5zphcB6oPIXmscn2JpVMk1CH-4Bfgf1S_K7fbsOvaIPR6z_vmFTaX3hFhxMud6DqutlmD7RwXiMBLSsmWZf5xYEaY6ZoTGI-NJu1iYYR7VOTbyFnN_kawWAwU3zNKa7bHTy_jLoEvBHtg)

- Start New Session

  - Name: iceberg-acid-session

  - Editor: Workbench

  - Enable Spark, and select Spark 3.2.0 (Spark 3 is required for Iceberg functionality)

![](https://lh7-us.googleusercontent.com/iWCNbMuJUSVTqbOtv1rrEbKIdLkgWWyUN2t53K-pI-BEE43aIz5zUZOy3H3NAd5FOyI5jWPcn0g3JpY7lynfTaob-CgAx7O1crknInjMACNiv72_IzT4eqcjWzRmoVyyXo6QvGnPjT6e7CoaLdmtiQ)

- From the Connection Code Snippet find and select the tile with TYPE = “Spark Data Lake”

  - Click the ![](https://lh7-us.googleusercontent.com/OYaVVisklPMa81qK6DZd2hiVqq0Zd3LBMWRpxazhFDkZC8K6p3aI5ZCmvlAlmFISG4LLMO6OzPLfMsWE1S7IB5QjB_uLxTUx7DAL7n6CfjB94xh7ZPDGbvaqstTfEN4PQIRCPYWVX0y25-fAPZvLvw)button in the top right corner of the Code Snippet and click Close

![](https://lh7-us.googleusercontent.com/PLaFtf99qGs98h1YCv2v_1I3oiMk8EIIhmtit1tL7U6Aox8lR3NbUn_OqbVA4E21vdhbMRIukgGcY2k8qQKLi0YV0AoTLHUj-7ogg2oWR1rdnrnb9vSZJYsCX6kwNxSshITQsN4yKJnuZy54trE4AQ)

- When you are back in the Workbench, paste the code into the Editor

![](https://lh7-us.googleusercontent.com/F4-iOlYGvLPmK4lAaJ8UdyzmsfZioAlNim1zQrEQkhQhzOk9H1wQrAh4KcDOFYWkTup8d_Cw2xq7MPwjtXgpbBeddmpDv1nP3ElkTUDKE1ZoffXjKMIg2E_FPc21uR8iwCnLj4kxqPp24MLzdlyPnw)

- Copy paste the following code, replacing \<user-id> with your user id, into the Workbench Editor after the code you copied connecting to the Spark Connection (from above screen you would start on line 11).  To summarize what this code will do: 1) create an Iceberg table, 2) load data into Iceberg table, and 3) updates a value in a row (ACID MERGE) - changes “United Airlines Inc.” to “Adrenaline Airways”.  WOW!!! What a Ride!!!! 

<!---->

    ### Code to add
    # Replace <user-id> with your user id in the following code

    # Query Raw Data Table
    spark.sql("SELECT * FROM <user-id>_airlines_csv.airlines_csv limit 5").show()

    # Create Iceberg Table
    spark.sql("CREATE EXTERNAL TABLE <user-id>_airlines.airlines (code string, description string) USING ICEBERG  TBLPROPERTIES ('format-version' = '2')")

    # Load Data into Iceberg Table
    spark.sql("INSERT INTO <user-id>_airlines.airlines SELECT * FROM <user-id>_airlines_csv.airlines_csv")

    # Review Results to ensure record was updated
    spark.sql("SELECT * FROM <user-id>_airlines.airlines WHERE code ='UA'").show()

    # ICEBERG ACID - Change row for UA (United Airlines) to reflect new name of Adrenaline Airways 
    spark.sql('MERGE INTO <user-id>_airlines.airlines s USING (SELECT t.code, "Adrenaline Airways" AS description FROM <user-id>_airlines.airlines t WHERE t.code = "UA") source \
    ON s.code = source.code \
    WHEN MATCHED AND s.description <> source.description THEN UPDATE SET s.description = source.description \
              ')

    # Review Results to ensure record was updated
    spark.sql("select * from <user-id>_airlines.airlines where code ='UA'").show()

- Once you’ve pasted the code at the end of the connection click on Run > Run All

![](https://lh7-us.googleusercontent.com/LgPAPFFximIon18UU05kOiq3fXJSdCkqfbYpBdCpXj_sq3y1kQdvJZfIkFrkNxDX_xYLJxgAQw2aBYfUuspE34OcmePam3-HXL3-Rwlrb4D8jYzrJQrOieVDQuJyh8_SHIakqxgz3h5DCsXe9rYqnA)

-  The Session output will show the following

![](https://lh7-us.googleusercontent.com/KX06rUfJEbdSRZMzmdhQUFFhzHlIIXMFfY7Mn5yXKN-rsGXQt7miiq5cN5LYZYNgqxWLvJgm7eiXebbdN-5mw5PEbcHBKa4c2jztJa9mNpuL17eGZUUFohHykd8Ybu-tieY5Zr5mDliiFTxTT2Zp2Q)


# SDX Integration (Fine Grained Access Control)<a id="sdx-integration-fine-grained-access-control"></a>

In this section you will create a Ranger policy to apply a Masking Policy for the Planes Iceberg table for your user id.

**Query the Planes Table (in CDW HUE)**

- Execute the following in HUE for Impala VW

<!---->

    SELECT * FROM ${user_id}_airlines.planes;

- In results you see that the Tailnum column is in plain readable text, as shown

```
```

**Enable Ranger Policy on Planes Table**

- Open Ranger UI for the Environment you are using.

- Open Hadoop SQL

- Edit the Policy **\<user-id>**-iceberg-fgac (replace \<user-id> with your user id)

  - On the Policy details click on Disabled to Enable the policy

![](https://lh7-us.googleusercontent.com/vOpN1BLCQWwMj_74Q5vEDmCmqQAlxpWs7I__Hwvol8pHPY5EJcoNsch--B-R5Fn7DIh_AXhI75ufYllpPV5rf2mtywgEL3azVDoa4EmdxXvtPN3c5FV8BzTPowpUOOT9smqBAuctDBVAQ4Fr-KQjcQ)

- Click on Save button

**Query the Planes Table (in CDW HUE)**

- Execute the following in HUE for Impala VW

<!---->

    SELECT * FROM ${user_id}_airlines.planes;

- In results you see that the Tailnum column is now been HASHed

![](https://lh7-us.googleusercontent.com/N3BW3pmdyRVJJtBBpLgVi0B89BNc3IQeq7hom0wdwUhaNY0qwgzbYZ12y1xGWdw89Wmqj596F_KBsNSfQoTqI9kDItyxlJ71RYqloVTPrBzrIHk45FLRy4Sz83v6xzTq6pAg6AziSYGYUYnBsR0B_Q)


# \[OPTIONAL] Load Iceberg Table using Cloudera DataFlow \[UNDER CONSTRUCTION - WORK IN PROGRESS]<a id="optional-load-iceberg-table-using-cloudera-dataflow-under-construction---work-in-progress"></a>

This section shows how you can use Cloudera Data Flow (CDF) to stream data into an Iceberg table.  This example will append new weather data into the Weather tables.  There are 2 steps involved in this process: 1) Call API to get weather data & send data to Kafka; and 2) Use ReadyFlow “Kafka To Iceberg” to append the Kafka weather data to the Iceberg tables (this is performed 3 times for each weather table)

**\[NOTE:]** - Be careful when using this as to not treat this like a RTDM (Kudu) equivalent.  If there are frequent updates requirements with large volumes of data, then Iceberg may not be the best choice - you should consider using Kudu instead.

**CDF - Load Iceberg Table using PutIceberg Processor**

-

- Click on the Dashboard named “**A**”


# \[OPTIONAL] Iceberg Table Format Table(s) joined with Hive Table Format Table(s) using Data Viz<a id="optional-iceberg-table-format-tables-joined-with-hive-table-format-tables-using-data-viz"></a>

It is NOT a requirement to convert all tables to Iceberg Table format.  In fact, this can be something that you move to over time.

**DataViz - Explore the “Airline Dashboard” Dashboard**

- Click on VISUALS tab

- Click on the Dashboard named “**Airline Dashboard**”

![](https://lh7-us.googleusercontent.com/q4ase6qIc-PYHTK1iOLSz6XZ9vKl0X1nZjGhSjlZKPZpEbNjVY5lSruKhwSQXl0HJpV2y6BF0liYJ1-N0jOTb0HGsm6KaenPf6rVygwoubDWvNpYURxuqZeEjCJQgFwvGwxBnzw2gJxLe_4XUga_i1g)

- **VALUE**: combine Iceberg with Hive table formats, means you can convert to use Iceberg where it makes sense and also can migrate over time vs. having to migrate at the same time

  - This Dashboard uses the same **Iceberg** tables (flights, planes, airports) that you have been working on in the Airlines DW (database name “**\<user\_id>**\_airlines”) and combines this with an existing table in the same database, named “unique\_carrier” which is still in a Hive Table format.

* Let’s explore the Dataset to see the various tables that are part of this Data Model

  - Click on the DATA tab, you will now have a Dataset named “Airlines Lakehouse”

![](https://lh7-us.googleusercontent.com/o_nZFAh7xF_X12NyTmgNJ8qJ42wTQkIUsi8SCmXkuaR1e8Gqqiio76SbahJr5mWoF2YPOwu1s3Rdo-6MBcsmkkx51erL8NdR3GUSlwo-1anSzzHKUfHjsQr0kmvfMa4GKPtuwuYJVETeqj1icOK2zOg)

- Click on the “Airlines Lakehouse” to open the Dataset

  - On the left navigation menu select Data Model

  - Click on the ![](https://lh7-us.googleusercontent.com/WFeeS7lE1aKJNO6HscACLjAzAkmflmihH7-JB68WVX12vlNvHDJDxRsocS5M2nYEE6wmazWRCQrIeXxyMp-VYn2cvRyxif5ncR6BQ12t38Ck6uO_xarB0A83sAjnB6iEwaqmtRmOm0lm-TT8m90lZpY)button

    - Click on the “unique\_tickets” table, to find the source table

      - Database: \<user\_id>\_airlines

      - Table: unique\_tickets

    - Click on the “flights” table, to find the source table

      - Database: \<user\_id>\_airlines

      - Table: unique\_tickets

    - Instead of showing each table lets take a look at the table types

* Execute the following in HUE for Impala VW

  - Remember that we have already seen the following tables in Iceberg Table format

    - flights (via Create Table in Iceberg format)

    - planes (migrated via Alter Table utility)

    - airports (CTAS to Iceberg table)

  - So, let’s see about the Unique Tickets table.  We’ll see that this table is still in Hive Table Format - see SerDe Library = ParquetHiveSerDe (not the Iceberg SerDe).  So we have a Dashboard that is combining both table formats in a single Dashboard

<!---->

    --
    -- [optional] SINGLE QUERY USING ICEBERG & HIVE TABLE FORMAT
    --            Uses CDV Dashboard, could also just query in HUE
    -- DESCRIBE FORMATTED ${user_id}_airlines.flights;
    DESCRIBE FORMATTED ${user_id}_airlines.unique_tickets;

![](https://lh7-us.googleusercontent.com/Pi-XHBa9TNjDNj-IonSAQsDj5ALM_WL7FbRRH9eCeNgOYI9UP9oQtARSxN4xG9VzJWMlD1i2SbmiIkS4bbd7CK7oh9G_7h0f6DuKUWrQMpg5oeyDUV6imOcgk6Zh6W6LYkAoHJzJPbd2Oj-d0B2e_s8)


# \[OPTIONAL] Iceberg Table Format Table Maintenance Tasks <a id="optional-iceberg-table-format-table-maintenance-tasks"></a>

Perform necessary clean up of Iceberg tables.  The following steps will build off of each other - ie. the first section will load data into the flights table in the \<user-id>\_airlines\_maint database and the next sections will use this data load to perform the table maintenance tasks

**Manual Table Compaction & Manifest rewrite - ensures optimal performance**

- In   project “\<user-id>-iceberg-project”

- Create a new file, named “iceberg\_table\_maintenance.py”

  - Copy paste the following code

<!---->

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
    spark.sql("SELECT file_path, file_size_in_bytes FROM " + table_name + ".files").show(100)

    ## Also should clean up Manifest files
    spark.sql("SELECT path, length, added_snapshot_id, added_data_files_count FROM jing_airlines_maint.flights.manifests").show(100)

    # Table Maintenance to Manually Compact files to size of 500MB
    #    Compact flights table files into fewer files
    spark.sql("CALL spark_catalog.system.rewrite_manifests(table => 'jing_airlines_maint.flights')").show()

    # After Compaction, see that there is only 1 file which is about 11MB that needs to be read in the same directory
    spark.sql("SELECT path, length, added_snapshot_id, added_data_files_count FROM jing_airlines_maint.flights.manifests").show(100)

    # Now let's investigate the "Bad" record we saw previously

-

**Rollback - Bad data load, need to be able to restore to last known good state**

- In Hive VW

- Execute to see the bad data

<!---->

    -- Check data that was loaded - will see year=9999 (invalid)
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
    GROUP BY year
    ORDER BY year desc;

- Execute to see Snapshots

<!---->

    -- See Snapshot to determine when this data was loaded
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

- Execute this block to ensure this is the Snapshot containing the bad record

<!---->

    -- SELECT DATA USING TIMESTAMP FOR SNAPSHOT
    --      Using the previous Snapshot will see that this is where the records were loaded (Rollback needed)
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
      FOR SYSTEM_VERSION AS OF ${snapshot_id}
    GROUP BY year
    ORDER BY year desc;

- Execute to Rollback

<!---->

    -- ROLLBACK TO LAST KNOWN "GOOD" STATE FOR THE TABLE
    ALTER TABLE ${user_id}_airlines_maint.flights EXECUTE ROLLBACK(${snapshot_id});

- Query to see that bad record has been removed

<!---->

    -- Check data has been restored to last known "GOOD" state - data to year 2006
    SELECT year, count(*) 
    FROM ${user_id}_airlines_maint.flights
    GROUP BY year
    ORDER BY year desc;

-

**Snapshot Expiration - save storage space**

- In Hive VW

  - Execute to see snapshots

<!---->

    -- EXPIRE SNAPSHOT(S)
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

- Select one of the Snapshots

- Execute the following to remove all Snapshots up to this Snapshot

<!---->

    -- Expire Snapshots up to the specified timestamp
    --      BE CAREFUL: Once you run this you will not be able to Time Travel for any Snapshots that you Expire
    ALTER TABLE ${user_id}_airlines_maint.flights EXECUTE expire_snapshots('${create_ts}');

- Execute to see the snapshots have been removed

<!---->

    -- Ensure Snapshots have been removed
    SELECT * FROM ${user_id}_airlines_maint.flights.snapshots;

-

 __

4. # Appendix <a id="appendix"></a>


## Coming Soon (planned)<a id="coming-soon-planned"></a>

In no particular order:

- ~~\[COMPLETED 2022-12-07] \[adding 2022-11-28] Join Iceberg Table(s) with Hive Table(s) - show that you don’t have to migrate “everything” at one time (also part of CDV)~~

- Iceberg v2 Features (some added already to Runbook, a few are missing, but will be added over time)

  - Unified Analytics support

  - Materialized View

  - Upload in HUE directly to Iceberg Table Format

- \[WORK IN PROGRESS] \[adding 2023-03-30] CDF writing

  - Publish data to Kafka Topic(s)

  - From Kafka to Iceberg (leverage a Ready Flow in DFx)

    - Flow using PutIceberg Processor

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

- On-demand Compaction (PySpark Code) - run in CML

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

- ~~\[COMPLETED 2022-12-07]\[adding 2022-11-28] DataViz on top of Iceberg tables (already part of other demos)~~

  - ~~Join Iceberg Table Format with table(s) in Hive Table Format~~

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
