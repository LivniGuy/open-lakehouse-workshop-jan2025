# Load Data into Iceberg Table - SparkSQL

**Insert data feature**

- Execute the following lines in CDE Session (this may take a few minutes).  This will demostrate:
   - The power of multi-function analytics
   - How Iceberg can be written to from multiple services, if they write concurrently Iceberg has Consistency guarantees and has an innovative way it handles these situations

```
# Variables - replace <user_id> with your user id
user_id = "<user_id>"
csv_database_name = user_id + "_airlines_csv"
odl_database_name = user_id + "_airlines"

# INSERT 2 YEARS OF DATA INTO FLIGHTS TABLE
spark.sql(f"INSERT INTO {odl_database_name}.flights SELECT * FROM {csv_database_name}.flights_csv WHERE year IN (2005, 2006)").show()

# CHECK RESULTS
spark.sql(f"SELECT year, count(*) FROM {odl_database_name}.flights GROUP BY year ORDER BY year desc").show()

```

