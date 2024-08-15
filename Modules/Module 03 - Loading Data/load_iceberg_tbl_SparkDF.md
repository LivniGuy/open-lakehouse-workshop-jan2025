# Load Data into Iceberg Table - Spark DataFrame

**Insert data feature**

- Execute the following lines in CDE Session (this may take a few minutes).  This will demostrate:
   - The power of multi-function analytics
   - How Iceberg can be written to from multiple services, if they write concurrently Iceberg has Consistency guarantees and has an innovative way it handles these situations

```
from pyspark.sql.functions import col

# Variables - replace <user_id> with your user id
user_id = "<user_id>"
csv_database_name = user_id + "_airlines_csv"
odl_database_name = user_id + "_airlines"

# INSERT 2 YEARS OF DATA INTO FLIGHTS TABLE
spark_df = spark.read.table(f"{csv_database_name}.flights_csv").where(col("year").isin([2005, 2006]))

# INSERT THE DATA INTO FLIGHTS TABLE
spark_df.writeTo(f"{odl_database_name}.flights").using("iceberg").append()

# CHECK RESULTS
spark.read.table(f"{odl_database_name}.flights").groupBy(col("year")).count().orderBy(col("year"), ascending=False).show()

```

