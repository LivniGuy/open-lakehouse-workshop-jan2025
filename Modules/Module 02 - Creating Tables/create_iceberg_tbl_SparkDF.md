# Create Iceberg Table Feature - Spark DataFrame

**Create table (partitioned) feature**

- Execute the following lines in a CDE Session

```
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# VARIABLES - change "<user_id>"
user_id = "<user_id>"
odl_database_name = user_id + "_airlines"


flightsSchema = StructType([
    StructField("month", IntegerType()),
    StructField("dayofmonth", IntegerType()),
    StructField("dayofweek", IntegerType()),
    StructField("deptime", IntegerType()),
    StructField("crsdeptime", IntegerType()),
    StructField("arrtime", IntegerType()),
    StructField("crsarrtime", IntegerType()),
    StructField("uniquecarrier", StringType()),
    StructField("flightnum", IntegerType()),
    StructField("tailnum", StringType()),
    StructField("actualelapsedtime", IntegerType()),
    StructField("crselapsedtime", IntegerType()),
    StructField("airtime", IntegerType()),
    StructField("arrdelay", IntegerType()),
    StructField("depdelay", IntegerType()),
    StructField("origin", StringType()),
    StructField("dest", StringType()),
    StructField("distance", IntegerType()),
    StructField("taxiin", IntegerType()),
    StructField("taxiout", IntegerType()),
    StructField("cancelled", IntegerType()),
    StructField("cancellationcode", StringType()),
    StructField("diverted", StringType()),
    StructField("carrierdelay", IntegerType()),
    StructField("weatherdelay", IntegerType()),
    StructField("nasdelay", IntegerType()),
    StructField("securitydelay", IntegerType()),
    StructField("lateaircraftdelay", IntegerType()),
    StructField("year", IntegerType())
])

# --- flights Iceberg Table partitioned PARQUET file format
spark_df = spark.createDataFrame([], flightsSchema)

spark_df.writeTo(f"{odl_database_name}.flights").using("iceberg").partitionedBy("year").tableProperty("format-version","2").createOrReplace()

# SHOW TABLE SCHEMA
spark_df.printSchema()


# CHECK PARTITION - list of columns (returns 'year')
spark_df = spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.flights")
spark_df.filter(spark_df.col_name.isin(['Part 0', 'Part 1', 'Part 2'])).show()

# CHECK TABLE FORMAT (returns 'iceberg')
(spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.flights").filter("col_name=='Provider'").collect()[0].data_type)

```

- In the output - look for the following (see highlighted fields)

![.png](../../images/.png)


Continue with 1 of the following
- Module 03 - Loading Data > load_iceberg_tbl_SQL.md
- Module 03 - Loading Data > ???
- Module 13 - load_new_data_to_flights_DF.md

