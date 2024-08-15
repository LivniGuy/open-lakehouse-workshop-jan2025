# Create Iceberg Table Feature - SparkSQL

**Create table (partitioned) feature**

- Execute the following lines in a CDE Session

```
# VARIABLES - change "<user_id>"
user_id = "<user_id>"
odl_database_name = user_id + "_airlines"

flights_cols_1 = "month int, dayofmonth int, dayofweek int, deptime int, crsdeptime int, arrtime int, crsarrtime int, "
flights_cols_2 = "uniquecarrier string, flightnum int, tailnum string, actualelapsedtime int, crselapsedtime int, "
flights_cols_3 = "airtime int, arrdelay int, depdelay int, origin string, dest string, distance int, taxiin int, taxiout int, "
flights_cols_4 = "cancelled int, cancellationcode string, diverted string, carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int"
flights_cols = flights_cols_1 + flights_cols_2 + flights_cols_3 + flights_cols_4

flights_part_cols = "year int"


# --- flights Iceberg Table partitioned PARQUET file format
tblprop = "TBLPROPERTIES('format-version'='2')"
partitioned_by = f"PARTITIONED BY ({flights_part_cols})"

spark.sql(f"drop table if exists {odl_database_name}.flights").show()

spark.sql(f"CREATE TABLE {odl_database_name}.flights ({flights_cols}) USING ICEBERG {partitioned_by} {tblprop}").show()

spark.sql(f"SHOW CREATE TABLE {odl_database_name}.flights").show()

# CHECK PARTITION - list of columns (returns 'year')
spark_df = spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.flights")
# ---- show all metadata
spark_df.show(100, truncate=False)
# ---- show partition details only
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

