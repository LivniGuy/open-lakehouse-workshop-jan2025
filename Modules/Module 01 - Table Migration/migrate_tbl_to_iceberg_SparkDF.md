# Migrate Existing Tables to Iceberg Tables - Spark DataFrame

**Migrate table feature (in-place)**

- Execute the following in a CDE Session, In the code, replace “<user\_id>”  with your user id 


```
# Variables - replace <user_id> with your user id
user_id = "<user_id>"
odl_database_name = user_id + "_airlines"

# CHECK TABLE FORMAT - before migration (returns 'hive')
(spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.planes").filter("col_name=='Provider'").collect()[0].data_type)

# MIGRATE planes Hive table format IN PLACE to Iceberg table format
spark.sql(f"CALL system.migrate('{odl_database_name}.planes')").show()

# CHECK TABLE FORMAT - after migration (returns 'iceberg')
(spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.planes").filter("col_name=='Provider'").collect()[0].data_type)

```

***** need to update the following still and add screen shot ******

- In the output - look for the following

  - Provider - indicates “ICEBERG” table format


![.png](../../images/.png)


**Create Table as Select (CTAS) - create new table**

- Execute the following lines in HUE for the Hive VW

```
# Variables - replace <user_id> with your user id
user_id = "<user_id>"
csv_database_name = user_id + "_airlines_csv"
odl_database_name = user_id + "_airlines"

# MIGRATE airports_csv Hive table format using CTAS to Iceberg table format
spark_df = spark.read.table(f"{csv_database_name}.airports_csv")

spark_df.write.format("iceberg").mode("overwrite").saveAsTable(f"{odl_database_name}.airports")

# CHECK TABLE FORMAT - after migration CTAS (returns 'iceberg')
(spark.sql(f"DESCRIBE FORMATTED {odl_database_name}.airports").filter("col_name=='Provider'").collect()[0].data_type)

```

- In the output - look for Provider to be set to “ICEBERG”

