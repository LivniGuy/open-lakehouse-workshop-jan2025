# Load Data into Iceberg Table

**Insert data feature**

- Execute the following lines in CDE Session (this may take a few minutes).  This will demostrate:
   - The power of multi-function analytics
   - How Iceberg can be written to from multiple services, if they write concurrently Iceberg has Consistency guarantees and has an innovative way it handles these situations

```
spark.sql("INSERT INTO jing_airlines.flights SELECT * FROM jing_airlines_csv.flights_csv WHERE year = 2008").show()

spark.sql("SELECT year, count(*) FROM jing_airlines.flights GROUP BY year ORDER BY year desc").show()
```

