# Best Practice 09 - File Formats

| File Format | Description | Suitable for |
| ------------ | ----------- | ------------- | 
| Apache ORC | Columnar data format, with good compression ratio performance. | Writing-intensive tasks, like streaming and real-time ingestion. |
| Apache Parquet | Columnar data format achieves storage efficiencies and processing performance. | Reading-intensive tasks, like analytics and OLAP workloads. |
| Apache Avro | A popular format for row-based data serialization. | Low-latency streaming analytics use cases. | 

## IceTip

Understanding the use case for the Iceberg table to choose the proper file format will directly impact the transactions (updates and deletes) and the access performance.
