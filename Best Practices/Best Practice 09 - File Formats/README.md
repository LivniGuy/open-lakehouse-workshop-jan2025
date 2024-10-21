# Best Practice 09 - File Formats

## Overview

As a rule of thumb, choosing the right file format for a specific use case in a data lakehouse is a best practice because it significantly impacts the performance, storage efficiency, and overall usability of your data, allowing for optimal query speeds, cost-effective storage, and flexibility to handle different data types and access patterns depending on the analysis needs, especially when dealing with large volumes of diverse data within a single repository.

Many vendors are currently offering a data lakehouse implementation powered by Apache Iceberg. Choose a vendor that can help lower your TCO, can be integrated with new and legacy systems, and offers enterprise-level features on cloud and on-prem. Cloudera's current file format support is shared below, but since we are truly an open-source platform there are so many features you can take advantage of through a huge list of partners. 

| File Format | Description | Suitable for |
| ------------ | ----------- | ------------- | 
| Apache ORC | Columnar data format, with good compression ratio performance. | Writing-intensive tasks, like streaming and real-time ingestion. |
| Apache Parquet | Columnar data format achieves storage efficiencies and processing performance. | Reading-intensive tasks, like analytics and OLAP workloads. |
| Apache Avro | A popular format for row-based data serialization. | Low-latency streaming analytics use cases. | 

## IceTip

Understanding the use case for the Iceberg table to choose the proper file format will directly impact the transactions (updates and deletes) and the access performance.
