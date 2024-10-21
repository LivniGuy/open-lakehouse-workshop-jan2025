#  Best Practice 04 - Data Compaction

## Overview 
Compaction reduces the number of small files by merging them into larger files, thus improving query performance and reducing metadata overhead. Data Compaction is a best practice for data lakehouses for many reasons, but here we list a few. 

- **Efficient Storage Utilization** By compacting small files, data lakehouses can optimize storage space. This reduces the number of metadata entries, which can help lower storage costs and improve efficiency.
- **Reduced Metadata Overhead** Iceberg manages metadata for each file. When there are many small files, the metadata overhead can become significant. Compaction helps minimize this overhead, leading to better performance in metadata operations.
- **Facilitated Data Cleanup** Compaction can be part of data lifecycle management processes, helping to identify and remove outdated or unnecessary data, thus improving overall data quality.
- **Compatibility with Evolving Workloads** As data access patterns and workloads evolve, regular compaction ensures that the data lakehouse can continue to perform efficiently under varying loads.

It must be mentioned that Data Compaction is not new, this best practice has been used since the early 60's. Way back from flat files and record structures to the latest and greatest Apache Iceberg. Over the past decades, data compaction has evolved from database management systems (e.g., 80s), data warehousing (e.g., 90s), columnar storage formats (e.g., 2000s), big data technologies (e.g., 2010s), data lake paradigms (e.g., 2015s onward), and it will continue to evolve. Having a good data compaction strategy will be essential for many reasons and some stated above. 

![best_practice_4a.jpg](../../images/best_practice_4a.jpg)

## 1. Strategy

- binPack
- order
- order

## 2. Filters

Run compaction on specific parts of the data

## 3. Options

- partial-progress-enabled
- rewrite-job order
- target-file-size-bytes,
- and so much more...

## 4. Automation

CDE => Spark job orchestrated by Airflow

## IceTip

Accordingly to the row-level mutations strategy mutation defined for the Iceberg table, data compactions should be run more or less frequently. 
As COW already creates new files with the changes, this strategy will eventually require less frequent compactions. 
On the other hand, MOR creates delta files to record the changes, and to keep good query performance, compactions will create new files, therefore, better access performance.
