# Best Practice 06 - Statisticts Collection

## Overview
Statistics collection is a best practice because it generates and stores statistics as part of the metadata for a table, ensuring that the statistics are always up to date. This eliminates the need to manually update the statistics. The statistics collected include record count, file size, value counts, null value counts, and lower and upper bounds for each column.

Data statistics is an integral part of data governance, data quality management, and performance monitoring. Most tools if not all need this best practice for all their advanced analytics and frameworks to enable real-time statistic collections and analysis which will provide insights into data usage, trends, and system performance.

## Control Columns' Metrics 

## IceTip
When creating an Iceberg table, you must understand the most common attributes end-users will use to filter out the table. This will indicate which columns are worth computing statistics and the appropriate metric mode. This task can be performed after table creation if the access pattern changes.

| Metric Mode | Description |
| ------------ | ----------- |
| none | Does not compute any column statistics |
| counts | Only compute counts for the column |
| truncate(length) | Similar to counts, but truncate column value to a certain number of characters |
| full | Compute counts, lower and upper bounds with the column's full value |

