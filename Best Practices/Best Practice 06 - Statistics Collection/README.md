# Best Practice 06 - Staticts Collection

## Control Columns' Metrics 

## IceTip
When creating an Iceberg table, you must understand the most common attributes end-users will use to filter out the table. This will indicate which columns are worth computing statistics and the appropriate metric mode. This task can be performed after table creation if the access pattern changes.

| Metric Mode | Description |
| ------------ | ----------- |
| none | Does not compute any column statistics |
| counts | Only compute counts for the column |
| truncate(length) | Similar to counts, but truncate column value to a certain number of characters |
| full | Compute counts, lower and upper bounds with the column's full value |

