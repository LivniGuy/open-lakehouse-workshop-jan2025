#  Best Practice 05 - Storage Optimization

## IceTip
Understand the data retention requirements according to the use case, project, business needs, or local regulations. Then automate jobs to expire the snapshots that are no longer needed periodically.

```sql
ALTER TABLE test_table EXECUTE expire_snapshots('2021-12-09 05:39:18);
```

