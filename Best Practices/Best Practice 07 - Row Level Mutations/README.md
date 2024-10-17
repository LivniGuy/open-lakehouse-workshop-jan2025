# Best Practice 07 - Row Level Mutations

| --- | Change actions | Speed |
| --- | --- | --- | --- | --- | --- |
| Mutation Strategy | DELETIONS | UPDATES | Read | Write | Recommendations |
| COW | The entire file is rewritten excluding the deleted rows. | The whole file is rewritten with the updated rows. | Fastest | Slowest | Recommended where changes to data are low frequency and reads performance is significantly more important. | 
