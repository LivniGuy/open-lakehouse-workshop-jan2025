### UNDER CONSTRUCTION

# Branching

In this section we will work with Branches to create a branch to let us .

Branches are named references to snapshots with their own independent lifecycles.  You can use a <b>Branch</b> in your data engineering workflows and create experimetnal branches for testing & validating new jobs.  Other use cases inclue GDPR requirements and retaining important historical snapshots for auditing. 

**Query the Planes Table (in CDW HUE)**

- Execute the following in HUE for Hive VW

```
SELECT * FROM ${user_id}_airlines.flights;
```

- In results you see that the Tailnum column is in plain readable text, as shown


**Create Branch for Testing new Data Engineering Workflow**

- Create a branch by basing the branch on a snapshot ID.  You can also use a timestamp, or state of the table.

, a timestamp, or the state of your table. Using the SNAPSHOT RETENTION clause, you can create a branch that limits the number of snapshots of a table.
.

- 

- E

**Query the Branch for the Flights Table (in CDW HUE)**

- Execute the following in HUE for Hive VW

```
SELECT * 
  FROM ${user_id}_airlines.flights.${branch_name}
LIMIT 100;
```

- In results you see that 


