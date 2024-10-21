# REST Catalog - Create Role and Credential

The Iceberg REST Catalog enables users to interact with Iceberg tables using a standardized REST API, making the catalog more accessible, scalable, and flexible for cloud-native and distributed architectures.

In this module you will learn how to create the Cloudera Role and Credential needed for 3rd party clients to connect to the Cloudera Iceberg REST Catalog.                                       
**Get Credential from Knox**

- Execute the following

```
sudo python3 knoxshare.py --username <CDP-user> --password <your-workload-pw> --knox-url https://<datalake-master-node>:8443/<CDP-environment>/cdp-share-management/ --contact <user-email> --comment "<your-comments>" --role <role-to-create> --showjwt=True
```

- Replace the following
   - \<CDP-user> - user used for creating
   - \<your-workload-pw>
   - \<datalake-master-node>
   - \<CDP-environment>
   - \<user-email>
   - \<your-comments>
   - \<role-to-create>

   - Example:
      sudo python3 knoxshare.py --username sso_user --password CDPisSuperCoo1 --knox-url https://demo-aws-dl-master0.jing-aws.a465-9q4k.cloudera.site:8443/demo-aws-dl/cdp-share-management/ --contact sso_user@cloudera.com --comment "Data Share Role" --role dsRole --showjwt=True

       -  When completed this command will create a new role "dsRole" in Ranger that can be used to define security policies


![.png](../../images/.png)


                                      
**Define Security Policy in Ranger**


- Setup the following policy in Ranger



                                      