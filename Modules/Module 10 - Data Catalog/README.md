# Module 10 - Data Catalog

## Overview

In this module, we will explore key operations within the **Cloudera Data Catalog**, focusing on managing, discovering, and viewing metadata for Iceberg tables in an Open Data Lakehouse on the Cloudera Data Platform (CDP). The Data Catalog provides centralized control over Iceberg table metadata and offers powerful tools for data discovery and governance.

### Key Operations in the Data Catalog

1. **Managing Datasets**: Learn how to create, edit, and delete asset collections in the Data Catalog to maintain organized and governed datasets.
2. **Searching for Data Assets**: Use the Data Catalog's search capabilities to discover Iceberg tables, view their metadata, and track their evolution.
3. **Viewing Data Asset Details**: Explore detailed metadata about Iceberg tables, including schema information, lineage, and governance policies.

### Why Data Catalog is Important for Iceberg

- **Centralized Metadata Management**: Iceberg tables' metadata is automatically available within the Data Catalog, providing a single source of truth.
- **Efficient Data Discovery**: Quickly locate Iceberg tables and explore their structure, history, and partitioning details.
- **Comprehensive Governance**: Use Ranger integration to apply access controls, ensuring data governance compliance across your organization.

### Methods Covered in This Module

#### 1. Managing Datasets in the Data Catalog

You will learn how to manage Iceberg table metadata by creating, editing, and deleting datasets.

#### 2. Searching for Iceberg Tables

This section focuses on how to search for Iceberg tables in the Data Catalog, making it easy to discover tables and view their metadata.

#### 3. Viewing Data Asset Details

You will also learn how to explore the detailed metadata of Iceberg tables, including schema versions, table history, and data lineage.

### Key Takeaways

- The Data Catalog enables centralized metadata management, data discovery, and governance for Iceberg tables in CDP.
- Managing datasets, searching for assets, and viewing asset details are core operations that streamline Iceberg table management.
- The integration with Ranger provides governance and security controls over Iceberg tables.

> **Note**: Replace `${prefix}` with your user ID throughout the process.

## Submodules

`01` [Managing Datasets in the Data Catalog](DataCatalog_ManagingDatasets.md)

`02` [Searching Iceberg Tables](DataCatalog_Search.md)

`03` [Viewing Iceberg Table Metadata](DataCatalog_ViewingMetadata.md)
