# Google Cloud BigQuery

To open this repository in cloud shell, click below:

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2Fjustinjm%2Fgcp-bigquery)

## Overview

* Fully managed Relational DB  
* No-ops, Serverless  
* Analytics data warehouse  
* Accepts both batch and streaming inserts  
* SQL Syntax (and legacy SQL)  
* Multi-Region (US or EU) or Regional (ie. asia-northeast)  
* BigQuery is NOT recommended as a transactional database (use Cloud SQL or Spanner)  
* Built in ML 
* BI engine for in-memory/caching 
  
### Architecture  

* Columnar data store (each column is stored in its own storage point)  
* Storage and Compute are separate (compute = Dremel and storage = Colossus)  
* Based on Dremel  
* Slots are a unit of computational capacity required to execute SQL queries  
* BQ automatically calculates how many slots are required by each query  
* Per-account slot quota

## Pricing  

BigQuery pricing has two main components:

* [Analysis pricing](https://cloud.google.com/bigquery/pricing#analysis_pricing_models) is the cost to process queries, including SQL queries, user-defined functions, scripts, and certain data manipulation language (DML) and data definition language (DDL) statements that scan tables.
  * on-demand 5$ per TB 
  * slot rservations 
  * streaming ingest, storage API and BI engine 
* [Storage pricing](https://cloud.google.com/bigquery/pricing#storage) is the cost to store data that you load into BigQuery.
  * .02 per GB per mo Active
  * .01 per GB per mo Long-term

BigQuery charges for other operations, including using [BigQuery Omni](https://cloud.google.com/bigquery/pricing#bqomni), [BigQuery ML](https://cloud.google.com/bigquery/pricing#bqml), [BI Engine](https://cloud.google.com/bigquery/pricing#bi_engine_pricing), and streaming [reads](https://cloud.google.com/bigquery/pricing#data_extraction_pricing) and [writes](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing).

In addition, BigQuery has [free operations](https://cloud.google.com/bigquery/pricing#free) and a [free usage tier](https://cloud.google.com/bigquery/pricing#free-tier).

Each project that you create has a billing account attached to it. Any charges incurred by BigQuery jobs run in the project are billed to the attached billing account. BigQuery storage charges are also billed to the attached billing account. 

## IAM

**[IAM (Security)](https://cloud.google.com/bigquery/docs/access-control)**  

* Project-level access control  
* Dataset-level access control  
* **NOT** Table-level access control  
* Public Datasets (“all authenticated users” ROLE)  
* Primitive Roles: Owner, Editor, and Viewer  
* Predefined Roles:  
    * `roles/bigquery.admin` (full access)
    * `roles/bigquery.dataOwner` (Read, update, delete dataset. Read the dataset metadata and list tables in dataset)  
    * `roles/bigquery.dataEditor` (Read the dataset metadata and list tables in the dataset.)
    * `roles/bigquery.metadataViewer`
    * `roles/bigquery.dataViewer`
    * `roles/bigquery.jobUser` (Permissions to run jobs, queries within project)  
    * `roles/bigquery.user` (Run jobs, queries, within the project. Most individuals, data scientists)  
* When applying access to a dataset, you can grant access to:  
  * User by e-mail - Gives an individual Google account access to the dataset  
  * Group by e-mail - Gives members of a Google group access to the dataset  
  * Domain - Gives users and groups in Google domain access to the dataset  
  * All Authenticated Users - Makes the dataset public to Google Acct holders  
  * Project Owners - Gives all project owners access to the dataset  
  * Project Viewers - Gives all project viewers access to the dataset  
  * Project Editors - Gives all project editors access to the dataset  
  * Authorized View - Gives a view access to the dataset

### DEMO - Console Overview 

[console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)  

* Analysis
  * SQL Workspace - datasets, tables, saved queries,
  * Data transfers 
  * scheduled Queries 
  * Analytics Hub 
  * Dataform
* Migration
* Admin
  * monitoring

## Data sharing with Analytics Hub 

[Introduction to Analytics Hub](https://cloud.google.com/bigquery/docs/analytics-hub-introduction)

* Exchange valuable data and analytics assets securely and efficiently
* a shared datasets as a contract between data publishers and subscribers. As a data publisher, you define the collection of tables and views that you want to deliver to your users.
* On the subscriber side, when you subscribe to a shared dataset, you get a corresponding linked dataset inside of your project. For the subscriber It looks like any other dataset in your project except that it is read-only. You can join it with your data for shared insights. When you query a linked dataset, you pay for the BigQuery compute in your project while the publisher pays for the storage of the data.

## Monitoring & Audit Logging  

2 main ways to manage BQ and monitor cost 1) reactive and 2) proactive

### Reactive  

* **GCP billing alerts** - alert me when spend of BQ reaches threshold per month (cloud billing <https://cloud.google.com/billing/docs/how-to/budgets> )
  * Applies to a billing account or a set of projects and products.
  * Does not stop resource usage when budget amount is reached
  * Can generate spending alerts (email or Pub/Sub)
* **Cloud Monitoring** - custom monitoring dashboards (limited)
* **BQ Admin panel** - simple view of slow usage, job concurrency, failed jobs, etc (no alerting)
* **Information Schema** - real-time metadata, The `INFORMATION_SCHEMA` metadata tables contain relevant, granular information about jobs, reservations, capacity commitments, and assignments. Using the data from these tables, users can create custom dashboards to report on the metrics they are interested in in ways that inform their decision making. [Monitoring BigQuery reservations and slot utilization with INFORMATION\_SCHEMA | Google Cloud Blog](https://cloud.google.com/blog/topics/developers-practitioners/monitoring-bigquery-reservations-and-slot-utilization-information_schema)
* **BQ Audit Logs** - show me users / applications running expensive queries [BigQuery audit logs overview  |  Google Cloud](https://cloud.google.com/bigquery/docs/reference/auditlogs) See more on analyzing exported BQ audit logss in this blog post: 
  * [BigQuery Audit Logs pipelines & Analysis](https://cloud.google.com/blog/products/data-analytics/bigquery-audit-logs-pipelines-analysis)  
  * [sample code repo](https://github.com/GoogleCloudPlatform/bigquery-utils/tree/master/views/audit)

### Proactive  

* IAM - restrict access to resources by user
* quotas/custom cost controls - set a quota to stop BQ [Create custom cost controls  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/custom-quotas)
  * For certain products and services. May limit number of concurrent resources, or number of requests per period
* custom cost controls + automated shutdown - <https://cloud.google.com/billing/docs/how-to/notify>

can also build your own:

* blog post: [Snitching On Expensive Google BigQuery Queries | by Vadim Solovey](https://www.doit.com/snitching-on-expensive-google-bigquery-queries/)
* code: [doitintl/bq-snitch-app (GitHub)](https://github.com/doitintl/bq-snitch-app)

### DEMO - Cloud Monitoring

### DEMO - Cloud Logging

### DEMO - Information schema query

### DEMO - Audit Logs

## Optimizing cost

* Less work = faster query = less cost  
  * avoid `SELECT *`
  * filter query early, use query estimated 
  * avoid [SQL antipatterns](https://cloud.google.com/bigquery/docs/best-practices-performance-patterns)
* BigQuery charges for:  
  * Storage  
  * Queries  
  * Streaming Inserts  
* **Use Caching**  
  * BigQuery does not charge for cached queries  
  * By default “use cached results” is checked  
  * Caching is per user only!! (cached results are not shared)  
* Partition and Clustering 
  * **Partititoned table** - A partitioned table is divided into segments, called partitions, that make it easier to manage and query your data. By dividing a large table into smaller partitions, you can improve query performance and control costs by reducing the number of bytes read by a query. You partition tables by specifying a partition column which is used to segment the table. [Introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioning)
  * **Clustered table** Clustered tables in BigQuery are tables that have a user-defined column sort order using clustered columns. Clustered tables can improve query performance and reduce query costs. Clustering can improve the performance of certain types of queries, such as those using filter clauses and queries aggregating data. [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables#when_to_use_clustering)
    * **Note: Clustering does not provide strict cost guarantees before running the query.**
* Creating Custom Cost Controls:  
  * Manage costs by requesting a custom quota that specifies a limit on the amount of query data processed per day  
  * Creating a custom quota on query data allows you to control costs at the project-level or at the user-level  
  * Project-level custom quotas limit the aggregate usage of all users in that project.  
  * User-level custom quotas are separately applied to each user or service account within a project.

### DEMO - partitioning and clustering

codelab: <https://codelabs.developers.google.com/codelabs/gcp-bq-partitioning-and-clustering#0>
docs:  
  * partitioning - <https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioning>
  * clustering: <https://cloud.google.com/bigquery/docs/clustered-tables#when_to_use_clustering>


## Best Practices

* Avoid using Select * (query only the columns you need)  
* LIMIT does NOT affect cost  
* Denormalize data when possible  
* Filter early and big with WHERE clause  
* Do bigger joins first (and filter pre-join when possible)  
* Partition data by date (1) ingest time or (2) partition by date column  
* Low cardinality GROUP BYs are faster  
* Sample data using preview options (Don't run queries to explore or preview table data)  
* Price your queries before running them (use dryRun flag)  
* Limit query costs by restricting the number of bytes billed  
* Materialize query results in stages  
* You can use nested and repeated fields to maintain relationships  
* Use streaming inserts with caution (extra cost associated with this)  
* Avoid excessive wildcard tables (be as specific as possible with wildcard prefix)  
* Avoid unbalanced joins  
* Avoid skew (if possible, filter and/or use approximate functions)  
* Avoid Cross joins (Cartesian product)  
* Avoid single row operations (Batch your updates and inserts)  
* Optimizing Storage (Use the expiration settings, lifecycle archival policies)

## References 

BigQuery  

* [What is BigQuery? | Google Cloud](https://cloud.google.com/bigquery/docs/introduction)
* [Pricing | BigQuery: Cloud Data Warehouse](https://cloud.google.com/bigquery/pricing)
* [Organizing BigQuery resources  |  Google Cloud](https://cloud.google.com/bigquery/docs/resource-hierarchy)

BigQuery ML  

* [What is BigQuery ML?  |  Google Cloud](https://cloud.google.com/bigquery-ml/docs/introduction)

Power BI 

* once BI Engine Reservation is configured on a project, queries will become eligible for acceleration by BI Engine regardless of API/BI tool used.


## Acknowledgements

[zaratsian/gcp\_bigquery: Google Cloud BigQuery - Scripts and References](https://github.com/zaratsian/gcp_bigquery)


## Open in Cloud Shell 



Reference: [Open in Cloud Shell  |  Google Cloud](https://cloud.google.com/shell/docs/open-in-cloud-shell)
