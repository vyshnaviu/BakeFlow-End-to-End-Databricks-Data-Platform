# **BakeFlow: Databricks Lakehouse Data Platform**

An end-to-end data pipeline on Databricks: CSV files go through Bronze, Silver and Gold layers under batch control, every run is audited for data quality, and the results feed a dashboard and a Genie space. Deployed to dev, qa and prod with Databricks Asset Bundles.


## **How it works**
**Source**: Databricks Bakehouse sample data (CSV), copied into a Volume by Azure Data Factory.

**Batch control:** each run opens a batch and closes it at the end.

**Bronze, Silver, Gold**: Auto Loader ingestion, AUTO CDC for history (SCD1 for customers, SCD2 for suppliers and franchises), and a star schema in Gold.

**Data quality**: row counts, null keys and duplicate keys are checked per batch and saved in dq_checks.

**Consumption**: an AI/BI dashboard and a Genie space on the Gold tables.


## **Tech stack**
Databricks, Unity Catalog, Delta Lake, Auto Loader, Lakeflow Declarative Pipelines, PySpark, SQL, Azure Data Factory, Databricks Jobs, Databricks Asset Bundles, GitHub.


## **Deployment**
One code base, three targets (dev, qa, prod), each with its own catalog, Git branch and name prefix. Catalog and schema names are bundle variables, so nothing is hard-coded. Changes move by pull request: dev to qa to main, and each environment is deployed and tested before the next.



<img width="700" height="300" alt="BakeFlow Lakehouse Architecture Infographic" src="https://github.com/user-attachments/assets/21cb3ca1-442c-464f-b682-68bc0b0198a9" />
