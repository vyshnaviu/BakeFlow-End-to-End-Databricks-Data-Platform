# **BakeFlow: Databricks Lakehouse Data Platform**

An end-to-end data pipeline on Databricks: CSV files go through Bronze, Silver and Gold layers under batch control, every run is audited for data quality, and the results feed a dashboard and a Genie space. Deployed to dev, qa and prod with Databricks Asset Bundles.

## **Data source**

The Databricks Bakehouse sample dataset: customers, franchises, suppliers, sales transactions and customer reviews, as CSV files kept in this repository.

## **How it works**

**File copy**: Azure Data Factory copies the CSV files into a Unity Catalog Volume.

**Batch control**: a new batch is registered at the start of each run (it fails if one is already open), and closed at the end.

**Bronze**: Auto Loader loads new files as they are — a raw copy of each file, plus batch_id and load time.

**Silver**: data is cleaned, and history is handled with AUTO CDC — customers keep latest values (SCD1), suppliers and franchises keep full history (SCD2), transactions and reviews append.

**Gold**: dimensions and facts are built as a star schema: dim_customer, dim_supplier, dim_franchise, fact_sales, fact_reviews.

**Data quality**: row counts, null keys and duplicate keys are checked per batch and saved in dq_checks.

**Consumption**: an AI/BI dashboard and a Genie space on the Gold tables.

Job order: check_and_open_batch → dlt_pipeline → dq_checks → close_batch.

## **Tech stack**

| Part | Technology |
| --- | --- |
| Source files | CSV (Databricks Bakehouse sample data), GitHub |
| File copy | Azure Data Factory |
| Landing storage | Unity Catalog Volume |
| Ingestion | Auto Loader |
| Transformation | Lakeflow Declarative Pipelines, PySpark, SQL |
| History handling | AUTO CDC (SCD Type 1 and 2) |
| Storage and governance | Delta Lake, Unity Catalog |
| Data quality | Pipeline expectations, audit notebook (dq_checks table) |
| Orchestration | Databricks Jobs |
| Consumption | AI/BI dashboard, Genie space |
| Deployment | Databricks Asset Bundles, GitHub |

## **Pipeline design**

| Layer | What it holds | How |
| --- | --- | --- |
| Batch control | One row per batch (id, times, status) | Open and close notebook |
| Bronze | Raw copy of each file, plus batch_id and load time | Auto Loader streaming tables |
| Silver | Customers: latest values (SCD1). Suppliers and franchises: full history (SCD2). Transactions and reviews: append | AUTO CDC and expectations |
| Gold | dim_customer, dim_supplier, dim_franchise, fact_sales, fact_reviews | Star schema |
| Audit | Source and target row counts, null and duplicate keys per batch | dq_checks table |

## **Deployment**

One code base is deployed to three targets (dev, qa, prod) with Databricks Asset Bundles, each with its own catalog, Git branch and name prefix. Catalog and schema names are bundle variables (variables.yml), set per target (targets.yml), and passed to the pipeline and notebooks — nothing is hard-coded. Changes move by pull request: dev to qa to main, and each environment is deployed and tested before the next.

| Target | Git branch | Catalog | Name prefix |
| --- | --- | --- | --- |
| dev | dev | dev catalog | dev- |
| qa | qa | qa catalog | qa- |
| prod | main | prod catalog | prod- |

<img width="700" height="300" alt="BakeFlow Lakehouse Architecture Infographic" src="https://github.com/user-attachments/assets/21cb3ca1-442c-464f-b682-68bc0b0198a9" />
