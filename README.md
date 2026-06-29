# Retail Sales Data Engineering Pipeline

An end-to-end cloud-native Data Engineering project implementing the **Medallion Architecture (Bronze, Silver, Gold)** using AWS, Apache Airflow, AWS Glue, PySpark, Delta Lake, Databricks, and Amazon Athena.

---

# Project Architecture

![Architecture]<img width="2116" height="1010" alt="image" src="https://github.com/user-attachments/assets/f20f01f8-e059-4549-bb7d-d30d4dabbee4" />


---

# Tech Stack

| Category | Technology |
|-----------|------------|
| Programming | Python |
| Workflow Orchestration | Apache Airflow |
| Cloud Platform | AWS |
| Data Lake | Amazon S3 |
| ETL | AWS Glue (PySpark) |
| Metadata | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Lakehouse | Delta Lake |
| Analytics | Databricks |
| Security | AWS IAM |

---

# Project Workflow

1. Extract retail datasets from GitHub.
2. Store raw CSV files in the Bronze layer (Amazon S3).
3. Transform raw data using AWS Glue PySpark jobs.
4. Store cleaned Delta tables in the Silver layer.
5. Run AWS Glue Crawlers to update the Glue Data Catalog.
6. Query Silver tables using Amazon Athena.
7. Trigger Databricks notebook from Apache Airflow.
8. Create Gold-layer analytical tables.
9. Store Gold Delta tables for business reporting.

---

# Medallion Architecture

## Bronze Layer

- Raw CSV files
- Stored in Amazon S3
- Immutable source data

Example:

```
bronze/
    customers.csv
    orders.csv
    order_items.csv
    products.csv
```

---

## Silver Layer

- Data cleansing
- Null handling
- Data type conversion
- Delta Lake format
- Partitioned by load date

Example:

```
silver/
    customers/
    orders/
    order_items/
    products/
```

---

## Gold Layer

Business-ready analytical tables

- fact_sales
- sales_summary

---

# Airflow DAG

![Airflow DAG]<img width="486" height="255" alt="image" src="https://github.com/user-attachments/assets/f1838ddc-d5ea-47b0-8553-45a3206e08fd" />
![Airflow DAG - Successful run]<img width="910" height="413" alt="image" src="https://github.com/user-attachments/assets/91c98968-b3d8-4fa1-b93a-226ca6741a06" />


The Airflow DAG orchestrates:

- Bronze ingestion
- AWS Glue ETL
- Parallel Glue Crawlers
- Databricks Gold Job

---

# AWS Glue Crawlers

![Glue Crawlers]<img width="746" height="301" alt="image" src="https://github.com/user-attachments/assets/ce26253c-5a1b-4aa7-bb22-f12653278065" />


Automatically updates the Glue Data Catalog after every Silver load.

---

# Databricks Gold Layer

![Databricks Job]<img width="443" height="353" alt="image" src="https://github.com/user-attachments/assets/2428d9ab-a62f-4a1d-9a9f-11784302f892" />


Creates business-ready Gold tables from Silver Delta tables.

---

# Amazon Athena

![Athena]<img width="957" height="358" alt="image" src="https://github.com/user-attachments/assets/c9794a05-98cb-4436-a98c-35919a5deb2e" />

Queries Silver data using the Glue Data Catalog.

---

# Project Features

- End-to-end ETL Pipeline
- Medallion Architecture
- Delta Lake
- Apache Airflow Orchestration
- AWS Glue ETL
- Parallel Glue Crawlers
- Databricks Integration
- Athena Analytics
- Incremental Date-based Processing

---

# Repository Structure

```
Retail-Sales-Data-Engineering/
│
├── dags/
├── utils/
├── notebooks/
├── images/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Future Improvements

- CI/CD with GitHub Actions
- Unit Testing
- Data Quality Validation
- Monitoring & Alerting
- Power BI Dashboard

---

# Author

Hariharan Thangarasu

AWS Certified Data Engineer – Associate
