# E-Commerce Analytics Platform

An end-to-end data engineering project that simulates a real-world e-commerce analytics environment using Apache Airflow, PostgreSQL, Docker, Python, SQL, and Apache Superset.

The project automates data ingestion, validation, transformation, and reporting processes to provide business-ready analytics datasets from raw operational data.

---

## Project Objectives

The goal of this project is to demonstrate practical data engineering skills by building a complete analytics pipeline that:

- Ingests raw e-commerce data from multiple sources
- Loads data into a PostgreSQL data warehouse
- Performs automated data quality checks
- Builds analytical data marts
- Orchestrates workflows using Apache Airflow
- Provides a reporting layer for BI dashboards

---

## Business Scenario

E-commerce companies generate large amounts of transactional and behavioral data every day.

Business stakeholders need reliable reporting to answer questions such as:

- How much revenue was generated today?
- Which products perform best?
- Which traffic sources drive the most engagement?
- What customer segments generate the highest value?
- How successful are different payment methods?

This project creates a centralized analytics platform to answer those questions automatically.

---

## Technology Stack

| Category | Technology |
|-----------|------------|
| Workflow Orchestration | Apache Airflow |
| Database | PostgreSQL |
| Programming | Python |
| Data Processing | Pandas |
| Query Language | SQL |
| Containerization | Docker |
| Data Visualization | Apache Superset |
| Version Control | Git & GitHub |

---

## Project Architecture

```text
CSV Files
│
├── customers.csv
├── products.csv
├── transactions.csv
└── click_stream.csv
        │
        ▼
Apache Airflow
        │
        ▼
PostgreSQL Raw Layer
        │
        ▼
Data Quality Checks
        │
        ▼
Business Intelligence Layer
        │
        ▼
Apache Superset Dashboards
```

---

## Dataset

This project uses the publicly available **E-commerce App Transactional Dataset** published on Kaggle by Aditya Bagus Pratama.

Dataset source:

https://www.kaggle.com/datasets/bytadit/transactional-ecommerce

The dataset contains four CSV files used throughout the pipeline:

| Dataset | Description |
|----------|-------------|
| customers.csv | Customer demographic and device information |
| products.csv | Product catalog and product metadata |
| transactions.csv | Customer purchase transactions and payment details |
| click_stream.csv | User behavior and application activity events |

The original dataset contains approximately 583 MB of data and includes transactional, behavioral, and product-level information suitable for building data warehouse and business intelligence solutions. :contentReference[oaicite:0]{index=0}

Due to file size limitations, the original raw datasets are not included in this repository. To run the project locally, download the dataset from Kaggle and place the files into the appropriate local directories:

```text
dataset/
org_dataset/

---

## Data Warehouse Structure

### Raw Layer

Stores source data with minimal transformation.

```text
raw.customers
raw.products
raw.transactions
raw.clicks
```

### BI Layer

Stores aggregated reporting tables.

```text
bi.daily_sales_mart
bi.daily_product_performance
bi.daily_customer_metrics
bi.daily_traffic_source_performance
```

---

## Airflow Pipeline

The DAG performs the following steps:

### 1. Source File Validation

Checks that all required source files exist before processing begins.

### 2. Database Initialization

Creates required schemas and tables if they do not already exist.

### 3. Data Loading

Loads source CSV files into PostgreSQL raw tables.

### 4. Data Quality Validation

Performs validation checks including:

- Required table existence
- Previous-day data availability
- Transaction availability for processing date
- Duplicate detection

### 5. Data Mart Generation

Builds analytical reporting tables.

### 6. Pipeline Completion

Marks successful completion of the workflow.

---

## Data Marts

### Daily Sales Mart

Provides daily sales performance metrics.

Metrics include:

- Total revenue
- Number of payments
- Successful payments
- Failed payments
- Average payment value

---

### Daily Product Performance

Provides product-level analytics.

Metrics include:

- Quantity sold
- Revenue by product
- Category performance

---

### Daily Customer Metrics

Provides customer segmentation analytics.

Metrics include:

- Total payments
- Successful payments
- Failed payments
- Total spend
- Average payment value
- Device type
- Gender

---

### Daily Traffic Source Performance

Provides traffic acquisition analytics.

Metrics include:

- Sessions
- Events
- Traffic source activity
- User engagement metrics

---

## Data Quality Framework

Implemented validation checks:

- Source file availability validation
- Database object validation
- Incremental data validation
- Duplicate record detection

These checks help ensure reliability and consistency of reporting datasets.

---

## Repository Structure

```text
ecommerce-analytics-with-airflow/
│
├── dags/
│   └── ecommerce_analysis.py
│
├── functions/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── validations.py
│
├── sql_scripts/
│   ├── create_schemas.sql
│   ├── create_tables.sql
│   └── marts/
│
├── dataset/
│
├── org_dataset/
│
├── config/
│
├── logs/
│
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Running the Project

### Clone Repository

```bash
git clone https://github.com/aziz-usenov1/ecommerce-analytics-with-airflow.git
cd ecommerce-analytics-with-airflow
```

### Start Services

```bash
docker compose up -d
```

### Airflow UI

```text
http://localhost:8080
```

### Superset UI

```text
http://localhost:8088
```

---

## Backfill Example

Run the DAG for an entire historical period:

```bash
docker compose exec airflow airflow dags backfill ecommerce_analysis \
--start-date 2022-06-01 \
--end-date 2022-06-30
```

---

## Skills Demonstrated

This project demonstrates:

- Data Engineering
- ETL Development
- Workflow Orchestration
- SQL Data Modeling
- PostgreSQL Development
- Python Data Processing
- Docker Containerization
- Data Quality Framework Design
- Business Intelligence Data Preparation

---

## Future Improvements

Planned enhancements:

- Incremental loading strategy
- Slowly Changing Dimensions (SCD)
- Star Schema implementation
- CI/CD with GitHub Actions
- dbt integration
- Automated data quality reporting
- Cloud deployment (AWS/GCP/Azure)

---

## Author

**Azizbek Ussenov**

Data Engineer | BI Developer

GitHub: https://github.com/aziz-usenov1

---

## Note

The original datasets are not included in this repository due to file size limitations.

To run the project locally, place the required datasets into:

```text
dataset/
org_dataset/
```

The repository focuses on demonstrating the architecture, orchestration, transformations, and reporting workflow rather than distributing raw source data.

---

## Copyright

© 2026 Azizbek Ussenov. All rights reserved.

This repository is provided for portfolio and educational review purposes only. No permission is granted to copy, modify, distribute, or use this code without explicit written permission from the author.