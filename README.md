# E-Commerce Lakehouse Platform

A production-inspired end-to-end Data Engineering project that simulates a modern e-commerce analytics ecosystem. The platform ingests batch, API, and real-time streaming data from multiple sources, processes it through a scalable Lakehouse architecture, and delivers analytics-ready datasets for business intelligence and reporting.

The project leverages Kafka for event streaming, PySpark for distributed processing, Delta Lake for reliable storage, Airflow for orchestration, dbt for analytical transformations, Great Expectations for data quality, and Power BI for visualization.

## Key Features

* Batch ingestion of customer, product, and seller datasets
* Real-time ingestion of orders, payments, and clickstream events through Kafka
* API integration for exchange rates and product ratings
* Medallion Architecture (Bronze, Silver, Gold)
* Schema evolution and late-arriving data handling
* SCD Type 2 dimensions and star-schema modeling
* Data quality validation and monitoring
* Airflow-based workflow orchestration
* Business-ready sales, customer, and product marts
* Interactive dashboards and KPI reporting

## Technology Stack

Kafka • PySpark • Delta Lake • Airflow • dbt • Great Expectations • Power BI • GitHub Actions • Docker • Terraform

This project demonstrates real-world Data Engineering practices including scalable pipeline design, data warehousing, streaming analytics, observability, and production-ready architecture patterns.

