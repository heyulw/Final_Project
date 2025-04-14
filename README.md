⚡ Real-time Data Processing and Monitoring Pipeline
A project demonstrating a real-time data pipeline that integrates Apache Kafka, Apache Spark Streaming, Airflow, and PostgreSQL to monitor and process over 1 million daily log records. Designed to track user behavior in an e-commerce setting with low latency and near-instant insights.

📅 Timeline
Feb 2025 – Mar 2025

🎯 Business Objective
Enable real-time tracking of user behavior to support faster decision-making in e-commerce operations. This helps:

Detect patterns and anomalies instantly

Reduce delays caused by traditional batch processing

Enhance personalization and operational efficiency

📌 Project Highlights
Ingested real-time logs from Kafka topics and processed them using PySpark Structured Streaming

Transformed data and loaded into PostgreSQL (5 dimension tables)

Built a monitoring Airflow DAG to validate new data every 2 minutes (~50,000 rows/run)

Optimized Spark micro-batch processing with a 10-second trigger interval, reducing latency by 30%

Containerized the entire stack with Docker

⚙️ Technologies Used
Apache Kafka – Real-time data ingestion

Apache Spark (PySpark) – Stream processing

PostgreSQL – Data storage (dimension tables)

Apache Airflow – Data validation & monitoring pipeline

Docker – Containerized environment

