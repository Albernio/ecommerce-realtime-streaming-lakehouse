# 🛒 E-Commerce Real-Time Streaming Pipeline (Medallion Architecture)

![Data Engineering](https://img.shields.io/badge/Role-Data%20Engineer-blue)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze--Silver--Gold)-gold)
![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka%20%7C%20PySpark-orange)
![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-green)
![Cloud](https://img.shields.io/badge/Cloud-AWS%20(S3%20%7C%20Athena)-yellow)
![DevOps](https://img.shields.io/badge/DevOps-Docker%20%7C%20Git%20%7C%20CI%2FCD-black)

## 📌 Project Overview

This repository contains an end-to-end, real-time data engineering pipeline built to process e-commerce event streams at scale. The project demonstrates a production-grade **Medallion Data Lakehouse Architecture (Bronze, Silver, Gold)** using **Apache Kafka**, **PySpark Structured Streaming**, **Apache Airflow**, and **AWS (S3 & Athena)**, optimized for high performance, zero-cost local development, and seamless cloud deployment.

It serves as a showcase of core Data Engineering capabilities: ingestion of large-scale raw data, schema validation, streaming transformations, analytical modeling, data quality testing, and automated orchestration.

---

## 🏗️ Architecture Diagram

```
┌───────────────────────────┐
│ E-Commerce Event Producer │  (Python Synthetic Event Generator)
└─────────────┬─────────────┘
              │ (JSON Stream)
              ▼
┌───────────────────────────┐
│    Apache Kafka Broker    │  (Ingestion Buffer & Event Streaming)
└─────────────┬─────────────┘
              │
              ▼ (PySpark Structured Streaming)
┌────────────────────────────────────────────────────────────────────────┐
│                        AWS S3 Data Lake (Lakehouse)                    │
│                                                                        │
│   ┌───────────────────┐    ┌───────────────────┐    ┌──────────────┐   │
│   │   Bronze Layer    │───►│   Silver Layer    │───►│  Gold Layer  │   │
│   │  (Raw JSON Events)│    │ (Cleaned Parquet) │    │(Business KPIs)│  │
│   └───────────────────┘    └───────────────────┘    └──────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
              ▲                                              │
              │ (Scheduled Pipeline & Checks)                │ (SQL Queries)
              │                                              ▼
┌───────────────────────────┐                      ┌──────────────────┐
│      Apache Airflow       │                      │    AWS Athena    │
└───────────────────────────┘                      └──────────────────┘
```

---

## 🔬 Data Layers (Medallion Architecture)

1. **Bronze Layer (Raw Storage)**
   - Ingests unalterable JSON payloads directly from Kafka topics into AWS S3.
   - Preserves historical event data with event-time metadata and partitioning by date (`year/month/day`).
2. **Silver Layer (Cleaned & Standardized)**
   - Consumes Bronze streams using PySpark, performing schema enforcement, data type casting, deduplication, and null filtering.
   - Stores data in compressed, columnar **Parquet / Delta Lake** format for optimal query efficiency.
3. **Gold Layer (Business Analytics & Aggregations)**
   - Aggregates Silver records into domain-specific business metrics (e.g., hourly sales revenue, conversion rate per category, active user sessions).
   - Formatted for fast querying via **AWS Athena** or BI dashboards.

---

## 🛠️ Tech Stack & Skills Highlighted

- **Language & Runtime**: Python 3.11+, SQL (ANSI)
- **Data Processing**: PySpark (Spark Structured Streaming, Watermarking, Windowing)
- **Streaming & Messaging**: Apache Kafka, Zookeeper / KRaft
- **Orchestration**: Apache Airflow (DAGs, Sensor Operators, Data Quality Gates)
- **Storage & Query Engine**: AWS S3, AWS Athena (LocalStack / MinIO for $0 local environment)
- **DevOps & Quality**: Docker, Docker Compose, Git, Pytest, Flake8, CI/CD Workflows
- **GenAI Integration**: Automated unit test generation, synthetic data simulation, and LLM-driven code optimization.

---

## 📂 Repository Structure

```
├── dags/                        # Apache Airflow DAG definitions
│   └── ecommerce_medallion_dag.py
├── src/
│   ├── producers/               # Kafka streaming producers (Python)
│   │   └── event_generator.py
│   ├── spark/                   # PySpark Streaming jobs (Bronze, Silver, Gold)
│   │   ├── bronze_ingestion.py
│   │   ├── silver_transformation.py
│   │   └── gold_aggregation.py
│   └── utils/                   # Helper modules and AWS S3 connections
├── tests/                       # Pytest unit & integration test suites
│   ├── test_transformations.py
│   └── test_producers.py
├── docker-compose.yml           # Local infrastructure stack (Kafka, Spark, Airflow, LocalStack)
├── Dockerfile                   # Custom PySpark/Airflow execution container
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```

---

## 🚀 Quickstart Guide (Local $0 Setup)

### Prerequisites
- [Docker](https://www.docker.com/) & Docker Compose
- Python 3.10+
- Git

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/ecommerce-realtime-pipeline.git
cd ecommerce-realtime-pipeline
```

### 2. Launch Local Infrastructure
Start Kafka, Spark Master/Worker, Airflow, and LocalStack via Docker Compose:
```bash
docker-compose up -d
```

### 3. Run Event Generator
Simulate e-commerce stream traffic (user clicks, cart additions, purchases):
```bash
python src/producers/event_generator.py --events-per-sec 50
```

### 4. Trigger Streaming Pipelines & Airflow Orchestration
- Access Airflow UI at `http://localhost:8080` (Default credentials: `admin` / `admin`).
- Unpause and trigger `ecommerce_medallion_dag` to monitor batch consolidations, quality validations, and Gold table refreshes.

---

## 🧪 Testing & Data Quality

Run the automated test suite with `pytest`:
```bash
pytest tests/ -v
```
Data quality validations are built into the Silver/Gold PySpark pipelines and Airflow DAGs to ensure schema compliance and null-rate thresholds prior to Gold layer promotion.

---

## 📄 License & Attribution

Developed as a Data Engineering showcase project applying enterprise-grade pipeline practices, modern DevOps, and AI-assisted engineering methodologies.

---
*Created by [Your Name] — Senior Data Engineer Candidate*
