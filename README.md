# Real-Time Mobility Streaming Data Platform

> Production-oriented Data Engineering portfolio project demonstrating event streaming, distributed processing, validation, orchestration, analytical storage, and real-time mobility analytics using Kafka and PySpark.

## Overview

This project models a ride-hailing data platform using publicly available **NYC TLC trip data** as the source. Batch trip records are transformed into streaming-style events, ingested through Kafka, processed with PySpark Structured Streaming, and prepared for analytical workloads.

The architecture is designed around the responsibilities of a modern Data Engineer:

**Source data → Event ingestion → Validation & enrichment → Distributed processing → Storage → Analytics → Orchestration**

## Architecture

```text
NYC TLC Trip Data
        │
        ▼
  Kafka Producer
        │
        ▼
  Kafka Topic(s)
        │
        ▼
PySpark Structured Streaming
        │
        ├── Schema parsing
        ├── Validation
        ├── Deduplication
        ├── Enrichment
        └── Windowed aggregations
        │
        ▼
Analytics / Operational Storage
        │
        ├── PostgreSQL
        └── Parquet
        │
        ▼
Spark SQL / Reporting

Apache Airflow orchestrates batch and supporting workflows.
```

## Technology Stack

| Layer | Technologies |
|---|---|
| Programming | Python |
| Event Streaming | Apache Kafka |
| Distributed Processing | PySpark Structured Streaming |
| Analytical Processing | Spark SQL |
| Storage | PostgreSQL, Parquet |
| Orchestration | Apache Airflow |
| Infrastructure | Docker / Docker Compose |
| Cloud Path | AWS S3 / EMR compatible |

## Data Source

The project uses NYC Taxi & Limousine Commission trip-record data.

Example public Parquet sources:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet
```

Official dataset portal:

```text
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

## Pipeline Responsibilities

### 1. Ingestion

- Reads source trip records.
- Converts batch records into Kafka events to simulate continuous arrival.
- Publishes events to partitioned Kafka topics.
- Preserves event timestamps for downstream processing.

### 2. Validation & Enrichment

The processing layer is responsible for:

- Schema validation
- Null handling
- Duplicate detection
- Invalid-value filtering
- Timestamp normalization
- Derived trip metrics
- Metadata enrichment

### 3. Distributed Processing

PySpark Structured Streaming provides scalable transformations and analytical aggregation.

Typical workloads include:

- Trips per time window
- Revenue/fare aggregation
- Average trip distance
- Trip duration
- Pickup/drop-off demand
- Peak-hour analysis

### 4. Storage

Processed data is designed for both operational and analytical consumption:

- **Parquet** for efficient columnar storage and data-lake workloads
- **PostgreSQL** for structured query and serving workloads

### 5. Orchestration

Airflow provides workflow scheduling and dependency management for supporting batch operations such as ingestion preparation, validation, aggregation, and warehouse refreshes.

## Project Structure

```text
Uber-Real-Time-Streaming-Data-Platform-using-Kafka-PySpark/
├── data/
│   └── *.parquet                 # Local source data; do not commit large files
├── producer/
│   └── kafka_producer.py         # Publishes trip events to Kafka
├── spark/
│   ├── ingestion_layer.py        # Structured Streaming ingestion
│   ├── validation_enrichment.py  # Data quality + enrichment
│   ├── realtime_processing.py    # Streaming transformations
│   └── analytics_aggregation.py  # Analytical aggregations
├── airflow/
│   └── dags/                     # Orchestration workflows
├── warehouse/                    # PostgreSQL/warehouse assets
├── dashboards/                   # Reporting assets
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Local Environment

### Prerequisites

- Docker Desktop
- Python 3.11+
- Java 11+
- Apache Spark / `spark-submit`

### Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Start infrastructure

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

## Kafka Workflow

Create the event topic using the Kafka CLI available in the Kafka container or local installation:

```bash
kafka-topics --create \
  --topic uber_rides \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Run the producer:

```bash
python producer/kafka_producer.py
```

## PySpark Streaming

Submit the streaming application:

```bash
spark-submit spark/ingestion_layer.py
```

The streaming layer can then be extended with the validation, enrichment, and aggregation stages under `spark/`.

## Example Analytics

The platform supports analytical questions such as:

- How many rides arrive in each five-minute window?
- Which pickup zones have the highest demand?
- What are the busiest hours of the day?
- How does average fare vary by trip distance?
- What is the average trip duration by location?
- Which periods show unusual demand patterns?

## Distributed Systems Considerations

The design highlights several concepts relevant to large-scale Data Engineering:

- Kafka topic partitioning
- Consumer parallelism
- Structured Streaming checkpoints
- Stateful/windowed aggregations
- Incremental processing
- Fault tolerance
- Partition-aware Spark transformations
- Minimizing unnecessary shuffles
- Columnar Parquet storage
- Separation of ingestion and analytics workloads

## Production Evolution

A production implementation could add:

- Schema Registry with Avro or Protobuf
- Kafka consumer groups and durable offsets
- Dead-letter topics for invalid events
- Delta Lake / Apache Iceberg tables
- S3-based medallion data lake
- dbt-based warehouse transformations
- Great Expectations or equivalent data-quality testing
- Prometheus/Grafana observability
- Kubernetes deployment
- CI/CD automation
- Secrets management
- Data lineage and cataloging

## Resume-Ready Summary

**Designed an event-driven mobility data platform using Apache Kafka and PySpark Structured Streaming to ingest, validate, enrich, and aggregate NYC transportation events, with Parquet/PostgreSQL storage and Airflow-oriented orchestration for scalable analytical workloads.**

## Skills Demonstrated

| Area | Skills |
|---|---|
| Streaming | Kafka, Structured Streaming |
| Big Data | PySpark, Spark SQL |
| ETL | Validation, enrichment, aggregation |
| Orchestration | Airflow |
| Storage | Parquet, PostgreSQL |
| Architecture | Event-driven, distributed processing |
| Performance | Partitioning, shuffle reduction, incremental processing |
| Deployment | Docker, cloud-ready architecture |

## Data Engineering Portfolio Note

This repository is intended to demonstrate architecture and engineering patterns rather than claim production ownership of Uber's systems. The source data is public NYC TLC data and the ride-hailing use case is an independent implementation.
