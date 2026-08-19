# 🚕 Real-Time Mobility Streaming Data Platform

> production-style **Data Engineering** reference project covering event streaming, distributed processing, validation, enrichment, analytical storage, and orchestration using **Apache Kafka, PySpark Structured Streaming, PostgreSQL, Parquet, Airflow, and Docker**.

## Executive Summary

This project models a ride-hailing data platform using public **NYC Taxi & Limousine Commission (TLC)** trip data. Batch trip records are converted into streaming-style events, published through Kafka, processed with PySpark Structured Streaming, validated and enriched, and prepared for analytical workloads.

The core engineering flow is:

**Source data → Kafka ingestion → validation/enrichment → distributed processing → storage → analytics → orchestration**

> **Portfolio disclaimer:** This is an independent implementation inspired by ride-hailing workloads. It does not represent Uber's internal architecture or systems.

## 🎯 What The implementation covers

- Kafka-based event ingestion
- PySpark Structured Streaming
- Schema parsing and validation
- Deduplication and data-quality filtering
- Event enrichment and derived metrics
- Windowed analytical aggregations
- Parquet-based analytical storage
- PostgreSQL serving/warehouse patterns
- Airflow-oriented orchestration
- Docker-based local infrastructure
- Distributed-processing and performance considerations

## 🏗️ Architecture

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
             │       │       │
             │       │       └── Windowed aggregations
             │       └────────── Enrichment / derived metrics
             └────────────────── Validation / deduplication
                     │
                     ▼
             Analytical Storage
                ┌────┴────┐
                ▼         ▼
             Parquet  PostgreSQL
                │         │
                └────┬────┘
                     ▼
             Spark SQL / Analytics
                     ▲
                     │
               Airflow workflows
```

## 🧰 Technology Stack

| Layer | Technology | Responsibility |
|---|---|---|
| Source | NYC TLC trip data | Public mobility events |
| Streaming | Apache Kafka | Event transport |
| Processing | PySpark Structured Streaming | Distributed transformation |
| Analytics | Spark SQL | Aggregation and analysis |
| Storage | Parquet | Columnar data-lake output |
| Serving | PostgreSQL | Structured analytical access |
| Orchestration | Apache Airflow | Workflow scheduling/dependencies |
| Infrastructure | Docker / Compose | Reproducible local environment |
| Cloud Path | AWS S3 / EMR compatible | Production evolution |

## 🌐 Data Source

The project uses public NYC Taxi & Limousine Commission trip-record data.

Example public Parquet sources:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet
```

Official portal:

```text
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

## 🔄 Pipeline Responsibilities

### 1. Ingestion

- Reads public trip records.
- Converts batch records into streaming-style events.
- Publishes events to Kafka topics.
- Preserves event timestamps for downstream processing.

### 2. Validation & Enrichment

The processing layer can apply:

- Schema validation
- Null handling
- Duplicate detection
- Invalid-value filtering
- Timestamp normalization
- Derived trip metrics
- Metadata enrichment

### 3. Distributed Processing

PySpark Structured Streaming supports scalable transformations and analytical aggregation such as:

- Trips per time window
- Fare/revenue aggregation
- Average trip distance
- Trip duration
- Pickup/drop-off demand
- Peak-hour analysis

### 4. Storage

- **Parquet** supports efficient columnar storage and data-lake workloads.
- **PostgreSQL** provides structured query and serving capabilities.

### 5. Orchestration

Airflow is used as the workflow-management layer for supporting batch and platform operations such as ingestion preparation, validation, aggregation, and downstream refreshes.

## 📁 Project Structure

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
├── warehouse/                    # PostgreSQL / warehouse assets
├── dashboards/                   # Reporting assets
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 🚀 Local Environment

### Prerequisites

- Docker Desktop
- Python 3.11+
- Java 11+
- Apache Spark / `spark-submit`

### Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Start infrastructure

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

## 📡 Kafka Workflow

Create the event topic using the Kafka CLI available in the container or local installation:

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

## ⚡ PySpark Streaming

Submit the streaming application:

```bash
spark-submit spark/ingestion_layer.py
```

The streaming layer can then be extended with the validation, enrichment, and aggregation stages under `spark/`.

## 📊 Analytical Workloads

The platform is designed to answer questions such as:

- How many rides arrive in each five-minute window?
- Which pickup zones have the highest demand?
- What are the busiest hours?
- How does average fare vary with trip distance?
- What is average trip duration by location?
- Which periods show unusual demand patterns?

## 🧠 Distributed Systems Considerations

The project provides an documented foundation for discussing:

- Kafka topic partitioning
- Producer/consumer parallelism
- Structured Streaming checkpoints
- Stateful and windowed aggregations
- Incremental processing
- Fault-tolerance patterns
- Partition-aware Spark transformations
- Shuffle reduction
- Columnar storage
- Separation of ingestion and analytics workloads

## 🧪 Production Data-Quality Evolution

A production deployment should add explicit quality gates around the streaming boundary:

- Schema contracts and schema evolution
- Dead-letter topics for invalid events
- Duplicate-event and idempotency controls
- Completeness/freshness checks
- Streaming checkpoint monitoring
- Pipeline latency and throughput metrics
- Automated regression tests

## 🔮 Production Evolution

A larger deployment could add:

- Schema Registry with Avro or Protobuf
- Kafka consumer groups and durable offsets
- Delta Lake or Apache Iceberg
- S3-based medallion data lake
- dbt warehouse transformations
- Great Expectations or equivalent data-quality testing
- Prometheus/Grafana observability
- Kubernetes deployment
- CI/CD automation
- Secrets management
- Data lineage and cataloging

## 💼 Data Engineer Interview Talking Points

1. **Why Kafka?** Decouples producers and consumers and supports scalable event transport.
2. **Why Spark Structured Streaming?** Provides distributed processing, event-time semantics, checkpoints, and scalable aggregations.
3. **Why Parquet?** Efficient columnar storage for analytical workloads.
4. **How does this scale?** Increase Kafka partitions and consumer parallelism while partitioning Spark workloads appropriately.
5. **How do you handle bad data?** Validate at ingestion/processing boundaries and route invalid events to a dead-letter path.
6. **How would you productionize it?** Add schema management, observability, orchestration, cloud storage, CI/CD, security, and data-quality SLAs.

## 📌 Project summary Project Description

**Designed an event-driven mobility data platform using Apache Kafka and PySpark Structured Streaming to ingest, validate, enrich, and aggregate public NYC transportation events, with Parquet/PostgreSQL storage and Airflow-oriented orchestration for scalable analytical workloads.**

## 👨‍💻 Portfolio

**Manish Kallu** — Data engineering work focused on streaming systems, distributed processing, SQL analytics, orchestration, and production-style data platforms.

- GitHub: [manishkallu01-wq](https://github.com/manishkallu01-wq)
- Email: manishkallu01@gmail.com

## Scope

This repository covers architecture and engineering patterns rather than claiming production ownership of Uber's systems. The data source is public NYC TLC trip data and the ride-hailing platform is an independent local implementation.

## Repository implementation status

This repository now includes a runnable reference skeleton rather than documentation alone:

- `docker-compose.yml` provisions Kafka and a Spark runtime.
- `src/producer.py` emits deterministic, schema-versioned trip events.
- `spark/streaming_job.py` validates, deduplicates, windows, and aggregates the stream.
- `tests/test_event_contract.py` tests the event contract without external services.
- `scripts/validate_project.py` provides a dependency-free structural and syntax gate.
- `.github/workflows/ci.yml` runs the validation and unit tests.

## Reproducibility contract

```bash
python scripts/validate_project.py
python -m unittest discover -s tests -v
docker compose up --build
```

The project is complete when valid trip events enter Kafka, malformed records are rejected, duplicate `event_id` values are removed within the watermark, and windowed city metrics are written to the console sink. Kafka/Spark must be available for the integration run; unit tests intentionally run without them.

## Data-engineering methodology

1. Model the trip as a versioned event with an immutable ID and event time.
2. Partition Kafka records by `trip_id` for stable routing.
3. Parse against an explicit Spark schema; do not infer production schemas.
4. Quarantine invalid records rather than silently coercing them.
5. Apply event-time watermarks and deduplication for late/replayed events.
6. Aggregate in bounded windows and checkpoint state for recovery.
7. Expose lag, invalid-event rate, throughput, and end-to-end latency in production.

## Business value

The reference pipeline converts high-volume trip activity into timely city-level demand and revenue signals. Those outputs support marketplace balancing, operations monitoring, incentive planning, and anomaly detection while preserving a clear path from raw event to metric.

## Results and interpretation

A successful reference run prints one row per five-minute window and city with:

| Metric | Interpretation |
|---|---|
| `trip_count` | Valid, deduplicated trip events observed in the window |
| `gross_booking_value_usd` | Sum of event fares; a pipeline demonstration metric, not audited revenue |
| `avg_distance_km` | Mean trip distance for the window |

Results must be interpreted with the generated-data boundary in mind: the producer creates deterministic synthetic values, so output proves pipeline behavior, state handling, and metric logic—not real marketplace performance. Production conclusions require governed source data, completeness reconciliation, late-data reporting, and metric-owner approval.
