# 🚕 Kafka and PySpark Mobility Stream

A runnable local reference for publishing trip events to Kafka and aggregating them with PySpark Structured Streaming.

## Why it exists

Mobility platforms receive events continuously and must handle invalid, duplicate, and late records before producing operational metrics. This repository keeps that path small enough to run locally while showing the important stream-processing boundaries.

## Implemented flow

```text
synthetic trip producer
        ↓ keyed by trip_id
Kafka topic: trip-events
        ↓ explicit JSON schema
Spark Structured Streaming
   ├─ required-field and range filters
   ├─ 10-minute event-time watermark
   ├─ event_id deduplication
   └─ 5-minute city windows
        ↓
console metrics
```

The output contains trip count, gross booking value, and average distance for each city and event-time window.

## Technology

| Component | Use |
|---|---|
| Python | Versioned event contract and producer |
| Apache Kafka | Event transport |
| PySpark Structured Streaming | Parsing, filtering, state, and aggregation |
| Docker Compose | Local Kafka, Spark, and producer services |
| unittest | Dependency-free event-contract tests |

Airflow, PostgreSQL, cloud storage, and NYC TLC ingestion are not implemented in this repository.

## Repository layout

```text
.
├── src/event_contract.py
├── src/producer.py
├── spark/streaming_job.py
├── tests/test_event_contract.py
├── scripts/validate_project.py
├── docker-compose.yml
├── requirements.txt
└── .github/workflows/ci.yml
```

## Validate without infrastructure

```bash
python scripts/validate_project.py
python -m unittest discover -s tests -v
```

CI runs the same commands.

## Run the stream

Prerequisites: Docker with Compose and enough memory for Spark.

```bash
docker compose up --build
```

Kafka starts first. The producer waits for the broker, publishes 100 schema-versioned synthetic events, and exits. Spark reads from the beginning of the topic and prints updated window metrics. Stop the stack with `docker compose down`.

## Event contract

| Field | Rule |
|---|---|
| `schema_version` | Must equal `1` |
| `event_id` | Non-empty unique event identifier |
| `trip_id` | Non-empty partition key |
| `event_time` | ISO-8601 timestamp |
| `city` | Non-empty string |
| `fare_usd` | Non-negative number |
| `distance_km` | Non-negative number |

The producer validates events before publishing. Spark applies an explicit schema and filters invalid records again.

## Results and interpretation

A successful run prints city-level metrics for five-minute windows:

- `trip_count`: valid, deduplicated events in the window
- `gross_booking_value_usd`: sum of synthetic fares
- `avg_distance_km`: average synthetic trip distance

These results confirm the pipeline logic. They do not describe a real mobility market or Uber system.

## Processing guarantees

The job checkpoints Spark state and uses an event-time watermark. It deduplicates within the retained state window. The console sink is for development and is not a durable analytical store. A production system would add a dead-letter path, durable sink, schema registry, access controls, monitoring, and integration tests against deployed infrastructure.
