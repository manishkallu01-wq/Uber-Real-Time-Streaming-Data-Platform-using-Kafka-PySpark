# 🚖 Uber Real-Time Streaming Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-Streaming-orange?style=for-the-badge&logo=apachespark" />
  <img src="https://img.shields.io/badge/Kafka-Event%20Streaming-black?style=for-the-badge&logo=apachekafka" />
  <img src="https://img.shields.io/badge/Airflow-Orchestration-blue?style=for-the-badge&logo=apacheairflow" />
  <img src="https://img.shields.io/badge/PostgreSQL-Warehouse-blue?style=for-the-badge&logo=postgresql" />
  <img src="https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker" />
  <img src="https://img.shields.io/badge/AWS-Cloud%20Ready-orange?style=for-the-badge&logo=amazonaws" />
</p>

---

# 📌 Overview

This project is a production-style real-time streaming data engineering platform inspired by large-scale ride-sharing systems such as Uber. The platform processes transportation events using Kafka, PySpark Structured Streaming, Airflow, PostgreSQL, and Docker to simulate scalable distributed data engineering workflows.

The system demonstrates:

- Event-driven architecture
- Real-time ETL processing
- Distributed Spark optimization
- Scalable pipeline orchestration
- Analytics-ready data modeling
- Production-style streaming systems

---

# 🏗️ System Architecture

```text
NYC TLC Datasets
        ↓
Kafka Producer
        ↓
Kafka Topics
        ↓
Spark Structured Streaming
        ↓
Validation & Enrichment Layer
        ↓
Real-Time Processing Layer
        ↓
Operational Data Store
        ↓
Analytics & Aggregation Layer
        ↓
Dashboard / Reporting
```

---

# ⚙️ Tech Stack

| Category | Technologies |
|---|---|
| Streaming | Apache Kafka |
| Distributed Processing | PySpark Structured Streaming |
| Workflow Orchestration | Apache Airflow |
| Storage | PostgreSQL |
| Containerization | Docker |
| Programming | Python |
| Data Format | Parquet |
| Analytics | Spark SQL |
| Cloud Ready | AWS S3 / EMR Compatible |

---

# 🚀 Key Features

✅ Real-time ride event streaming  
✅ Distributed Spark-based ETL pipelines  
✅ Kafka event-driven architecture  
✅ Incremental processing workflows  
✅ Validation and enrichment pipelines  
✅ Streaming aggregations and analytics  
✅ Workflow orchestration with Airflow  
✅ Scalable distributed systems optimization  
✅ Analytics-ready warehouse modeling  
✅ Cloud-ready deployment architecture  

---

# 📂 Datasets

This project uses large-scale NYC TLC transportation datasets.

## Included Datasets

### 🚕 Yellow Taxi Trip Dataset

Download URL:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

### 🚖 FHV (For-Hire Vehicle) Dataset

Download URL:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet
```

### 🌐 Official NYC TLC Dataset Portal

```text
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

---

# 📊 Dataset Characteristics

- Millions of transportation records
- Large-scale distributed processing
- Real-world transportation analytics
- Columnar parquet storage format
- Streaming-compatible event simulation

---

# 🧾 Example Dataset Schema

| Column | Description |
|---|---|
| pickup_datetime | Ride pickup timestamp |
| dropoff_datetime | Ride dropoff timestamp |
| trip_distance | Distance traveled |
| fare_amount | Fare charged |
| passenger_count | Number of passengers |
| payment_type | Payment method |
| pickup_location | Pickup zone |
| dropoff_location | Dropoff zone |

---

# 🧠 Event-Driven Pipeline Layers

## 1️⃣ Ingestion Layer

Responsible for:

- Kafka event ingestion
- Schema parsing
- Raw event processing
- Timestamp management

### Technologies Used

- Kafka
- PySpark Structured Streaming

---

## 2️⃣ Validation & Enrichment Layer

Responsible for:

- Schema validation
- Deduplication
- Null handling
- Data enrichment
- Data quality checks

### Features

- Incremental processing
- Event validation
- Error handling
- Metadata enrichment

---

## 3️⃣ Real-Time Processing Layer

Responsible for:

- Streaming aggregations
- Ride metrics
- Distributed transformations
- Window-based analytics
- Performance optimization

### Spark Optimization Techniques

- Partitioning strategies
- Reduced data shuffling
- Incremental transformations
- Efficient aggregations

---

## 4️⃣ Operational Data Store

Responsible for:

- Structured analytics storage
- Query-ready datasets
- Operational reporting
- Serving processed data

### Technologies Used

- PostgreSQL
- Spark SQL

---

## 5️⃣ Analytics Layer

Responsible for:

- KPI generation
- Revenue analytics
- Ride trend analysis
- Peak-hour insights
- Business reporting

---

# 📁 Project Structure

```text
uber-data-platform/
│
├── data/
│   ├── yellow_tripdata_2024-01.parquet
│   └── fhv_tripdata_2024-01.parquet
│
├── producer/
│   └── kafka_producer.py
│
├── spark/
│   ├── ingestion_layer.py
│   ├── validation_enrichment.py
│   ├── realtime_processing.py
│   └── analytics_aggregation.py
│
├── airflow/
│   └── dags/
│
├── warehouse/
│
├── dashboards/
│
├── docker/
│
├── requirements.txt
│
├── docker-compose.yml
│
└── README.md
```

---

# 🔧 Local Setup

## Prerequisites

Install the following:

- Docker Desktop
- Python 3.11+
- Java 11+
- Apache Spark
- VS Code

---

# 📦 Install Dependencies

```bash
pip install pandas pyarrow kafka-python pyspark sqlalchemy psycopg2-binary
```

---

# 🐳 Docker Compose Setup

Create:

```yaml
version: '3'

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
    depends_on:
      - zookeeper

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: uber
      POSTGRES_PASSWORD: uber
      POSTGRES_DB: uberdb
    ports:
      - "5432:5432"
```

---

# ▶️ Start Infrastructure

```bash
docker-compose up -d
```

Verify running containers:

```bash
docker ps
```

---

# 📨 Create Kafka Topic

```bash
kafka-topics --create \
--topic uber_rides \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

---

# 🚀 Kafka Producer

## producer/kafka_producer.py

```python
from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_parquet('data/yellow_tripdata_2024-01.parquet')

for _, row in df.iterrows():
    producer.send('uber_rides', row.to_dict())
    time.sleep(0.1)
```

Run producer:

```bash
python producer/kafka_producer.py
```

---

# ⚡ Spark Structured Streaming Consumer

## spark/ingestion_layer.py

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UberStreamingPlatform") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "uber_rides") \
    .load()

query = kafka_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

Run consumer:

```bash
spark-submit spark/ingestion_layer.py
```

---

# 📈 Example Analytics

The platform can generate real-time analytics such as:

- 🚕 Rides per hour
- 📍 Top pickup/dropoff zones
- 💰 Revenue trends
- ⏱️ Trip duration analysis
- 📈 Peak traffic windows
- 🔥 Ride surge patterns
- 🚦 Active trip monitoring

---

# 🔄 Workflow Orchestration

Apache Airflow is used for:

- DAG scheduling
- Pipeline orchestration
- Retry handling
- Monitoring workflows
- Dependency management
- Batch aggregation workflows

### Example DAGs

- ingestion pipeline
- validation workflow
- aggregation pipeline
- warehouse refresh jobs

---

# 📈 Scalability & Optimization

This project focuses heavily on distributed systems optimization techniques commonly used in enterprise data engineering environments.

## Optimization Techniques Used

- Spark partitioning strategies
- Incremental processing
- Streaming aggregations
- Distributed transformations
- Reduced data shuffling
- Checkpointing
- Fault tolerance
- Scalable ETL design
- Efficient Spark SQL operations

---

# ☁️ Cloud-Ready Design

The platform is designed to be compatible with:

- AWS S3
- EMR
- Databricks
- Delta Lake
- Kubernetes
- Dockerized Spark clusters

---

# 🔮 Future Improvements

Planned enhancements include:

- Delta Lake integration
- dbt transformations
- Great Expectations validation
- Streamlit dashboards
- Grafana monitoring
- Kubernetes deployment
- CI/CD pipelines
- AWS S3 integration
- EMR deployment
- Real-time anomaly detection
- Iceberg support
- Data observability

---

# 💼 Resume Highlights

### Example 1

Built a production-style Uber-inspired real-time streaming data platform using Kafka, PySpark Structured Streaming, Airflow, and PostgreSQL to process transportation events at scale.

### Example 2

Designed an event-driven distributed architecture with ingestion, validation, enrichment, operational, and analytics layers optimized for scalable ETL workflows.

### Example 3

Implemented distributed Spark optimization techniques including partitioning, incremental processing, and streaming aggregations for high-performance data processing systems.

---

# 🎯 Skills Demonstrated


| Area | Skills |
|---|---|
| Streaming | Kafka, Structured Streaming |
| Big Data | Spark, Distributed Systems |
| ETL | Data Pipelines, Transformations |
| Orchestration | Airflow |
| Warehousing | PostgreSQL |
| Optimization | Spark Tuning, Partitioning |
| Architecture | Event-Driven Design |
| Engineering | Scalable Data Processing |


---

# 🧪 Sample Use Cases

- Real-time ride analytics
- Transportation trend analysis
- Peak traffic monitoring
- Revenue forecasting
- Driver activity insights
- Distributed ETL optimization
- Streaming event processing
- Large-scale operational analytics

---

# 📚 Learning Outcomes

This project demonstrates hands-on experience with:

- Real-time streaming systems
- Distributed data engineering
- Event-driven architecture
- Spark optimization
- Workflow orchestration
- Cloud-oriented pipeline design
- Production-scale ETL systems
- Analytics engineering

---

# 👨‍💻 Author

## Manish Reddy Kallu

📧 manishkallu01@gmail.com

🔗 LinkedIn: https://www.linkedin.com/in/manish-reddy-kallu-8a7587254/

💻 GitHub: https://github.com/manishkallu01-wq

---

# ⭐ Acknowledgements

- NYC TLC Open Data
- Apache Spark Community
- Apache Kafka
- Apache Airflow
- Open-source Data Engineering Community
