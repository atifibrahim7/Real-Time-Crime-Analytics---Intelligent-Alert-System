# 🔍 Real-Time Crime Analytics — Lambda Architecture

A complete **Lambda Architecture** implementation for real-time Chicago crime data analytics, built with **Apache Spark**, **Apache Kafka**, **Apache Storm** (Python-based), **PostgreSQL**, **MongoDB**, and **Streamlit**.

> **Course:** Big Data Analytics  
> **Author:** Muhammad Ali (i220827)  
> **University:** FAST NUCES, Islamabad

---

## 📐 Architecture Overview

```
                         ┌──────────────────────────────────┐
                         │        DATA SOURCES              │
                         │   Chicago Open Data Portal CSVs  │
                         └────────────┬─────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                   │
              ┌─────▼─────┐   ┌──────▼──────┐   ┌───────▼───────┐
              │  BATCH     │   │  SPEED      │   │  SERVING      │
              │  LAYER     │   │  LAYER      │   │  LAYER        │
              │            │   │             │   │               │
              │ Spark      │   │ Kafka →     │   │ PostgreSQL    │
              │ (Clean +   │   │ Storm       │   │ MongoDB       │
              │  Analytics │   │ Topology    │   │               │
              │  + ML)     │   │             │   │               │
              └─────┬──────┘   └──────┬──────┘   └───────┬───────┘
                    │                 │                   │
                    └─────────────────┼───────────────────┘
                                      │
                         ┌────────────▼─────────────────────┐
                         │     APPLICATION LAYER             │
                         │     Streamlit Dashboard           │
                         └──────────────────────────────────┘
```

---

## 📁 Project Structure

```
RealTimeCrimeAnalytics/
├── config/
│   └── config.yaml                  # Centralized configuration
├── data/
│   ├── crimes.csv                   # Raw crime data
│   ├── arrests.csv                  # Raw arrest data
│   ├── police_stations.csv          # Raw police station data
│   ├── sex_offenders.csv            # Raw sex offender data
│   ├── violence.csv                 # Raw violence data
│   └── cleaned/                     # Parquet output (generated)
├── kafka/
│   ├── producer.py                  # Kafka crime event simulator
│   └── requirements.txt
├── spark/
│   ├── preprocessing/
│   │   └── clean_data.py            # Data cleaning + Parquet conversion
│   ├── analytics/
│   │   └── batch_analytics.py       # Core BI analytics (7.1–7.6)
│   ├── ml/
│   │   └── hotspots.py              # KMeans crime hotspot detection
│   └── schemas/
├── storm/
│   ├── spouts/
│   │   ├── __init__.py
│   │   └── kafka_spout.py           # KafkaSpout — reads from Kafka
│   ├── bolts/
│   │   ├── __init__.py
│   │   ├── parse_bolt.py            # ParseBolt — JSON validation
│   │   ├── district_bolt.py         # DistrictBolt — route by district
│   │   ├── window_bolt.py           # WindowBolt — sliding time window
│   │   ├── anomaly_bolt.py          # AnomalyBolt — threshold detection
│   │   └── alert_bolt.py            # AlertBolt — write to DBs
│   ├── topology/
│   │   ├── __init__.py
│   │   └── crime_topology.py        # Main topology orchestrator
│   └── requirements.txt
├── app/                             # Streamlit dashboard (mounted volume)
├── docker-compose.yml               # All 9+ services
├── data_download.sh                 # Download datasets from Chicago API
└── README.md                        # This file
```

---

## 🛠️ Prerequisites

- **Docker** & **Docker Compose** (v2+)
- **Git**
- **curl** or **wget**
- ~4 GB free RAM (for Spark + Storm + databases)

---

## 🚀 Step-by-Step Setup Guide

### Step 1: Clone the Repository

```bash
git clone <your-repo-url> RealTimeCrimeAnalytics
cd RealTimeCrimeAnalytics
```

---

### Step 2: Download the Datasets

Run the provided download script to fetch all 5 CSV datasets from the Chicago Open Data Portal:

```bash
chmod +x data_download.sh
./data_download.sh
```

This creates the `data/` directory with:
| File | Description | Source |
|------|-------------|--------|
| `crimes.csv` | 50,000 crime incident records | Chicago Crime Data |
| `arrests.csv` | 10,000 arrest records | Chicago Arrests |
| `police_stations.csv` | All Chicago police stations | Chicago Police Stations |
| `sex_offenders.csv` | 10,000 sex offender records | Chicago Sex Offenders |
| `violence.csv` | 10,000 violence reduction records | Chicago Violence Data |

Verify the downloads:
```bash
ls -lh data/
```

---

### Step 3: Start All Docker Containers

Launch all 9 services (Zookeeper, Kafka, Storm, Spark, PostgreSQL, MongoDB, Streamlit):

```bash
docker compose up -d
```

Verify all containers are running:
```bash
docker ps
```

You should see these containers:
| Container | Image | Ports |
|-----------|-------|-------|
| `zookeeper` | wurstmeister/zookeeper | 2181 |
| `kafka` | wurstmeister/kafka | 9092 |
| `nimbus` | storm:2.5.0 | 6627 |
| `supervisor` | storm:2.5.0 | — |
| `storm-ui` | storm:2.5.0 | 8080 |
| `spark-master` | apache/spark:3.5.3 | 7077, 8081 |
| `spark-worker` | apache/spark:3.5.3 | — |
| `postgres` | postgres:15-alpine | 5432 |
| `mongodb` | mongo:6.0 | 27017 |
| `streamlit` | python:3.9-slim-buster | 8501 |

> **Troubleshooting:** If any container fails to stop/start due to permission errors, run:
> ```bash
> sudo systemctl restart docker
> docker compose down --remove-orphans
> docker compose up -d
> ```

---

### Step 4: Batch Layer — Data Cleaning (Spark)

This step reads the raw CSV files, applies explicit schemas (no `inferSchema`), casts data types, and saves as Parquet.

```bash
# Create the output directory with proper permissions
docker exec -u root spark-master mkdir -p /data/cleaned
docker exec -u root spark-master chmod 777 /data/cleaned

# Copy the cleaning script into the container
docker cp spark/preprocessing/clean_data.py spark-master:/tmp/clean_data.py

# Run the cleaning job
docker exec -it spark-master /opt/spark/bin/spark-submit /tmp/clean_data.py
```

Verify the Parquet files were created:
```bash
docker exec -it spark-master ls -la /data/cleaned/
```

Expected output:
```
arrests.parquet/
crimes.parquet/
police_stations.parquet/
sex_offenders.parquet/
violence.parquet/
```

---

### Step 5: Batch Layer — Core Analytics (Spark → PostgreSQL)

This step computes business intelligence metrics and writes them to PostgreSQL.

```bash
# Download the PostgreSQL JDBC driver
wget -q -O /tmp/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Copy the driver and script into the container
docker cp /tmp/postgresql-42.7.3.jar spark-master:/tmp/postgresql-42.7.3.jar
docker cp spark/analytics/batch_analytics.py spark-master:/tmp/batch_analytics.py

# Run the analytics job
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --jars /tmp/postgresql-42.7.3.jar \
    /tmp/batch_analytics.py
```

This creates **14 tables** in PostgreSQL:
- `crime_trends_by_year`, `crime_trends_by_month`, `crime_trends_by_day_of_week`, `crime_trends_by_hour`
- `arrest_rate_by_crime_type`, `top10_crime_types_by_arrest_rate`, `arrest_rate_by_district`, `arrests_by_race`
- `violence_by_month`, `violence_by_district`, `gunshot_injury_proportion`, `gunshot_proportion_by_district`
- `sex_offender_density_by_district`
- `district_arrest_vs_violence_correlation`

---

### Step 6: Batch Layer — ML Hotspot Detection (Spark MLlib → PostgreSQL)

This step runs KMeans clustering (k=10) on crime coordinates to identify hotspots.

```bash
# Install numpy in the Spark container (required by MLlib)
docker exec -u root spark-master pip install numpy

# Copy and run the ML script
docker cp spark/ml/hotspots.py spark-master:/tmp/hotspots.py
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --jars /tmp/postgresql-42.7.3.jar \
    /tmp/hotspots.py
```

This creates **2 more tables**: `hotspots` and `crimes_with_clusters`.

Verify all **16 tables** exist:
```bash
docker exec -it postgres psql -U admin -d crime_db -c "\dt"
```

---

### Step 7: Speed Layer — Kafka Producer (Crime Simulator)

This script simulates a real-time crime feed by reading `crimes.csv` row-by-row and publishing JSON messages to Kafka.

```bash
# Install dependencies in the Streamlit container
docker exec -u root streamlit pip install kafka-python PyYAML

# Create directories and copy files
docker exec -u root streamlit mkdir -p /app/config /app/data
docker cp config/config.yaml streamlit:/app/config/config.yaml
docker cp data/crimes.csv streamlit:/app/data/crimes.csv
docker cp kafka/producer.py streamlit:/app/producer.py

# Start the producer (runs continuously — use a dedicated terminal)
docker exec -it streamlit python /app/producer.py --csv /app/data/crimes.csv --delay 0.2
```

> **Note:** Keep this terminal open. The producer sends ~5 messages/second with `--delay 0.2`.

---

### Step 8: Speed Layer — Storm Topology (Real-Time Anomaly Detection)

Open a **second terminal** and start the Storm topology:

```bash
# Install dependencies
docker exec -u root streamlit pip install kafka-python PyYAML psycopg2-binary pymongo

# Copy the entire Storm project into the container
docker cp storm/ streamlit:/app/storm/

# Start the topology (runs continuously — use a dedicated terminal)
docker exec -it streamlit python /app/storm/topology/crime_topology.py
```

You should see output like:
```
🌪️  Storm Topology — Real-Time Crime Anomaly Detection
Pipeline: KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt

🚨 ANOMALY [LOW] District 006: 5 crimes in 300s window (threshold: 5)
🚨 ANOMALY [LOW] District 011: 7 crimes in 300s window (threshold: 5)
```

---

### Step 9: Verify the Complete Pipeline

Open a **third terminal** to check all outputs:

```bash
# Check Kafka messages are flowing
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic crime_events --max-messages 3

# Check real-time alerts in PostgreSQL
docker exec -it postgres psql -U admin -d crime_db \
    -c "SELECT alert_id, district, severity, crime_count_in_window FROM speed_layer_alerts LIMIT 10;"

# Check real-time alerts in MongoDB
docker exec -it mongodb mongosh --eval "db.alert_logs.find().limit(5).pretty()" crime_db

# Check batch analytics tables
docker exec -it postgres psql -U admin -d crime_db -c "\dt"
```

---

## 🌐 Web UIs

| Service | URL |
|---------|-----|
| **Spark Master UI** | http://localhost:8081 |
| **Storm UI** | http://localhost:8080 |
| **Streamlit Dashboard** | http://localhost:8501 |

---

## ⚙️ Configuration

All settings are centralized in `config/config.yaml`:

```yaml
kafka:
  broker: "kafka:9092"
  topic: "crime_events"

producer:
  csv_path: "/data/crimes.csv"
  delay_seconds: 1.0

storm:
  anomaly_threshold: 5       # crimes in window to trigger alert
  window_seconds: 300        # 5-minute sliding window

postgresql:
  host: "postgres"
  port: 5432
  database: "crime_db"
  user: "admin"
  password: "admin"

mongodb:
  host: "mongodb"
  port: 27017
  database: "crime_db"
```

---

## 🛑 Stopping the System

```bash
# Stop the producer and topology (Ctrl+C in their terminals)

# Stop all Docker containers
docker compose down

# To also remove volumes (⚠️ deletes database data):
docker compose down -v
```

---

## 📊 PostgreSQL Tables Reference

| Table | Source | Description |
|-------|--------|-------------|
| `crime_trends_by_year` | Batch | Crime counts by year |
| `crime_trends_by_month` | Batch | Crime counts by year + month |
| `crime_trends_by_day_of_week` | Batch | Crime counts by day of week |
| `crime_trends_by_hour` | Batch | Crime counts by hour |
| `arrest_rate_by_crime_type` | Batch | Arrest rates per crime type |
| `top10_crime_types_by_arrest_rate` | Batch | Top 10 crime types by arrest rate |
| `arrest_rate_by_district` | Batch | Arrest rates per district |
| `arrests_by_race` | Batch | Arrest counts by race |
| `violence_by_month` | Batch | Homicides vs non-fatal by month |
| `violence_by_district` | Batch | Violence incidents by district |
| `gunshot_injury_proportion` | Batch | Overall gunshot injury rate |
| `gunshot_proportion_by_district` | Batch | Gunshot rate per district |
| `sex_offender_density_by_district` | Batch | Offender density per district |
| `district_arrest_vs_violence_correlation` | Batch | Arrest rate vs violence rate |
| `hotspots` | ML | KMeans cluster centroids (k=10) |
| `crimes_with_clusters` | ML | Each crime labeled with cluster |
| `speed_layer_alerts` | Speed | Real-time anomaly alerts |

---

## 🧰 Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Batch Processing | Apache Spark | 3.5.3 |
| Stream Ingestion | Apache Kafka | Latest |
| Stream Processing | Storm-style Python Topology | Custom |
| Coordination | Apache Zookeeper | Latest |
| Relational DB | PostgreSQL | 15 |
| Document DB | MongoDB | 6.0 |
| ML | PySpark MLlib (KMeans) | 3.5.3 |
| Dashboard | Streamlit | Latest |
| Containerization | Docker Compose | v2 |

---

## 📝 License

This project is for academic purposes as part of the Big Data Analytics course at FAST NUCES.
