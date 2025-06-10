# Real-Time Rides Data Pipeline

## Project Overview

This project demonstrates a scalable, real-time data pipeline built on **Azure Databricks** and **Confluent Cloud Kafka**. The pipeline processes simulated ride-sharing data, transforming it through a Medallion Architecture (Bronze -> Silver -> Gold) and providing real-time analytics.

## Architecture

![Real-Time Rides Data Pipeline Architecture](screenshots/architecture.png)

This diagram illustrates the end-to-end flow of data: from the Kafka Producer to Confluent Cloud, through the Medallion Architecture layers (Bronze, Silver, Gold) within Azure Databricks, and finally to data consumers for analytics and reporting.

## Technologies Used

* **Cloud Platform:** Azure
* **Data Lake:** Delta Lake
* **Streaming Platform:** Apache Kafka (Confluent Cloud)
* **Data Processing:** Apache Spark Structured Streaming (Databricks)
* **Languages:** Python (PySpark, Kafka Producer)
* **Orchestration:** Databricks Jobs (or implied from notebooks)

## Data Model

The pipeline processes ride-sharing data with a schema including:
* `ride_id`
* `driver_id`
* `passenger_id`
* `start_time`
* `end_time`
* `fare`
* `status`
* `event_timestamp`
* `timestamp` (Kafka timestamp)

## Pipeline Stages (Medallion Architecture)

### 1. Bronze Layer (Raw Ingestion)
* **Source:** Real-time stream from Kafka.
* **Processing:** Reads raw JSON messages from Kafka, casts them to string, and lands them directly into a Bronze Delta table. Minimal transformation, acting as a data lake landing zone.

### 2. Silver Layer (Cleaned & Enriched)
* **Source:** Bronze Delta table (streaming read).
* **Processing:** Parses raw JSON messages into a structured schema, filters out malformed records, and performs basic data type conversions. Enriched data is written to a Silver Delta table.

### 3. Gold Layer (Aggregated & Transformed)
* **Source:** Silver Delta table (streaming read).
* **Processing:** Performs business-level aggregations and transformations, such as hourly ride counts per driver and identifying suspicious rides. Data is continuously updated (upserted) into a Gold Delta table using `foreachBatch` and Delta Lake's `MERGE` operation.

---
