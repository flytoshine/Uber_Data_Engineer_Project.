# Uber Data Engineering Project

A real-time data engineering project built with **FastAPI, Azure Data Factory, Azure Event Hubs, Databricks, Apache Spark, Delta Lake, and dimensional modeling**.

The project simulates live ride data, processes it through a streaming lakehouse architecture, enriches it with reference data, and produces an analytics-ready Star Schema.

## Architecture

<img width="700" height="700" alt="uber data engineering" src="https://github.com/user-attachments/assets/cc6af6b6-c999-4303-b610-d98a28498e58" />


### Data Flow

```text
                         Live Ride Data
                              │
                              ▼
                       FastAPI Web App
                              │
                              ▼
                      Azure Data Factory
                              │
                              ▼
                       Azure Event Hubs
                              │
                              ▼
                         Databricks
                              │
                    Spark Structured Streaming
                              │
                              ▼
                           Bronze
                              │
                    Spark Declarative Pipelines
                              │
                              ▼
                           Silver
                        One Big Table
                              │
                              ▼
                            Gold
                         Star Schema
                              │
                              ▼
                         Analytics / BI
```

### Reference Data Flow

```text
                    GitHub Reference Data
                              │
                              ▼
                      Azure Data Factory
                              │
                              ▼
                           Bronze
                              │
                              ▼
                     Reference Tables
                              │
                              ▼
                         Silver OBT
                              │
                              ▼
                            Gold
```

## Tech Stack

| Technology | Purpose |
|---|---|
| **FastAPI** | Simulated live ride data source |
| **Azure Data Factory** | Data ingestion and orchestration |
| **Azure Event Hubs** | Real-time event streaming |
| **Azure Data Lake Storage** | Data lake storage |
| **Databricks** | Data processing and pipeline execution |
| **Apache Spark** | Distributed data processing |
| **Spark Structured Streaming** | Real-time stream processing |
| **Spark Declarative Pipelines** | Declarative pipeline development |
| **Delta Lake** | Lakehouse storage |
| **SQL / Python** | Transformation logic |
| **GitHub** | Source control and reference data |

## Data Sources

### Live Ride Data

A custom **FastAPI web application** simulates an Uber-style ride platform and generates live ride events.

The events contain ride-related information such as:

- Passenger
- Driver
- Vehicle
- Booking
- Location
- Payment
- Fare
- Rating
- Ride status

The application acts as the simulated operational source for the real-time pipeline.

### Reference Data

GitHub is used as a static/internal API-style source for reference and mapping data.

Examples include:

- Cities
- Payment methods
- Ride statuses
- Vehicle types
- Vehicle makes
- Cancellation reasons

These datasets are used to enrich the live streaming data before it reaches the Gold layer.

## Medallion Architecture

The processing architecture follows:

```text
Bronze → Silver → Gold
```

### Bronze

The Bronze layer stores source-aligned data.

The primary streaming dataset is:

```text
rides_raw
```

It contains raw ride events and reference datasets with minimal transformation.

### Silver

The Silver layer contains the main transformation and enrichment logic.

The project uses a **One Big Table (OBT)**:

```text
silver_obt
```

The OBT combines streaming ride data with reference data.

```text
Live Ride Data
      +
Reference Data
      │
      ▼
Business Enrichment
      │
      ▼
silver_obt
```

A **3-minute watermark** is applied to the booking timestamp to handle late-arriving streaming events.

### Gold

The Gold layer converts the Silver OBT into an analytical **Star Schema**.

```text
                         ┌─────────────────┐
                         │  dim_passenger  │
                         └────────┬────────┘
                                  │
                                  │
┌────────────────┐        ┌───────▼────────┐        ┌────────────────┐
│   dim_driver   │───────▶│    fact_ride   │◀───────│  dim_vehicle   │
└────────────────┘        └───────┬────────┘        └────────────────┘
                                  │
                       ┌──────────┼──────────┐
                       │          │          │
                       ▼          ▼          ▼
                ┌────────────┐ ┌────────────┐ ┌───────────────┐
                │dim_payment │ │dim_booking │ │ dim_location  │
                └────────────┘ └────────────┘ └───────────────┘
```

## Fact Table

### `fact_ride`

The central fact table represents the ride-level business event.

The defined grain is:

```text
1 Row = 1 Ride Event
```

Typical measures include:

- Distance
- Duration
- Base fare
- Distance fare
- Time fare
- Surge multiplier
- Tip amount
- Total fare
- Rating

## Dimension Tables

The Gold layer contains:

```text
dim_passenger
dim_driver
dim_vehicle
dim_payment
dim_booking
dim_location
```

These dimensions provide descriptive context for the ride fact.

## Change Data Capture

The Gold layer uses Databricks Auto CDC:

```text
create_auto_cdc_flow
```

CDC is used to propagate inserts, updates, and deletes from the Silver layer into the Gold dimensional model.

## Slowly Changing Dimensions

The project demonstrates two SCD strategies.

### SCD Type 1

Used where only the latest state is required.

Examples:

```text
Passenger
Driver
Vehicle
Payment
Booking
```

Existing values are updated with the latest state.

### SCD Type 2

Used where historical changes need to be preserved.

Applied to:

```text
Location
```

Historical versions are retained using the update timestamp as the sequencing field.

```text
Location Version 1
        │
        ▼
Location Version 2
        │
        ▼
Location Version 3
```

## Data Modeling

The project pays particular attention to fact-table grain and join cardinality.

The intended fact grain is:

```text
1 Ride → 1 Fact Record
```

Incorrect many-to-many joins can cause fan-out:

```text
1 Ride
  │
  ▼
Multiple Matches
  │
  ▼
Duplicated Fact Rows
```

This can lead to incorrect ride counts, revenue, distance, and other measures.

Therefore, relationships and join cardinality are considered when building both the Silver OBT and Gold Star Schema.

## Project Structure

```text
UBER_DATA_ENGINEERING/
│
├── ingest.py
├── bronze_adls.py
├── silver_obt.sql
├── model.py
│
├── explorations/
│   └── sample_exploration.py
│
├── docs/
│   └── architecture.png
│
└── README.md
```

## Pipeline Components

### `ingest.py`

Consumes live ride events from Azure Event Hubs using Spark Structured Streaming.

### `bronze_adls.py`

Loads static and reference datasets into the Bronze layer.

### `silver_obt.sql`

Creates the enriched Silver One Big Table:

```text
silver_obt
```

### `model.py`

Builds the Gold fact and dimension model and applies CDC/SCD logic.

### `explorations/sample_exploration.py`

Used for development, testing, and streaming-data exploration.

## End-to-End Pipeline

```text
FastAPI
   │
   ▼
Azure Data Factory
   │
   ▼
Azure Event Hubs
   │
   ▼
Databricks
   │
   ▼
Spark Structured Streaming
   │
   ▼
Bronze
   │
   ▼
Silver OBT
   │
   ▼
Gold Star Schema
   │
   ▼
Analytics / BI
```

## Key Engineering Practices

- Real-time streaming
- Event-driven ingestion
- Medallion architecture
- One Big Table
- Reference-data enrichment
- Watermarking
- Spark Declarative Pipelines
- Delta Lake
- Change Data Capture
- SCD Type 1
- SCD Type 2
- Dimensional modeling
- Star Schema
- Fact-table grain management
- Join-cardinality and fan-out control

## Project Scope

This project focuses on:

**FastAPI → Azure Data Factory → Azure Event Hubs → Databricks → Bronze → Silver → Gold**

including:

- Real-time ingestion
- Spark Structured Streaming
- Spark Declarative Pipelines
- Medallion architecture
- Reference-data enrichment
- One Big Table
- Watermarking
- Change Data Capture
- SCD Type 1
- SCD Type 2
- Fact and dimension modeling
- Star Schema

Production CI/CD, monitoring, alerting, and BI deployment are outside the current scope.

## What This Project Demonstrates

**FastAPI · Azure Data Factory · Azure Event Hubs · Databricks · Apache Spark · Structured Streaming · Spark Declarative Pipelines · Delta Lake · Medallion Architecture · One Big Table · CDC · SCD Type 1 · SCD Type 2 · Dimensional Modeling · Star Schema · Real-Time Data Engineering**

## Future Improvements

- Power BI dashboards
- Databricks SQL dashboards
- Data quality checks
- Pipeline monitoring
- Alerting
- Schema evolution
- Automated CI/CD
- Unit and integration testing
- Infrastructure as Code
- Azure Key Vault integration
- Production observability

## Author

Built as a hands-on **data engineering portfolio project** demonstrating practical experience with real-time streaming, Azure data services, Databricks, Apache Spark, Delta Lake, CDC, and analytical dimensional modeling.

## Disclaimer

This is a portfolio and learning project inspired by an Uber-style ride platform. It is not an official Uber system and does not use Uber proprietary data or infrastructure.
