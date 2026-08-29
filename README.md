<div align="right">
  🌐 <strong><a href="README.md">🇬🇧 English</a></strong> | <strong><a href="README_th.md">🇹🇭 ภาษาไทย</a></strong>
</div>

# FinnHub Streaming Data Pipeline

An end-to-end real-time data engineering pipeline that streams financial market trade events (stocks and cryptocurrencies) from the **Finnhub WebSocket API** (or simulates them in Mock Mode), stores them in **ClickHouse**, performs batch transformations using **dbt**, schedules models via **Apache Airflow**, and visualizes trading metrics in **Looker Studio (Google Data Studio)**.

## Architecture

```mermaid
graph TD
    A[Finnhub WebSocket API / Mock Generator] -->|Stream JSON Trades| B(Python Streamer)
    B -->|Batch Insert| C[(ClickHouse: raw.trades)]
    D[Apache Airflow] -->|Orchestrate| E[dbt seed / run / test]
    E -->|Clean & Cast Types| F[(ClickHouse: analytics.stg_trades)]
    E -->|Core Models| G[(ClickHouse: analytics.dim_assets & fct_trades)]
    E -->|Aggregate OHLCV| H[(ClickHouse: analytics.mart_ohlcv)]
    H -->|Expose via Tunnel| I[Looker Studio Dashboard]
```

## Features
- **Decoupled Architecture**: Strictly separates the ingestion engine (standalone Python daemon) from transformations (dbt scheduled by Airflow) to prevent pipeline lockups and ClickHouse performance drops.
- **Dual Ingestion Mode**: Stream real-time data using a Finnhub API key or automatically fallback to a robust **Mock Trade Generator** if no key is provided.
- **Star Schema Modeling**: Implements a professional dimensional modeling design with fact (`fct_trades`) and dimension (`dim_assets`) tables.
- **Incremental Processing**: Configured dbt incremental loads for high-frequency transactional data to minimize compute resource usage.
- **ClickHouse Optimization**: Uses buffered batch inserts to maximize ClickHouse write performance and utilizes native ClickHouse functions like `argMin`/`argMax` for fast aggregates.
- **Fully Containerized**: PostgreSQL (Airflow backend), ClickHouse, Airflow Scheduler/Webserver, and the Python Streamer run in Docker Compose.

---

## Getting Started

### 1. Prerequisites
- Docker & Docker Compose installed.
- (Optional) A free API key from [Finnhub.io](https://finnhub.io/).
- (Optional) [ngrok](https://ngrok.com/) installed on your host machine to connect Looker Studio to your local ClickHouse.

### 2. Running the Pipeline
Clone the repository and run:

```bash
docker-compose up --build -d
```

If you have a Finnhub API Key, create a `.env` file in the root directory first:
```env
FINNHUB_API_KEY=your_api_key_here
```

### 3. Verify Container Status
Check that all containers are healthy:
```bash
docker-compose ps
```

Access the Airflow UI:
- **URL**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`

Trigger the `finnhub_dbt_run` DAG to execute transformations.

---

## Querying the Data in ClickHouse

To verify raw trade events are streaming into ClickHouse:
```bash
docker exec -it sharp-maxwell-clickhouse-1 clickhouse-client --query "SELECT * FROM raw.trades LIMIT 10"
```

To check the transformed minutely OHLCV data:
```bash
docker exec -it sharp-maxwell-clickhouse-1 clickhouse-client --query "SELECT * FROM analytics.mart_ohlcv LIMIT 10"
```

---

## Architectural Design: Separation of Components

This pipeline employs a highly decoupled architecture following production-grade best practices, specifically addressing potential anti-patterns in data pipeline design:

### 1. Decoupled Ingestion vs. Transformation
- **The Ingestion Layer** runs continuously as a standalone Python daemon (`streamer` service). It reads from the WebSocket API, buffers incoming ticks in memory, and writes micro-batches to ClickHouse raw storage. 
- **The Transformation Layer** runs as a batch dbt project.
- **Why separate them?** Chaining ingestion and dbt run sequentially in the same Airflow DAG (e.g. running dbt every time a chunk of data is ingested) is an anti-pattern. If ingestion runs frequently (e.g. streaming or every few minutes), triggering dbt run on every write creates massive, redundant database load and will exhaust scheduler resources. By separating them, we allow ClickHouse to ingest raw trades continuously in real-time, while Airflow triggers dbt on an hourly or daily schedule to build analytical tables asynchronously.

### 2. Streaming Outside Airflow
- **Why not run ingestion inside Airflow?** Running a long-running WebSocket subscription or streaming daemon inside an Airflow worker task (e.g., using PythonOperator) is an anti-pattern and overkill. Airflow is designed to coordinate short-lived, stateless, batch task processes. Running a continuous streaming job inside Airflow locks up workers, violates task isolation, makes scheduler health checks fragile, and leads to unstable execution. Instead, the streamer runs as a dedicated, lightweight Docker container, completely external to Airflow.

---

## Business Questions & Dashboard Metrics Mapping

To ensure the Looker Studio dashboard directly answers key business questions, our dbt mart models and visualizations are explicitly mapped as follows:

| Business Question | Dashboard Metric / Chart | SQL Implementation Details |
| :--- | :--- | :--- |
| **1. What is the total volume and transaction count of each asset?** | **Total Volume** & **Trade Count** KPI cards and bar charts. | Sums volume (`sum(volume)`) and counts transactions (`count()`) within each minute window. |
| **2. What is the price movement trend and rate?** | **OHLCV Candlestick Chart** & **Average Price Line Chart**. | Computes opening, highest, lowest, and closing prices for each minute window. |
| **3. How volatile/diverse is the trading behavior?** | **Price Spread** & **Relative Spread** Trend Charts. | Computes the absolute spread (`high - low`) and the relative spread (`spread / open`) for each window. This gauges the variance and diversity of market price fluctuations. |

---

## Connecting Looker Studio

Since Looker Studio is a cloud service, you can expose your local ClickHouse server HTTP port (`8123`) using **ngrok**:

1. Run ngrok on your host:
   ```bash
   ngrok http 8123
   ```
2. Copy the public forwarding URL (e.g., `https://xxxx-xx-xx.ngrok-free.app`).
3. Open [Looker Studio](https://lookerstudio.google.com/), click **Create Data Source**, and search for the **ClickHouse Connector** by ClickHouse.
4. Configure the connection using the ngrok URL:
   - **Host**: `xxxx-xx-xx.ngrok-free.app` (exclude the `https://`)
   - **Port**: `443` (since ngrok uses HTTPS)
   - **User**: `default`
   - **Password**: (leave blank)
   - **Database**: `analytics`
   - **Table**: `mart_ohlcv`

### Dashboard Visualization
Here is a preview of the Looker Studio Dashboard showing stock price trends, trading volumes, and transaction counts:

![Looker Studio Dashboard Mockup](dashboard_mockup_th.png)

---

## Homework Answers

### 1. What did you learn from this project?
- **Real-Time Data Streaming & Ingestion**: Learned how to implement a WebSocket client in Python that maintains a persistent connection, handles reconnections, buffers streaming trade data, and loads it to ClickHouse in micro-batches to prevent high IO overhead.
- **ClickHouse Performance**: Realized ClickHouse's power for analytical processing. Instead of performing slow joins and row-by-row updates, I used ClickHouse's native `argMin` and `argMax` functions to calculate `Open` and `Close` prices in `GROUP BY` aggregates in seconds.
- **dbt ClickHouse Adapter**: Gained experience utilizing the `dbt-clickhouse` adapter, managing staging views and table-materialized marts, and using Airflow to orchestrate the pipeline stages automatically.

### 2. How would you improve it?
- **Decouple with Message Broker (Kafka)**: In production, direct writes from a WebSocket client to ClickHouse are risky (e.g. if ClickHouse is temporarily unavailable). Introducing a message broker like Apache Kafka or Redpanda would queue raw events, add durability, and allow other microservices to consume the data.
- **dbt Incremental Models**: Instead of doing full table refreshes (`+materialized: table`), transform the dbt models into `incremental` tables using the `insert_overwrite` or `append` strategies to only process new trades since the last run.
- **Schema Validation & Registry**: Use a Schema Registry (like Confluent's) and enforce schemas (e.g. Avro or Protobuf) to handle API contract changes gracefully.

### 3. If you have to do it all over again, what would you do differently?
- **Use ClickHouse Materialized Views**: Instead of scheduling batch dbt models on a schedule via Airflow, I would define ClickHouse **Materialized Views** directly on the raw tables. ClickHouse Materialized Views compute aggregations (like OHLCV) *on insert*, turning this into a true real-time, zero-batch-overhead pipeline.
- **Use an Ingestion Agent**: Instead of writing a custom Python daemon, I would consider using lightweight agents like **Vector** or **Benthos** to read from the WebSocket and write directly to ClickHouse, reducing the amount of custom code to maintain.
