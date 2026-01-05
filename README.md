# Financial Data Engineering Pipeline

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students:

- Adrian Abi Saleh
- Romain Poirrier
- Aymerick Yzidee

## Abstract

This project implements a comprehensive data engineering pipeline for financial market analysis, integrating multiple data sources including cryptocurrency, forex, futures, indices data from Yahoo Finance, and worldwide geopolitical events from UCDP (Uppsala Conflict Data Program). The architecture follows a three-layer approach: **Ingestion**, **Staging**, and **Production**, orchestrated by Apache Airflow and supported by a multi-database ecosystem.

The pipeline is designed to handle both online and offline data acquisition modes, ensuring reproducibility and resilience. Metadata flows through Redis queues between pipeline stages, with MongoDB serving as the raw data lake, PostgreSQL as the structured analytical and transformation database, and a Streamlit web application for data visualization and insights.

## Architecture Overview

The project implements a Multi Stage Architecture pattern with three distinct processing layers:

### Data Flow Architecture

![Data Flow](./images/report/Project_architecture.png)

### Technology Stack

- **Orchestration**: Apache Airflow (Celery Executor)
- **Data Storage**:
  - MongoDB: Raw/semi-structured data storage (Ingestion layer)
  - PostgreSQL: Structured relational storage (Staging layer)
  - Redis: Message queuing and inter-layer communication
- **Visualization**: Streamlit web application
- **Data Sources**: Yahoo Finance API, UCDP CSV datasets
- **Containerization**: Docker & Docker Compose

## Project Design by Layer

### 1. Ingestion Layer

**Purpose**: Acquire raw data from external sources and store in MongoDB with minimal transformation.

#### Components:

- **Airflow DAG**: [`src/dags/ingestion.py`](src/dags/ingestion.py)
- **Data Store**: MongoDB (`mongo-db` container)
- **Queue**: Redis-1 (`redis-1` container)
- **Networks**: `ingestion`

#### Data Sources:

1. **Financial Assets** (Yahoo Finance API):

   - Cryptocurrencies (BTC-USD, ETH-USD)
   - Forex pairs (23 major currency pairs)
   - Futures contracts (37 commodity/index futures)
   - Stock indices (40 global market indices)

2. **Geopolitical Data** (UCDP):
   - Worldwide conflict events
   - Actor information
   - Georeferenced events

#### Process Flow:

1. **Mode Detection**:

   - Checks internet connectivity
   - Routes to **online** or **offline** processing path

2. **Online Mode**:

   - Fetches asset symbols (used to be fetched from the web by scrapping, but are hardcoded now because of lack of reliability due to web pages updates)
   - Queries Yahoo Finance in batches of 5 symbols to enable parallel processing and minimize data loss if individual tasks fail. For each symbol, the pipeline retrieves:

     - Asset information (company details, market data)
     - Historical prices (20 years of monthly data)

   - Downloads UCDP CSV files from URLs
   - Extracts zip archives

3. **Offline Mode**:

   - Reads pre-downloaded JSON files from `/offline` directory
   - Processes local UCDP zip files

4. **Data Storage**:

   - Inserts documents into MongoDB collection with structure:
     ```json
     { // metadata
       "_id": "asset_type_symbol_info_type",
       "type": "crypto|forex|futures|indices",
       "symbol": "BTC-USD",
       "information_type": "info|history",
       "extracted_at": "2026-01-05T...",
       "data": { ... } // actual data
     }
     ```

5. **Queue Population**:
   - Pushes metadata (without raw data) to Redis-1 queues:
     - `CRYPTO_INFO_QUEUE`, `CRYPTO_HISTORY_QUEUE`
     - `FOREX_INFO_QUEUE`, `FOREX_HISTORY_QUEUE`
     - `FUTURES_INFO_QUEUE`, `FUTURES_HISTORY_QUEUE`
     - `INDICES_INFO_QUEUE`, `INDICES_HISTORY_QUEUE`
     - `FILES_QUEUE` (for CSV files)

#### Key Features:

- **Chunked Processing**: Symbols processed in batches of 5 for parallel execution
- **Memory Management**: Explicit garbage collection after each symbol
- **Idempotent Writes**: MongoDB upserts prevent duplicate data
- **Airflow Dataset**: Signals staging layer when data is ready

### 2. Staging Layer

**Purpose**: Transform and normalize raw data into structured relational format.

#### Components:

- **Airflow DAG**: [`src/dags/staging.py`](src/dags/staging.py) (placeholder)
- **Data Store**: PostgreSQL (`postgres-db` container)
- **Input Queue**: Redis-1
- **Output Queue**: Redis-2
- **Networks**: `staging`

#### Database Schema:

The PostgreSQL database implements a normalized schema for financial and geopolitical data:

**Core Entities**:

- `Index_Exchange`, `Forex_Exchange`, `Futures_Exchange`, `Crypto_Exchange`: General informations about the assets
- `Index_History`, `Forex_History`, `Futures_History`, `Crypto_History`: Monthly financial informations of the assets
- `UCDP_Conflict`: Yearly conflicts from UCDP
- `UCDP_Acto`: Actors used in UCDP datasets
- `UCDP_GEOREFERENCES`: Events referenced with geo localisation, down to each battle

**Key Relationships**:

- Assets history are linked to Asset informations
- Conflicts contain episodes
- Actors are linked to episodes of conflicts, either as primary/secondary party
  - Actors take a side in an episode of a conflict (ex: Side A or B)
- Geo referenced events are linked to conflicts

#### Planned Process Flow:

1. Poll Redis-1 queues for new metadata & files paths
2. Retrieve raw documents from MongoDB or CSV files
3. Insert raw data transformed to fit the Postgres DB
4. Run multiple SQL queries in a "dbt" way to transform the data
   1. New tables get created for each transformation model
5. Trigger production when staging ends

### 3. Production Layer

**Purpose**: Serve analytical queries and power visualization applications.

#### Components:

- **Data Consumers**: Streamlit web application
- **Networks**: `production`

#### Streamlit Application:

- **Container**: `streamlit-webapp`
- **Port**: 8501
- **Features**: Interactive dashboards for market analysis and event correlation

#### Planned process flow

1. Load data from staging schema to production schema
2. Shape tables into facts and dimension made to simply queries

## Configuration

The project uses a comprehensive configuration system divided by service and environment.

### Directory Structure

```
config/
├── airflow/
│   └── airflow.cfg          # Airflow custom configuration
├── mongodb/
│   └── init.js              # Production MongoDB initialization
├── postgres/
│   └── init.sql             # Schema creation script
└── redis/
    └── redis.conf           # Redis configuration
```

### MongoDB Configuration

**File**: [`config/mongodb/init.js`](config/mongodb/init.js)

**Collections Created**:

- `ingestion`: Holds all the fininacial history and information

**Users**:

- User: `crud` (ReadWrite permissions on `raw_data_db` database)

### PostgreSQL Configuration

**File**: [`config/postgres/init.sql`](config/postgres/init.sql)

Automatically executed on container startup to create the normalized schema described in the Staging Layer section.

### Redis Configuration

**Two Redis Instances**:

1. **Redis-1** (Port 6380): Ingestion → Staging communication
2. **Redis-2** (Port 6381): Staging → Production communication

**Configuration**: [`config/redis/redis.conf`](config/redis/redis.conf)

## How to Run

### Prerequisites

- Docker Desktop installed and running
- Minimum 4GB RAM available
- Minimum 2 CPU cores
- At least 10GB free disk space
- (Optional) Internet connection for online data fetching

### Environment Setup

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd Data_Engineering_Project_2025
   ```

2. **Create Environment File**:

// TODO

### Running the Pipeline

The project includes a helper script [`runner.sh`](runner.sh) for common operations:

1. **Start All Services**:

   ```bash
   ./runner.sh up
   ```

   This command starts both Airflow and the data infrastructure.

2. **Check Service Status**:

   ```bash
   ./runner.sh status
   ```

3. **Stop All Services**:

   ```bash
   ./runner.sh down
   ```

4. **Restart Services**:

   ```bash
   ./runner.sh restart
   ```

5. **Full Reset** (removes all data):
   ```bash
   ./runner.sh full_restart
   ```
   ⚠️ **Warning**: This deletes all volumes, logs, and data!

### Manual Docker Compose Commands

Alternatively, use Docker Compose directly:

```bash
# Start services
docker compose -f docker-compose.airflow.yml -f docker-compose.yml up -d

# View logs
docker compose -f docker-compose.airflow.yml -f docker-compose.yml logs -f

# Stop services
docker compose -f docker-compose.airflow.yml -f docker-compose.yml down
```

### Accessing Services

After starting the containers, access the following UIs:

| Service                  | URL                   | Credentials                      |
| ------------------------ | --------------------- | -------------------------------- |
| **Airflow**              | http://localhost:8080 | admin / admin                    |
| **PgAdmin** (PostgreSQL) | http://localhost:5050 | admin@admin.com / admin          |
| **Redis Insight 1**      | http://localhost:5540 | redis://crud:crud@redis-1:6379/0 |
| **Streamlit**            | http://localhost:8501 | -                                |
| **MongoDB**              | localhost:27017       | admin / admin                    |
| **PostgreSQL**           | localhost:5432        | postgres / postgres              |

### Running the Ingestion Pipeline

1. Navigate to Airflow UI at http://localhost:8080
2. Login with credentials (admin/admin)
3. Find the DAG `ingestion_pipeline`
4. Toggle the DAG to "On" (if paused)
5. Click the "Play" button → "Trigger DAG"
6. Monitor progress in the Grid/Graph view

The pipeline will automatically:

- Detect internet connectivity
- Fetch financial data from Yahoo Finance (or use offline data)
- Download UCDP datasets
- Store raw data in MongoDB
- Populate Redis queues for downstream processing

## Connection Guides

### Connecting to PostgreSQL via PgAdmin

1. Access PgAdmin at http://localhost:5050
2. Login with credentials from `.env`
3. Right-click "Servers" → "Register" → "Server"
4. **General Tab**:
   - Name: `Financial Data DB`
5. **Connection Tab**:
   - Host: `postgres-db`
   - Port: `5432`
   - Maintenance database: `stock`
   - Username: `postgres`
   - Password: `postgres`
6. Click "Save"

### Connecting to MongoDB

**Via Command Line**:

```bash
# Access the container
docker exec -it mongo bash

# Connect with mongosh
mongosh "mongodb://admin:admin@localhost:27017/stocks?authSource=admin"

# Query data
use stocks
db.raw_data.find({type: "crypto"}).limit(5)
```

**Via MongoDB Compass**:

- Connection string: `mongodb://admin:admin@localhost:27017/stocks?authSource=admin`

### Connecting to Redis

**Via Redis Insight**:

1. Access Redis Insight 1 at http://localhost:5540
2. Add database:
   - Host: `redis-1`
   - Port: `6379`
   - Name: `Ingestion Redis`

**Via CLI**:

```bash
# Access Redis-1 (Ingestion)
docker exec -it redis-1 redis-cli

# Access Redis-2 (Staging)
docker exec -it redis-2 redis-cli

# Check queue length
LLEN crypto_info

# Peek at queue items (without removing)
LRANGE crypto_info 0 -1

# Pop item from queue
LPOP crypto_info
```

## Key Considerations

### 1. **Data Consistency**

- MongoDB uses upsert operations (`_id` based) to prevent duplicates
- Each document has a unique `_id` combining asset type, symbol, and information type
- Timestamps (`extracted_at`) track data freshness

### 2. **Scalability**

- **Chunked Processing**: Asset symbols processed in batches (configurable chunk size)
- **Parallel Tasks**: Airflow's dynamic task mapping enables concurrent processing
- **Queue-Based Decoupling**: Redis queues allow independent scaling of pipeline stages

### 3. **Resilience**

- **Offline Mode**: Complete functionality without internet using pre-downloaded data
- **Retry Mechanism**: Airflow tasks retry 3 times with 5-minute delays
- **Health Checks**: All services have Docker health checks

### 4. **Resource Management**

- Explicit garbage collection (`gc.collect()`) after processing each symbol
- YFinance cache configured to use `/tmp` to avoid permission issues
- Logging set to CRITICAL for yfinance to reduce noise

### 5. **Network Isolation**

Three separate Docker networks ensure security and organization:

- `ingestion`: Airflow worker ↔ MongoDB ↔ Redis-1
- `staging`: Airflow worker ↔ PostgreSQL ↔ Redis-1
- `production`: Airflow worker ↔ PostgreSQL ↔ Streamlit
- `airflow`: Internal Airflow component communication

### 6. **Volume Persistence**

Data persists across container restarts:

- `./data/mongo`: MongoDB data files
- `./data/postgres`: PostgreSQL data
- `./metadata/airflow`: Airflow logs and metadata
- `./shared_data`: Shared unzipped CSV files between ingestion and staging zones
- `./offline`: Offline data samples

### 7. **Airflow Executor**

Uses **CeleryExecutor** for distributed task execution:

- `airflow-scheduler`: Schedules DAG runs
- `airflow-worker`: Executes tasks (connected to all data networks)
- `airflow-triggerer`: Handles deferred tasks
- `redis-metadata-cache`: Celery broker

### 8. **Data Volume**

Expected data volumes per run:

- **Cryptocurrencies**: 2 symbols × 2 data types = 4 documents
- **Forex**: 23 symbols × 2 data types = 46 documents
- **Futures**: 37 symbols × 2 data types = 74 documents
- **Indices**: 40 symbols × 2 data types = 80 documents
- **UCDP Files**: 3 CSV files
- **Total**: ~200+ MongoDB documents + 3 large CSV files per ingestion run

### 9. **Yahoo Finance API Limitations**

- No official rate limits but may throttle aggressive requests
- Chunking and parallel processing balance speed vs. reliability
- Historical data limited to 20 years of monthly intervals to reduce size

### 10. **Future Enhancements**

- Incremental updates (only fetch new data)
- Monitoring and alerting with Prometheus/Grafana
- Other sources of data

## Note for Students

- Clone the created repository offline
- Add your name and surname into the README file and your teammates as collaborators
- Complete the field above after project is approved
- Make any changes to your repository according to the specific assignment
- Ensure code reproducibility and instructions on how to replicate the results
- Add an open-source license, e.g., Apache 2.0
- README is automatically converted into PDF

## Troubleshooting

### Common Issues

1. **Airflow services fail to start**:

   - Check system resources (minimum 4GB RAM, 2 CPUs)
   - Verify `.env` file exists and is properly formatted
   - Run `./runner.sh full_restart` to reset everything

2. **"No such file or directory" errors**:

   - Ensure all required directories exist
   - Check file permissions (especially on Linux)
   - Verify `AIRFLOW_UID` matches your system user

3. **MongoDB connection refused**:

   - Wait for MongoDB health check to pass (~30 seconds)
   - Verify credentials in `.env` match MongoDB initialization
   - Check MongoDB logs: `docker logs mongo`

4. **Tasks stuck in "queued" state**:

   - Check Celery worker logs: `docker logs airflow-worker`
   - Verify Redis is healthy: `docker ps | grep redis`
   - Restart Airflow: `./runner.sh restart`

5. **Offline mode not working**:
   - Ensure offline data files exist in `./offline` directory
   - Verify file paths in `.env` match actual file locations
   - Check file permissions inside containers

## License

Apache License 2.0

## Datasets Description

### Yahoo Finance Data

- **Cryptocurrencies**: Bitcoin (BTC-USD), Ethereum (ETH-USD)
- **Forex**: 23 major currency pairs (EUR/USD, GBP/USD, USD/JPY, etc.)
- **Futures**: 37 commodity and index futures contracts
- **Indices**: 40 global stock market indices (S&P 500, FTSE, DAX, etc.)
- **Timeframe**: 20 years of monthly historical data

### UCDP (Uppsala Conflict Data Program)

- **Events Dataset**: Armed conflict events worldwide (1989-2024)
- **Actors Dataset**: Conflict participants and organizations
- **Georeference Dataset**: Geographic coordinates of conflict locations
- **Source**: https://ucdp.uu.se/

## Queries

Example analytical queries enabled by this pipeline:

1. **Correlation Analysis**: How do geopolitical events correlate with market volatility?
2. **Asset Performance**: Which asset classes perform best during conflict periods?
3. **Geographic Impact**: How do regional conflicts affect local vs. global markets?
4. **Trend Analysis**: Long-term price trends across different asset types
5. **Event Clustering**: Identifying patterns in conflict events and market reactions

---

_Last Updated: January 2026_
