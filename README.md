# Financial Data Engineering Pipeline

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python)](https://www.python.org/)

![Insalogo](./images/logo-insa_0.png)

**Course**: [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) @ [INSA Lyon](https://www.insa-lyon.fr/)

**Team Members**:

- Adrian Abi Saleh
- Romain Poirrier
- Aymerick Yzidee

---

## 📋 Table of Contents

- [Abstract](#abstract)
- [Architecture Overview](#architecture-overview)
  - [Data Flow Architecture](#data-flow-architecture)
  - [Technology Stack](#technology-stack)
- [Project Design by Layer](#project-design-by-layer)
  - [1. Ingestion Layer](#1-ingestion-layer)
  - [2. Staging Layer](#2-staging-layer)
  - [3. Production Layer](#3-production-layer)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation & Setup](#installation--setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Accessing Services](#accessing-services)
- [Connection Guides](#connection-guides)
- [Key Considerations](#key-considerations)
- [Troubleshooting](#troubleshooting)
- [Datasets Description](#datasets-description)
- [Example Queries](#example-queries)
- [Licenses](#licenses)
- [Contributing](#contributing)

---

## Abstract

This project implements a comprehensive data engineering pipeline for financial market analysis, integrating multiple data sources including cryptocurrency, forex, futures, indices data from Yahoo Finance, and worldwide geopolitical events from UCDP (Uppsala Conflict Data Program). The architecture follows a three-layer approach: **Ingestion**, **Staging**, and **Production**, orchestrated by Apache Airflow and supported by a multi-database ecosystem.

The pipeline is designed to handle both online and offline data acquisition modes, ensuring reproducibility and resilience. Metadata flows through Redis queues between pipeline stages, with MongoDB serving as the raw data lake, PostgreSQL as the structured analytical and transformation database, and a Streamlit web application for data visualization and insights.

## Architecture Overview

The project implements a **Multi-Stage Architecture** pattern with three distinct processing layers, ensuring separation of concerns and scalable data processing.

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
- **Networks**: `staging`

#### Database Schema:

The PostgreSQL database implements a normalized schema for financial and geopolitical data:

**Core Entities**:

- `Index_Exchange`, `Forex_Exchange`, `Futures_Exchange`, `Crypto_Exchange`: General informations about the assets
- `Index_History`, `Forex_History`, `Futures_History`, `Crypto_History`: Monthly financial informations of the assets
- `UCDP_Conflict`: Yearly conflicts from UCDP
- `UCDP_Actors`: Actors used in UCDP datasets
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
2. Shape tables into facts and dimension made to simplify queries

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

**Redis-1** (Port 6379): Ingestion → Staging communication

**Configuration**: [`config/redis/redis.conf`](config/redis/redis.conf)

---

## Getting Started

### Quick Start

**TL;DR** - Already have Docker installed?

```bash
cp env.temp .env && ./runner.sh up
```

Access Airflow at http://localhost:8080 (admin/admin) and trigger the `ingestion_pipeline` DAG.

**For first-time setup**, follow the detailed instructions **below ↓**

### Prerequisites

Before running this project, ensure you have:

- **Docker Desktop** installed and running
  - [Download Docker Desktop](https://www.docker.com/products/docker-desktop)
- **System Requirements**:
  - Minimum 4GB RAM available
  - Minimum 2 CPU cores
  - At least 10GB free disk space
- **Optional**: Internet connection for online data fetching (offline mode is also supported)

### Installation & Setup

#### 1. Clone the Repository

```bash
git clone <repository-url>
cd Data_Engineering_Project_2025
```

#### 2. Environment Configuration

The project requires environment variables to be configured. A template file `env.temp` is provided in the repository.

**Create your `.env` file from the template**:

```bash
cp .env.temp .env
```

> **Note**: The `.env` file is git-ignored for security. You must create it from `env.temp` before running the project.

**Review and modify** the `.env` file if needed. Default credentials are suitable for development:

```dotenv
# PostgreSQL
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DB=stock

# MongoDB
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=root

# Redis
REDIS_1_USERNAME=crud
REDIS_1_PASSWORD=crud
```

> ⚠️ **Security Warning**: Change default credentials for production deployments!

-> For production deployments, you should change the credentials of the Databases AND Airflow.

**⚠️ Airflow Warning**

If you have problem with Airflow not showing DAGs, you might want to consider changing the ``AIRFLOW_UID`` to your user and restart the project.

Get your user id : 
```bash
id -u
```

Run a full restart: 
```bash
sudo ./runner.sh full_restart
```


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
   -> This command might require ``sudo`` to work.

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

#### Configuring Airflow PostgreSQL Connection

After starting the services using `.env` and `runner.sh`, you need to configure the PostgreSQL connection in Airflow:

1. Navigate to **Airflow UI** at http://localhost:8080
2. Login with credentials: **admin** / **admin**
3. Click on **Admin** ⚙️ in the navigation bar
4. Select **Connections** from the dropdown menu
5. Click the **Add a new connection** button
6. Configure the connection with the following settings:
   - **Connection Id**: `postgres`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres-db`
   - **Schema**: `stock`
   - **Login**: `admin`
   - **Password**: `admin`
   - **Port**: `5432`
7. Click **Save**

> **Note**: These credentials match the values defined in your `.env` file:
>
> ```dotenv
> POSTGRES_USER=admin
> POSTGRES_PASSWORD=admin
> POSTGRES_DB=stock
> POSTGRES_HOST=postgres-db
> ```

#### Running the Ingestion DAG

Once all services are running and the PostgreSQL connection is configured, **unpause the three DAGs (toggle them to "On" if paused)** and then trigger the data ingestion pipeline:

1. Navigate to **Airflow UI** at http://localhost:8080
2. Login with credentials: **admin** / **admin**
3. Add the Postgres connection, with the Postgres credentials in your ``.env`` file, and ``postgres`` as connection id and ``postgres-db`` as host.
4. Find the DAG named `ingestion_pipeline`
5. Toggle the DAG to **"On"** (if paused)
6. Click the **"Play"** button → **"Trigger DAG"**
7. Monitor progress in the **Grid** or **Graph** view

> **Note**: To run the pipeline in offline mode, please turn off the wifi connection on your pc, if you do, you need to be connected to internet to add the Postgres connection first, once the connection is made you can turn off your internet.

The pipeline will automatically:

- Detect internet connectivity (online vs offline mode)
- Fetch financial data from Yahoo Finance or use local cache
- Download UCDP conflict datasets
- Store raw data in MongoDB
- Populate Redis queues for downstream processing

**Expected Runtime**: 5-15 minutes depending on network speed and system resources.

---

## Accessing Services

After starting the containers, access the following UIs:

| Service                  | URL                   | Credentials                     |
| ------------------------ | --------------------- | ------------------------------- |
| **Airflow**              | http://localhost:8080 | admin / admin                   |
| **PgAdmin** (PostgreSQL) | http://localhost:5050 | admin@admin.com / admin         |
| **Redis Insight**        | http://localhost:5540 | (Add database manually)         |
| **Streamlit**            | http://localhost:8501 | No authentication required      |
| **MongoDB**              | localhost:27017       | root / root (admin auth)        |
| **PostgreSQL**           | localhost:5432        | admin / admin (database: stock) |

> **Note**: Default credentials are configured in the `.env` file. Change them for production use!

---

## Connection Guides

### Connecting to PostgreSQL via PgAdmin

1. Access PgAdmin at http://localhost:5050
2. Login with `admin@admin.com` / `admin`
3. Right-click **"Servers"** → **"Register"** → **"Server"**
4. **General Tab**:
   - Name: `Financial Data DB`
5. **Connection Tab**:
   - Host: `postgres-db`
   - Port: `5432`
   - Maintenance database: `stock`
   - Username: `admin`
   - Password: `admin`
6. Click **"Save"**

### Connecting to MongoDB

**Via Command Line**:

```bash
# Access the container
docker exec -it mongo bash

# Connect with mongosh
mongosh "mongodb://crud:crud@localhost:27017/?authSource=raw_data_db"

# Switch to the stocks database
use raw_data_db

# Query data
db.ingestion.find({type: "crypto"}).limit(5)
```

**Via MongoDB Compass**:

- Connection string: `mongodb://crud:crud@localhost:27017/?authSource=raw_data_db`

### Connecting to Redis

**Via Redis Insight**:

1. Access Redis Insight at http://localhost:5540
2. Click **"Add Redis Database"**
3. Select **"Connect to a Redis database"**
4. Enter connection details:
   - **Host**: `redis-1`
   - **Port**: `6379`
   - **Database Alias**: `Ingestion Redis`
   - **Username**: `crud`
   - **Password**: `crud`
5. Click **"Add Redis Database"**

**Via CLI**:

```bash
# Access Redis-1 (Ingestion → Staging)
docker exec -it redis-1 redis-cli -a crud

# Check queue length
LLEN crypto_info_queue

# Peek at queue items (without removing)
LRANGE crypto_info_queue 0 -1

# Pop item from queue
LPOP crypto_info_queue
```

---

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
- Additional data sources integration
- Enhanced data quality checks

---

## Troubleshooting

### Common Issues

1. **Airflow services fail to start**:

   - Check system resources (minimum 4GB RAM, 2 CPUs)
   - Verify `.env` file exists: `ls -la .env`
   - Ensure `.env` is properly formatted (no syntax errors)
   - Run `./runner.sh full_restart` to reset everything

2. **"No such file or directory" errors**:

   - Ensure `.env` file exists: `cp env.temp .env`
   - Check all required directories exist
   - Check file permissions (especially on Linux)
   - Verify `AIRFLOW_UID` in `.env` matches your system user ID: `echo $UID`

3. **MongoDB connection refused**:

   - Wait for MongoDB health check to pass (~30 seconds after startup)
   - Verify credentials in `.env` match MongoDB configuration
   - Check MongoDB logs: `docker logs mongo`
   - Ensure MongoDB container is running: `docker ps | grep mongo`

4. **Tasks stuck in "queued" state**:

   - Check Celery worker logs: `docker logs airflow-worker`
   - Verify Redis is healthy: `docker ps | grep redis`
   - Restart Airflow: `./runner.sh restart`

5. **Offline mode not working**:
   - Ensure offline data files exist in `./offline` directory
   - Verify file paths in `.env` match actual file locations
   - Check file permissions inside containers
   - Check that your PC is disconnected from the internet

### Need More Help?

- Check Docker container logs: `docker logs <container-name>`
- Review Airflow logs in : `/metadata/airflow/logs`
- Ensure all prerequisites are met and `.env` file is properly configured

---

## Datasets Description

### Yahoo Finance Data

**Asset Categories**:

- **Cryptocurrencies**: Bitcoin (BTC-USD), Ethereum (ETH-USD)
- **Forex**: 23 major currency pairs (EUR/USD, GBP/USD, USD/JPY, etc.)
- **Futures**: 37 commodity and index futures contracts
- **Indices**: 40 global stock market indices (S&P 500, FTSE, DAX, etc.)

**Data Coverage**:

- **Timeframe**: 20 years of historical data
- **Granularity**: Monthly intervals
- **Data Points**: Price (Open, High, Low, Close), Volume, Dividends, Stock Splits

**API**: [Yahoo Finance](https://finance.yahoo.com/) via `yfinance` Python library

### UCDP (Uppsala Conflict Data Program)

**Datasets**:

1. **UCDP/PRIO Armed Conflict Dataset**: Information on armed conflicts (1989-2024)
2. **UCDP Actor Dataset**: Details on conflict actors and organizations
3. **UCDP Georeferenced Event Dataset**: Geographic coordinates of conflict events

**Coverage**:

- **Geographic**: Worldwide
- **Temporal**: 1989 - 2024 (updated annually)
- **Event Types**: State-based conflicts, non-state conflicts, one-sided violence

**Source**: [Uppsala Conflict Data Program](https://ucdp.uu.se/)

**License**: Free for academic use with proper attribution

---

## Example Queries

This pipeline enables sophisticated analytical queries that correlate geopolitical events with financial market behavior:

### 1. Regional Market Response to Conflict Episode Onset

**Objective**: Analyze how regional markets react to the start of conflict episodes by examining market values from 3 months before to 3 months after episode initiation.

**Use Case**: Understand market volatility patterns when conflicts begin. For example, this query reveals how European indices (DAX, FTSE, CAC 40) responded to the onset of the Ukraine conflict in 2022, showing the immediate market impact and subsequent stabilization or decline.

**Data Required**:

- Conflict episode start dates and geographic regions
- Regional market indices historical data (monthly granularity)
- Time window: -3 months to +3 months from episode start

**Example Application**: "How did European equity markets respond to the Ukraine conflict onset compared to Middle Eastern conflicts?"

### 2. US Market Reaction Based on Maximum Involvement Role

**Objective**: Measure how US markets react based on the United States' highest level of involvement in conflict episodes (primary vs. secondary actor).

**Use Case**: When the US participates in multiple conflict episodes simultaneously with different roles, this query identifies the maximum involvement level (e.g., if the US is a primary actor in one episode and secondary in another, the primary role is used). This helps correlate market behavior with US foreign policy engagement intensity.

**Data Required**:

- US involvement classification (primary/secondary actor) per episode
- US market indices (S&P 500, Dow Jones, NASDAQ)
- Episode temporal boundaries

**Example Application**: "Do US markets react differently when the US is a primary combatant versus a secondary supporter in armed conflicts?"

### 3. S&P 500 Trend Analysis: New vs. Ongoing Episodes

**Objective**: Identify S&P 500 price trends based on whether new conflict episodes are starting versus periods with only ongoing episodes or no active conflicts.

**Use Case**: Determine if market trends differ when new geopolitical tensions emerge compared to the continuation of existing conflicts. This helps investors understand if markets price in conflict risk differently during escalation versus sustained conflict periods.

**Data Required**:

- Episode initiation dates and status (new/ongoing)
- S&P 500 monthly close prices
- Time-series aggregation of episode counts (new vs. ongoing)

**Example Application**: "Does the S&P 500 exhibit bearish trends during months with new conflict episodes compared to months with only ongoing or no conflicts?"

---

### Query Execution

These queries can be executed through:

- **Streamlit Dashboard**: Interactive visualizations with parameter selection
- **PostgreSQL Direct**: Custom SQL queries on the production schema for advanced analysis

> **💡 Recommendation**: For the most comprehensive and reliable results, run the ingestion pipeline in **online mode** with an active internet connection. This ensures you have the latest financial data and conflict datasets, providing more accurate correlations and up-to-date insights. The offline mode is suitable for testing and demonstrations but may contain limited or outdated sample data.

---

## Licenses

### Project License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.

### Third-Party Software Licenses

This project uses the following open-source software:

#### Core Infrastructure

| Software                                      | License                           | Purpose                                           |
| --------------------------------------------- | --------------------------------- | ------------------------------------------------- |
| [Apache Airflow](https://airflow.apache.org/) | Apache 2.0                        | Workflow orchestration and scheduling             |
| [PostgreSQL](https://www.postgresql.org/)     | PostgreSQL License                | Relational database for structured data           |
| [MongoDB](https://www.mongodb.com/)           | SSPL (Server Side Public License) | Document database for raw data storage            |
| [Redis](https://redis.io/)                    | BSD 3-Clause                      | In-memory data structure store and message broker |
| [Docker](https://www.docker.com/)             | Apache 2.0                        | Containerization platform                         |

#### Python Libraries

| Library                                            | License      | Purpose                                     |
| -------------------------------------------------- | ------------ | ------------------------------------------- |
| [yfinance](https://github.com/ranaroussi/yfinance) | Apache 2.0   | Yahoo Finance data fetching                 |
| [Streamlit](https://streamlit.io/)                 | Apache 2.0   | Web application framework for visualization |
| [pandas](https://pandas.pydata.org/)               | BSD 3-Clause | Data manipulation and analysis              |
| [pymongo](https://pymongo.readthedocs.io/)         | Apache 2.0   | MongoDB Python driver                       |
| [psycopg2](https://www.psycopg.org/)               | LGPL v3      | PostgreSQL adapter for Python               |
| [redis-py](https://github.com/redis/redis-py)      | MIT          | Redis Python client                         |
| [celery](https://docs.celeryq.dev/)                | BSD 3-Clause | Distributed task queue                      |
| [plotly](https://plotly.com/)                      | MIT          | Data visualisation                          |
| [matplotlib](https://matplotlib.org/)              | Custom based on PSF | Data visualisation                   |

#### Additional Tools

| Tool                                                               | License                           | Purpose                             |
| ------------------------------------------------------------------ | --------------------------------- | ----------------------------------- |
| [pgAdmin](https://www.pgadmin.org/)                                | PostgreSQL License                | PostgreSQL administration interface |
| [Redis Insight](https://redis.com/redis-enterprise/redis-insight/) | Server Side Public License (SSPL) | Redis GUI and analytics             |

### Data Sources Licenses

| Data Source   | License/Terms                                                                 | Attribution Required             |
| ------------- | ----------------------------------------------------------------------------- | -------------------------------- |
| Yahoo Finance | [Terms of Service](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html) | Data for personal/academic use   |
| UCDP Dataset  | [Creative Commons BY 4.0](https://creativecommons.org/licenses/by/4.0/)       | Yes - Academic citation required |

**UCDP Citation**:

```
Pettersson, Therese & Magnus Öberg (2020) Organized violence, 1989-2024.
Journal of Peace Research 57(4).
Davies, Shawn, Therese Pettersson & Magnus Öberg (2025).
Organized violence 1989-2024 and drone warfare.
Journal of Peace Research 60(4).
```

### Compliance Notes

- All third-party software is used in compliance with their respective licenses
- This project does not modify the core functionality of Apache Airflow, PostgreSQL, MongoDB, or Redis
- Yahoo Finance data is used for educational and non-commercial purposes only
- UCDP data usage complies with their academic use policy and attribution requirements

For commercial use of this pipeline, please review the licenses of all dependencies and ensure compliance with data source terms of service.

---

## Contributing

We welcome contributions to improve this project! Here's how you can help:

### How to Contribute

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make your changes** and test thoroughly
4. **Commit your changes**: `git commit -m 'Add some feature'`
5. **Push to the branch**: `git push origin feature/your-feature-name`
6. **Open a Pull Request**

### Guidelines

- Follow existing code style and conventions
- Add tests for new features
- Update documentation (README, code comments) as needed
- Ensure all services start and run correctly after your changes
- Test both online and offline modes if modifying ingestion logic

### Reporting Issues

Found a bug or have a suggestion? Please open an issue on GitHub with:

- Clear description of the problem or feature request
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- System information (OS, Docker version, etc.)

---

## Acknowledgments

- **INSA Lyon** and **Prof. Riccardo Tommasini** for the Data Engineering course
- **Yahoo Finance** for providing free financial data API
- **Uppsala Conflict Data Program** for maintaining comprehensive conflict datasets
- **Apache Software Foundation** for Airflow and other open-source tools
- All contributors and open-source maintainers of the libraries used in this project

---

**Project Status**: Active Development | **Last Updated**: January 2026
