# Airflow ETL Pipeline Project

A complete ETL (Extract, Transform, Load) pipeline system built with Apache Airflow, featuring REST API access, web dashboard, and full Docker containerization.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Accessing Services](#accessing-services)
- [Running DAGs](#running-dags)
- [Common Commands](#common-commands)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Documentation](#documentation)

## 🎯 Overview

This project implements a comprehensive ETL pipeline for processing e-commerce data (customers, products, stores, sales, exchange rates) with the following capabilities:

- **5 Source DAGs**: Individual pipelines for each data source
- **1 Master Orchestrator**: Coordinates all pipeline executions
- **1 Reporting DAG**: Generates 9 analytical reports
- **REST API**: 13 endpoints for monitoring and querying
- **Web Dashboard**: Interactive UI for easy pipeline monitoring
- **Full Containerization**: 6 Docker services working together

### Data Processing

- **Input**: CSV files from `data/raw/dataset/`
- **Output**: PostgreSQL database (`etl_output` schema)
- **Processing**: Data cleaning, transformation, validation, and reporting
- **Volume**: ~47,000+ records processed across 5 tables

## ✨ Features

### Sprint Coverage (8 Sprints Completed)

- ✅ **Sprint 2**: Data Quality & Cleaning
- ✅ **Sprint 3**: Advanced Transformations
- ✅ **Sprint 4**: Database Loading Strategies
- ✅ **Sprint 5**: DAG Orchestration
- ✅ **Sprint 6**: Combined E-T-L Pipelines
- ✅ **Sprint 7**: REST API & Monitoring
- ✅ **Sprint 8**: Docker Deployment

### Key Capabilities

- 🔄 Event-driven DAG triggering with ExternalTaskSensor
- 📊 9 automated business reports (customer segmentation, sales trends, product performance, etc.)
- 🔍 Data quality validation with rejected records tracking
- 🎯 Incremental and full load strategies
- 🚀 Bulk operations with optimized chunking
- 📈 Real-time monitoring via REST API
- 🌐 User-friendly web dashboard
- 🗄️ Database visualization with pgAdmin

## 🔧 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker Desktop**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Git**: For cloning the repository
- **4GB RAM**: Minimum available memory for Docker
- **10GB Disk Space**: For images and volumes

### Check Installations

```powershell
# Check Docker
docker --version
docker-compose --version

# Check Docker is running
docker ps
```

## 🚀 Quick Start

### 1. Clone the Repository

```powershell
git clone https://github.com/KJoshiSaiGovind/Airflow_Amazon.git
cd Airflow_ETL
```

### 2. Prepare Data Files

Ensure your CSV data files are in the correct location:

```
data/raw/dataset/
├── Customers.csv
├── Products.csv
├── Stores.csv
├── Sales.csv
└── Exchange_Rates.csv
```

**First-Time Users**: If you don't have data files, the project includes sample data. Verify the files exist:

```powershell
ls data\raw\dataset
```

### 3. Start All Services

```powershell
cd Docker
docker-compose up -d
```

This single command will:

- Pull required Docker images (first time: ~5-10 minutes, downloads ~2GB)
- Create a PostgreSQL database with Airflow metadata schema
- Initialize Airflow database tables
- Start 15 services (Postgres, Redis, Airflow Webserver, Scheduler, REST API, pgAdmin, Grafana, Prometheus, Loki, etc.)
- Set up internal Docker networking
- Create persistent volumes for data storage

**What to Expect on First Run**:

```
[+] Running 16/16
 ✔ Network docker_etl-network         Created
 ✔ Container docker-postgres-1        Healthy
 ✔ Container docker-redis-1           Healthy
 ✔ Container docker-loki-1            Healthy
 ✔ Container docker-airflow-init-1    Exited (0)
 ✔ Container docker-webserver-1       Healthy
 ✔ Container docker-scheduler-1       Healthy
 ✔ Container docker-api-1             Healthy
 ✔ Container docker-grafana-1         Started
 ✔ Container docker-prometheus-1      Started
 ...
```

### 4. Wait for Initialization

**IMPORTANT**: First-time setup requires ~3-5 minutes for complete initialization:

- **Minute 1-2**: PostgreSQL database creation and Airflow schema setup
- **Minute 2-3**: Airflow webserver starting and DAG parsing
- **Minute 3-4**: All services becoming healthy
- **Minute 4-5**: DAGs appearing in Airflow UI

Check service status:

```powershell
docker-compose ps
```

All services should show `Up` with status `(healthy)`:

```
NAME                    STATUS
docker-webserver-1      Up (healthy)
docker-scheduler-1      Up
docker-postgres-1       Up (healthy)
docker-redis-1          Up (healthy)
...
```

If services show as `starting` or `unhealthy`, wait an additional 1-2 minutes.

### 5. Access the Airflow UI

Open your browser and navigate to:

- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`

**First Login**: The Airflow UI will show a list of DAGs. Initially, all DAGs will be paused (gray toggle). This is normal!

**Expected DAGs**:

- `etl_master_orchestrator` - Master pipeline coordinator
- `etl_customers` - Customer data pipeline
- `etl_products` - Product data pipeline
- `etl_stores` - Store data pipeline
- `etl_exchange_rates` - Exchange rate pipeline
- `etl_sales` - Sales transaction pipeline
- `etl_reports` - Report generation pipeline
- `etl_data_quality` - Data quality checks

### 6. Set Up PostgreSQL Admin Access (pgAdmin)

pgAdmin provides a graphical interface to view and query the PostgreSQL database.

**Step 6.1**: Open pgAdmin at http://localhost:5050

**Step 6.2**: Login with default credentials:

- **Email**: `admin@admin.com`
- **Password**: `admin`

**Step 6.3**: Register the PostgreSQL Server

1. In pgAdmin, right-click **"Servers"** in left sidebar
2. Select **"Register" → "Server..."**
3. Fill in the **General Tab**:

   - **Name**: `Airflow ETL Database` (or any name you prefer)
4. Fill in the **Connection Tab**:

   - **Host name/address**: `postgres` (this is the Docker service name)
   - **Port**: `5432` (internal Docker port, NOT 5434)
   - **Maintenance database**: `airflow`
   - **Username**: `airflow`
   - **Password**: `airflow`
5. Click **"Save"**

**Step 6.4**: Verify Connection

After saving, you should see:

```
Servers
└── Airflow ETL Database
    └── Databases (2)
        ├── airflow (Airflow metadata)
        └── postgres (system database)
```

**Step 6.5**: View ETL Data

1. Expand: **Servers → Airflow ETL Database → Databases → airflow → Schemas**
2. You'll see two schemas:

   - **public**: Airflow system tables (dag_run, task_instance, etc.)
   - **etl_output**: Your ETL data tables (customers, products, sales, etc.)
3. To view data:

   - Right-click on `etl_output.customers` → **View/Edit Data → First 100 Rows**
   - Or use the Query Tool: **Tools → Query Tool**, then run:
     ```sql
     SELECT * FROM etl_output.customers LIMIT 100;
     ```

**Troubleshooting pgAdmin Connection**:

- **Error "Unable to connect to server"**:
  - Make sure PostgreSQL container is healthy: `docker-compose ps postgres`
  - Use `postgres` as hostname (NOT `localhost`)
  - Use port `5432` (NOT 5434)
- **Access Denied**: Verify username/password are both `airflow`

### 7. Set Up Python Environment for Web Dashboard (Optional but Recommended)

The web dashboard provides a modern, user-friendly interface for monitoring all DAGs, viewing execution logs, and downloading processed data.

**Step 7.1**: Create Python Environment

```powershell
# If using Conda
conda create -n airflow_env python=3.11
conda activate airflow_env

# If using venv
python -m venv airflow_env
.\airflow_env\Scripts\Activate.ps1
```

**Step 7.2**: Install Dependencies

```powershell
# Navigate to project root
cd D:\sam\Projects\Infosys\Airflow

# Install requirements
pip install -r requirements.txt
```

This will install:

- Flask 3.1.0 (web framework)
- pandas 2.2.3 (data processing)
- requests 2.32.3 (API calls)
- python-dotenv 1.0.1 (config management)

**Step 7.3**: Start the Web Dashboard

```powershell
# Ensure you're in project root
cd D:\sam\Projects\Infosys\Airflow

# Start Flask dashboard
python scripts/api/web_dashboard.py
```

**Expected Output**:

```
 * Serving Flask app 'web_dashboard'
 * Debug mode: off
🚀 Airflow API Web Dashboard
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API Endpoint: http://localhost:8000
Dashboard URL: http://localhost:5000
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Dashboard Features:
  • DAG Overview & Status Monitoring
  • Real-time Task Execution Tracking
  • Interactive Log Viewer
  • Data Downloads (Bronze/Silver/Gold Layers)
  • Performance Metrics & Charts

Press Ctrl+C to stop the server
```

**Step 7.4**: Access the Web Dashboard

Open http://localhost:5000 in your browser.

**Dashboard Features**:

- **Overview Tab**: Summary of all DAGs with status
- **DAG Details**: Execution history and task breakdown
- **Logs Tab**: Real-time log streaming
- **Downloads Tab**: Download processed datasets and reports
- **Metrics Tab**: Performance analytics

**Keep Terminal Running**: The web dashboard requires the terminal to stay open. The REST API at http://localhost:8000 must also be running (it's part of the Docker stack).

### 8. Verify System Health

Before running pipelines, verify all components are working:

**Check 1**: Docker Services

```powershell
docker-compose ps
```

All services should be `Up` and `healthy`.

**Check 2**: Airflow UI

- Open http://localhost:8080
- Login successful
- DAGs visible in list

**Check 3**: REST API

```powershell
# Test API health
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

Should return: `{"status":"healthy","timestamp":"..."}`

**Check 4**: Database Connection

```powershell
# Test database from container
docker exec -it docker-postgres-1 psql -U airflow -d airflow -c "\dt etl_output.*"
```

Should show: "Did not find any relations" (tables will appear after first DAG run)

**Check 5**: Web Dashboard (if running)

- Open http://localhost:5000
- Should show empty state (no runs yet)

**All Checks Passed?** You're ready to trigger your first DAG! 🎉

### 9. Create Additional Airflow Users (Optional)

By default, the admin user has username/password `airflow`/`airflow`. You can create additional users:

```powershell
# Access Airflow container
docker exec -it docker-webserver-1 bash

# Create a new admin user
airflow users create \
  --username your_username \
  --firstname Your \
  --lastname Name \
  --role Admin \
  --email your.email@example.com

# You'll be prompted to enter a password
# Exit container
exit
```

**Available Roles**:

- **Admin**: Full access to all features
- **User**: Can view and trigger DAGs
- **Viewer**: Read-only access
- **Op**: Operations (manage connections, variables)

### 10. Trigger Your First Pipeline

You're now ready to execute the ETL pipeline!

**Option A: Using Airflow UI** (Recommended for beginners)

1. Open http://localhost:8080
2. Find `etl_master_orchestrator` in the DAG list
3. Click the **toggle switch** on the left to unpause the DAG (should turn blue/green)
4. Click the **"Play" button** (▶) on the right
5. Select **"Trigger DAG"**
6. Optional: Click **"Trigger DAG w/ config"** to add parameters
7. Click **"Trigger"** to start execution

**What Happens Next**:

- Master orchestrator starts
- Triggers 4 dimension DAGs in parallel: customers, products, stores, exchange_rates
- Waits for products to complete, then triggers sales (fact table)
- After all data loads, triggers reports DAG (generates 9 reports)
- Finally triggers data quality checks

**Expected Duration**: 5-8 minutes for complete pipeline execution

**Option B: Using Web Dashboard**

1. Open http://localhost:5000
2. Select `etl_master_orchestrator` from dropdown
3. Click **"Trigger DAG"** button
4. Monitor execution in real-time

**Option C: Using REST API**

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/trigger" `
  -Method POST `
  -Headers @{"X-API-Key"="dev-key-12345"}
```

### 11. Monitor Execution

**In Airflow UI**:

1. Click on `etl_master_orchestrator` DAG name
2. Click on the latest run (shown with execution date)
3. Click **"Graph"** view to see task dependencies
4. Green = Success, Red = Failed, Yellow = Running, Gray = Not started
5. Click any task → **"Log"** to see detailed execution logs

**In Web Dashboard**:

1. Go to http://localhost:5000
2. Select DAG from dropdown
3. View real-time status updates
4. Click **"View Logs"** for any task

**Common Things to Watch**:

- **Dimension DAGs** (customers, products, stores, exchange_rates) run in parallel - should all complete within 1-2 minutes
- **Sales DAG** waits for products completion - starts after dimensions finish
- **Reports DAG** generates 9 CSV reports in `data/reports/`
- **Data Quality DAG** runs quality checks and creates scorecard

### 12. View Results

After successful execution:

**Check Database Tables**:

```powershell
# Using pgAdmin (http://localhost:5050)
# Navigate to: airflow → Schemas → etl_output → Tables

# Or using psql
docker exec -it docker-postgres-1 psql -U airflow -d airflow
\dt etl_output.*
SELECT COUNT(*) FROM etl_output.customers;  -- Should show ~15,000 rows
SELECT COUNT(*) FROM etl_output.products;   -- Should show ~2,500 rows
SELECT COUNT(*) FROM etl_output.sales;      -- Should show ~26,000 rows
\q
```

**Check Generated Reports**:

```powershell
ls data\reports\
```

Expected files:

- `customer_summary.csv` - Customer statistics
- `customer_segmentation.csv` - RFM analysis
- `order_status.csv` - Order tracking
- `sales_trends_daily.csv` - Daily patterns
- `product_performance.csv` - Top products
- `store_performance.csv` - Store rankings
- `anomaly_detection.csv` - Outliers
- `data_quality_scorecard.csv` - Quality metrics
- `dag_execution_summary.csv` - Pipeline performance

**Download Reports from Dashboard**:

1. Go to http://localhost:5000
2. Click **"Downloads"** tab
3. Select **"Generated Reports"** category
4. Click **"Preview"** to view first 50 rows
5. Click **"Download"** to save locally

**Congratulations! 🎉** You've successfully set up and run your first ETL pipeline!

### 13. Understanding Your Setup

**Docker Architecture**:

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Network                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Airflow     │  │  Airflow     │  │  PostgreSQL  │  │
│  │  Webserver   │  │  Scheduler   │  │  Database    │  │
│  │  :8080       │  │              │  │  :5432       │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│         │                  │                  │          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  REST API    │  │  pgAdmin     │  │  Grafana     │  │
│  │  :8000       │  │  :5050       │  │  :3000       │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
          │
   ┌──────────────┐
   │ Flask Web    │
   │ Dashboard    │
   │ :5000        │
   └──────────────┘
```

**Data Flow**:

```
CSV Files (data/raw/dataset/)
       ↓
   Airflow DAGs Extract → Transform → Load
       ↓
PostgreSQL (etl_output schema)
       ↓
Reports Generated (data/reports/)
       ↓
Accessible via: pgAdmin | Web Dashboard | REST API
```

**File Locations**:

- **Source Data**: `data/raw/dataset/` (CSV files)
- **Staging Data**: `data/staging/` (intermediate processing)
- **Cleaned Data**: `data/processed/` (validated CSVs)
- **Reports**: `data/reports/` (business analytics)
- **Logs**: `logs/dag_id=*/` (execution logs)
- **DAGs**: `dags/` (pipeline definitions)
- **Scripts**: `scripts/` (ETL logic)
- **Config**: `config/` (YAML configurations)

### Next Steps

Now that your system is running:

1. **Explore the Airflow UI**: Try pausing/unpausing DAGs, viewing logs, checking task durations
2. **Query the Database**: Use pgAdmin to run SQL queries on your data
3. **Review Reports**: Analyze the business reports generated in `data/reports/`
4. **Try the API**: Use the interactive docs at http://localhost:8000/docs
5. **Monitor with Grafana**: Set up dashboards at http://localhost:3000 (default login: admin/admin)
6. **Experiment**: Modify DAG schedules, add new transformations, create custom reports

## 📁 Project Structure

```
Airflow_ETL/
├── dags/                          # Airflow DAG definitions
│   ├── etl_master_orchestrator.py # Master coordination DAG
│   ├── etl_customers.py           # Customer dimension pipeline
│   ├── etl_products.py            # Product dimension pipeline
│   ├── etl_stores.py              # Store dimension pipeline
│   ├── etl_exchange_rates.py      # Exchange rate pipeline
│   ├── etl_sales.py               # Sales fact pipeline
│   ├── etl_reports.py             # Business reporting DAG
│   └── dag_base.py                # Shared configuration
│
├── scripts/                       # Utility modules
│   ├── Extract.py                 # Data extraction logic
│   ├── Transform.py               # Data transformation logic
│   ├── Load.py                    # Database loading logic
│   ├── ReportGenerator.py         # Report generation
│   ├── api/                       # REST API service
│   │   ├── main.py               # FastAPI application
│   │   ├── web_dashboard.py      # Flask web UI
│   │   ├── routes/               # API endpoints
│   │   ├── models/               # Pydantic models
│   │   └── utils/                # API utilities
│   └── utils/                     # Shared utilities
│
├── config/                        # YAML configurations
│   ├── customers_config.yaml
│   ├── products_config.yaml
│   ├── stores_config.yaml
│   ├── sales_config.yaml
│   └── exchange_rates_config.yaml
│
├── data/                          # Data directories
│   ├── raw/dataset/              # Source CSV files
│   ├── staging/                  # Intermediate processing
│   ├── processed/                # Cleaned data
│   └── reports/                  # Generated reports
│
├── Docker/                        # Docker configuration
│   ├── docker-compose.yaml       # Service orchestration
│   ├── Dockerfile                # Airflow image
│   ├── Dockerfile.api            # API service image
│   └── .env                      # Environment variables
│
└── docs/                          # Documentation
    └── Implementation_Snippets.md # Code reference
```

## 🌐 Accessing Services

After running `docker-compose up -d`, access these services:

| Service                 | URL                        | Credentials             | Purpose                                                                   |
| ----------------------- | -------------------------- | ----------------------- | ------------------------------------------------------------------------- |
| **Airflow UI**    | http://localhost:8080      | airflow / airflow       | DAG management & monitoring                                               |
| **REST API**      | http://localhost:8000      | API Key: dev-key-12345  | Programmatic access                                                       |
| **API Docs**      | http://localhost:8000/docs | -                       | Interactive API documentation                                             |
| **Web Dashboard** | http://localhost:5000      | -                       | User-friendly monitoring UI (run `python scripts/api/web_dashboard.py`) |
| **pgAdmin**       | http://localhost:5050      | admin@admin.com / admin | Database visualization                                                    |
| **PostgreSQL**    | localhost:5434             | airflow / airflow       | Direct database access                                                    |

### Direct PostgreSQL Access (Advanced Users)

In addition to pgAdmin's graphical interface, you can connect to PostgreSQL directly:

**Method 1: From Outside Docker (Desktop Tools)**

Use any PostgreSQL client (DBeaver, DataGrip, psql) with these settings:

- **Host**: `localhost` (external access)
- **Port**: `5434` (exposed Docker port)
- **Database**: `airflow`
- **Username**: `airflow`
- **Password**: `airflow`

**Example with psql CLI**:

```powershell
# If you have PostgreSQL installed locally
psql -h localhost -p 5434 -U airflow -d airflow

# List tables in etl_output schema
\dt etl_output.*

# Query data
SELECT COUNT(*) FROM etl_output.customers;
```

**Method 2: From Inside Docker Container**

```powershell
# Access PostgreSQL container directly
docker exec -it docker-postgres-1 psql -U airflow -d airflow

# You're now in psql prompt
airflow=# \l                    -- List databases
airflow=# \dn                   -- List schemas
airflow=# \dt etl_output.*      -- List tables in etl_output schema
airflow=# \d etl_output.sales   -- Describe sales table structure
airflow=# \q                    -- Exit psql
```

**Connection String for Applications**:

```
postgresql://airflow:airflow@localhost:5434/airflow
```

**Security Note**: These are development credentials. For production:

1. Change default passwords in `Docker/.env`
2. Use secrets management (Docker secrets, Vault)
3. Restrict network access with firewall rules
4. Enable SSL/TLS connections

## 🎯 Running DAGs

### Option 1: Master Orchestrator (Recommended)

Runs all pipelines in the correct order:

```
1. Navigate to Airflow UI (http://localhost:8080)
2. Find "etl_master_orchestrator"
3. Toggle to ON (unpause)
4. Click Play → Trigger DAG
```

**Execution Order**:

1. Customers, Products, Stores, Exchange Rates (parallel)
2. Sales (waits for Products to complete)
3. Reports (waits for all pipelines to complete)

### Option 2: Individual DAGs

Run specific pipelines independently:

- `etl_customers` - Customer dimension
- `etl_products` - Product dimension
- `etl_stores` - Store dimension
- `etl_exchange_rates` - Exchange rate dimension
- `etl_sales` - Sales fact table
- `etl_reports` - Generate 9 business reports

### Option 3: API Trigger

```powershell
# Trigger via REST API
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/trigger" `
  -Method POST `
  -Headers @{"X-API-Key"="dev-key-12345"}
```

### Monitoring Execution

**Via Airflow UI**:

- Click on DAG name → Graph view
- See task status (green=success, red=failed, yellow=running)
- Click task → Logs to see execution details

**Via Web Dashboard**:

- Open http://localhost:5000
- Select DAG from dropdown
- View runs, tasks, and logs

**Via REST API**:

```powershell
# Get DAG status
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/status" `
  -Headers @{"X-API-Key"="dev-key-12345"}

# Get recent runs
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/runs?page_size=10" `
  -Headers @{"X-API-Key"="dev-key-12345"}
```

## 🛠️ Common Commands

### Docker Management

```powershell
# Start all services
cd Docker
docker-compose up -d

# Stop all services
docker-compose down

# View service status
docker-compose ps

# View logs for specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
docker-compose logs -f api

# View logs for all services
docker-compose logs -f

# Restart a specific service
docker-compose restart airflow-webserver

# Restart all services
docker-compose restart

# Stop and remove all containers + volumes (CAUTION: deletes database!)
docker-compose down -v

# Rebuild images after code changes
docker-compose build
docker-compose up -d
```

### Airflow CLI Commands

Execute commands inside the webserver container:

```powershell
# Access Airflow CLI
docker exec -it airflow-webserver bash

# List all DAGs
airflow dags list

# Trigger a specific DAG
airflow dags trigger etl_master_orchestrator

# Test a specific task
airflow tasks test etl_customers extract 2026-01-19

# Clear task state to re-run
airflow tasks clear etl_sales --yes

# Check DAG structure
airflow dags show etl_master_orchestrator

# List DAG runs
airflow dags list-runs -d etl_master_orchestrator

# Create Airflow user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Database Access

```powershell
# Connect to PostgreSQL via Docker
docker exec -it airflow-postgres psql -U airflow -d airflow

# Common SQL queries
# List all tables
\dt etl_output.*

# Count records
SELECT 'customers' as table, COUNT(*) FROM etl_output.customers
UNION ALL
SELECT 'products', COUNT(*) FROM etl_output.products
UNION ALL
SELECT 'stores', COUNT(*) FROM etl_output.stores
UNION ALL
SELECT 'sales', COUNT(*) FROM etl_output.sales
UNION ALL
SELECT 'exchange_rates', COUNT(*) FROM etl_output.exchange_rates;

# Exit psql
\q
```

### Python Environment (Local Development)

```powershell
# Activate conda environment
conda activate KB_1978

# Install dependencies
pip install -r requirements.txt

# Run REST API locally (not in Docker)
cd scripts/api
uvicorn main:app --reload --port 8000

# Run Flask dashboard locally
python web_dashboard.py

# Run tests
python test_api.py
```

## 🐛 Troubleshooting

### First-Time Setup Issues

#### Issue: Docker Desktop Not Running

**Symptom**: `error during connect: This error may indicate that the docker daemon is not running`

**Solution**:

```powershell
# Start Docker Desktop from Start Menu
# Or run command (if installed as service)
Start-Service docker

# Verify Docker is running
docker version
```

#### Issue: Docker Images Taking Too Long to Download

**Symptom**: First `docker-compose up -d` stuck on "Pulling..."

**Context**: First-time setup downloads ~2GB of images (PostgreSQL, Airflow, Redis, Grafana, etc.)

**Solution**:

- **Be patient**: Initial download can take 5-15 minutes depending on internet speed
- **Check progress**: Open Docker Desktop → Images to see download progress
- **Resume interrupted download**: Just run `docker-compose up -d` again
- **Use faster mirror**: Edit docker daemon settings to use a closer registry mirror

#### Issue: Port Conflicts on Startup

**Symptom**: `Bind for 0.0.0.0:8080 failed: port is already allocated`

**Solution**:

```powershell
# Identify what's using the port
netstat -ano | findstr :8080

# Kill the process (replace <PID> with actual process ID)
taskkill /PID <PID> /F

# Or change port in Docker/.env file
AIRFLOW_PORT=8081  # Change to any available port
```

**Common Port Conflicts**:

- **8080**: Airflow Webserver (alternative: 8081, 8082)
- **5434**: PostgreSQL (alternative: 5435, 5436)
- **8000**: REST API (alternative: 8001, 8888)
- **5000**: Flask Dashboard (alternative: 5001, 3001)
- **5050**: pgAdmin (alternative: 5051, 8888)

#### Issue: Insufficient Memory for Docker

**Symptom**: Containers crash with `OOMKilled` or fail to start

**Solution**:

```powershell
# Increase Docker memory allocation
# Docker Desktop → Settings → Resources → Memory
# Set to at least 4GB (recommended: 6-8GB)
```

#### Issue: Airflow Init Container Fails

**Symptom**: `docker-airflow-init-1 exited with code 1`

**Solution**:

```powershell
# Check init logs
docker-compose logs airflow-init

# Common causes:
# 1. Database not ready - wait 30 seconds and restart
docker-compose restart airflow-init

# 2. Permissions issue - reset volumes
docker-compose down -v
docker-compose up -d

# 3. Configuration error - check Docker/.env file
```

#### Issue: No DAGs Appearing in UI

**Symptom**: Airflow UI is empty, no DAGs listed

**Solution**:

```powershell
# Wait 2-3 minutes for DAG parsing
# Check scheduler logs
docker-compose logs airflow-scheduler | Select-String -Pattern "ERROR"

# Verify DAG files are mounted
docker exec -it docker-webserver-1 ls /opt/airflow/dags

# Check for Python syntax errors
docker exec -it docker-webserver-1 python /opt/airflow/dags/etl_master_orchestrator.py

# Force DAG refresh
docker-compose restart airflow-scheduler
```

#### Issue: pgAdmin Can't Connect to PostgreSQL

**Symptom**: "Unable to connect to server"

**Common Mistakes & Solutions**:

❌ **Mistake 1**: Using `localhost` as hostname
✅ **Solution**: Use `postgres` (Docker service name)

❌ **Mistake 2**: Using port `5434`
✅ **Solution**: Use port `5432` (internal Docker port)

❌ **Mistake 3**: PostgreSQL not ready
✅ **Solution**: Wait 2-3 minutes after `docker-compose up`, check health:

```powershell
docker-compose ps postgres
# Should show: Up (healthy)
```

❌ **Mistake 4**: Wrong credentials
✅ **Solution**: Username=`airflow`, Password=`airflow`, Database=`airflow`

#### Issue: Web Dashboard Returns Connection Errors

**Symptom**: Dashboard shows "Failed to connect to API" or "Connection refused"

**Solution**:

```powershell
# 1. Verify REST API is running
docker-compose ps api
# Should show: Up

# 2. Test API health
Invoke-RestMethod -Uri "http://localhost:8000/health"
# Should return: {"status":"healthy"}

# 3. Check API logs for errors
docker-compose logs api

# 4. Restart API service
docker-compose restart api

# 5. Verify Python dependencies installed
pip list | Select-String -Pattern "flask|requests|pandas"
```

#### Issue: Python Module Import Errors in DAGs

**Symptom**: Tasks fail with `ModuleNotFoundError: No module named 'scripts'`

**Solution**:

```powershell
# Verify PYTHONPATH in docker-compose.yaml
# Should include: PYTHONPATH=/opt/airflow

# Restart scheduler to reload environment
docker-compose restart airflow-scheduler

# Check if scripts are mounted correctly
docker exec -it docker-webserver-1 ls /opt/airflow/scripts
```

### Runtime Issues### Runtime Issues

#### Issue: DAG Takes Too Long (50+ Minutes)

**Symptom**: Master orchestrator or child DAGs running very slowly

**Solutions Applied** (Already Optimized in v2.0):

- ✅ Sensor poke interval reduced: 30s → 5s
- ✅ Sensor timeout reduced: 3600s → 1200s
- ✅ Sensor mode changed to 'reschedule'
- ✅ Parallelism increased: 32 concurrent tasks
- ✅ Expected duration: **5-8 minutes**

**If Still Slow**:

```powershell
# Check active task count
docker exec -it docker-webserver-1 airflow tasks states-for-dag-run etl_master_orchestrator <run_id>

# Review task durations in Airflow UI
# Go to: DAG → Run → Task Duration chart

# Check for blocked sensors
docker-compose logs airflow-scheduler | Select-String -Pattern "sensor"
```

### API Returns 404 or 500 Errors

**Problem**: REST API endpoints not working

```powershell
# Check API container logs
docker-compose logs api

# Verify API is running
Invoke-RestMethod -Uri "http://localhost:8000/health"

# Check database connection from API
docker exec -it airflow-api python -c "from utils.airflow_client import AirflowClient; print('OK')"
```

### Volumes Not Persisting

**Problem**: Data lost after restart

1. Don't use `docker-compose down -v` (deletes volumes)
2. Use `docker-compose down` to preserve data
3. Check volume status:
   ```powershell
   docker volume ls
   docker volume inspect docker_postgres-db-volume
   ```

## 💻 Development

### Adding New DAGs

1. Create new DAG file in `dags/` directory
2. Import from `dag_base.py` for consistency:
   ```python
   from dag_base import DEFAULT_ARGS, DEFAULT_DAG_CONFIG, get_db_config
   ```
3. Test locally:
   ```powershell
   python dags/your_new_dag.py
   ```
4. Restart scheduler to pick up changes:
   ```powershell
   docker-compose restart airflow-scheduler
   ```

### Modifying Existing Code

For changes to `scripts/` or `dags/`:

- Files are mounted as volumes - changes reflect immediately
- No need to rebuild Docker images
- Restart scheduler if DAG structure changes

For changes to API code (`scripts/api/`):

- Restart API service:
  ```powershell
  docker-compose restart api
  ```

For changes to Docker configuration:

```powershell
# Rebuild and restart
docker-compose build
docker-compose up -d
```

### Testing Changes

```powershell
# Test a specific task without running full DAG
docker exec -it airflow-webserver \
  airflow tasks test etl_customers extract 2026-01-19

# Run Python scripts manually
docker exec -it airflow-webserver python /opt/airflow/scripts/Extract.py
```

## 📚 Documentation

Comprehensive documentation is available in the following files:

| Document                                                        | Description                                              |
| --------------------------------------------------------------- | -------------------------------------------------------- |
| [PROJECT_TRACKER.md](PROJECT_TRACKER.md)                           | Master tracking with all sprints, tasks, and credentials |
| [TEAM1_TASK_TRACKER.md](TEAM1_TASK_TRACKER.md)                     | Detailed Team 1 implementation tracker                   |
| [TEAM2_TASK_TRACKER.md](TEAM2_TASK_TRACKER.md)                     | Team 2 planning tracker                                  |
| [API_ROUTES_GUIDE.md](API_ROUTES_GUIDE.md)                         | Complete REST API reference (850+ lines)                 |
| [Docker/API_SERVICE_GUIDE.md](Docker/API_SERVICE_GUIDE.md)         | Docker-specific API usage guide                          |
| [scripts/api/README.md](scripts/api/README.md)                     | API developer documentation                              |
| [docs/Implementation_Snippets.md](docs/Implementation_Snippets.md) | Code examples for all 42 tasks                           |

### Key Resources

- **Task Reference**: All 42 tasks (T0007-T0042) documented in Implementation_Snippets.md
- **API Examples**: 13 endpoints with curl/PowerShell examples in API_ROUTES_GUIDE.md
- **Configuration**: YAML files in `config/` directory for each data source
- **Credentials**: All access information in PROJECT_TRACKER.md

## 🎓 Learning Resources

### Airflow Concepts

- **DAG**: Directed Acyclic Graph - defines workflow
- **Task**: Single unit of work (extract, transform, load)
- **Operator**: Template for a task (PythonOperator, BashOperator)
- **Sensor**: Waits for condition (ExternalTaskSensor)
- **XCom**: Cross-communication between tasks

### Project Patterns

- **Master-Child DAGs**: Orchestrator triggers individual pipelines
- **ExternalTaskSensor**: Wait for upstream DAG completion
- **Task Groups**: Organize related tasks visually
- **Bulk Operations**: Process data in optimized chunks
- **Rejected Records**: Track and store failed validations

## 📊 Expected Output

After successful execution:

### Database Tables

```sql
-- etl_output schema contains:
customers         (15,266 rows)
products          (2,517 rows)
stores            (67 rows)
exchange_rates    (3,655 rows)
sales             (26,326 rows)
```

### Generated Reports

Located in `data/reports/`:

1. `customer_summary.csv` - Customer statistics
2. `customer_segmentation.csv` - RFM analysis
3. `order_status.csv` - Order tracking
4. `sales_trends_daily.csv` - Daily sales patterns
5. `product_performance.csv` - Top products
6. `store_performance.csv` - Store rankings
7. `anomaly_detection.csv` - Outlier identification
8. `data_quality_scorecard.csv` - Quality metrics
9. `dag_execution_summary.csv` - Pipeline performance

### Logs

- Task logs: `logs/dag_id={dag_name}/run_id={run_id}/task_id={task_name}/`
- Scheduler logs: `logs/scheduler/`

## 🤝 Contributing

This project was developed as part of Infosys training program. For questions or issues:

1. Check existing documentation
2. Review logs for error messages
3. Consult troubleshooting section
4. Contact project maintainers

## Quick Reference Card

```powershell
# START EVERYTHING
cd Docker
docker-compose up -d

# START WEB DASHBOARD (in separate terminal)
conda activate KB_1978
cd D:\sam\Projects\Infosys\Airflow
python scripts/api/web_dashboard.py

# CHECK STATUS
docker-compose ps

# VIEW LOGS
docker-compose logs -f

# STOP EVERYTHING
docker-compose down

# AIRFLOW UI
http://localhost:8080
airflow / airflow

# TRIGGER MASTER DAG
# Go to UI → etl_master_orchestrator → Play → Trigger

# CHECK RESULTS
# pgAdmin: http://localhost:5050
# Web Dashboard: http://localhost:5000
```

---

## 📝 First-Time Setup Checklist

Use this checklist to ensure your environment is properly configured:

### Pre-Installation (Before Running Docker)

- [ ] **Docker Desktop installed** (version 20.10+)
  - Run: `docker --version`
- [ ] **Docker Compose installed** (version 2.0+)
  - Run: `docker-compose --version`
- [ ] **Docker Desktop is running**
  - Check: Docker icon in system tray should be green
- [ ] **Docker has sufficient resources**
  - Docker Desktop → Settings → Resources
  - Memory: Minimum 4GB (Recommended: 6-8GB)
  - Disk: At least 10GB free space
- [ ] **Repository cloned**
  - `git clone https://github.com/SammyBoy-09/Airflow_ETL.git`
- [ ] **CSV data files present**
  - Check: `ls data\raw\dataset` shows 5 CSV files
- [ ] **Port availability checked**
  - Ports free: 8080, 5434, 8000, 5000, 5050, 3000, 9090

### Initial Docker Setup

- [ ] **Navigated to Docker directory**
  - `cd Docker`
- [ ] **Started services**
  - `docker-compose up -d`
- [ ] **Waited for initialization**
  - Wait 3-5 minutes for first-time setup
- [ ] **Verified all services running**
  - `docker-compose ps` - All services show `Up` and `(healthy)`
- [ ] **Checked logs for errors**
  - `docker-compose logs | Select-String -Pattern "ERROR"`

### Airflow Configuration

- [ ] **Accessed Airflow UI**
  - Open: http://localhost:8080
- [ ] **Logged in successfully**
  - Username: `airflow`, Password: `airflow`
- [ ] **Verified DAGs visible**
  - Should see 8+ DAGs in list
- [ ] **All DAGs parsed without errors**
  - No red "Import Error" messages

### Database Setup (pgAdmin)

- [ ] **Accessed pgAdmin**
  - Open: http://localhost:5050
- [ ] **Logged into pgAdmin**
  - Email: `admin@admin.com`, Password: `admin`
- [ ] **Registered PostgreSQL server**
  - Host: `postgres`, Port: `5432`, Database: `airflow`
  - Username: `airflow`, Password: `airflow`
- [ ] **Connection successful**
  - Can see: Servers → Airflow ETL Database → Databases → airflow
- [ ] **Verified schemas present**
  - `public` schema (Airflow metadata)
  - `etl_output` schema (for ETL data - empty until first run)

### API & Dashboard Setup

- [ ] **Verified REST API running**
  - Test: `Invoke-RestMethod -Uri "http://localhost:8000/health"`
  - Should return: `{"status":"healthy"}`
- [ ] **Python environment created** (for dashboard)
  - `conda create -n airflow_env python=3.11` OR `python -m venv airflow_env`
- [ ] **Python environment activated**
  - `conda activate airflow_env` OR `.\airflow_env\Scripts\Activate.ps1`
- [ ] **Dependencies installed**
  - `pip install -r requirements.txt`
- [ ] **Web dashboard started**
  - `python scripts/api/web_dashboard.py`
- [ ] **Dashboard accessible**
  - Open: http://localhost:5000

### First Pipeline Run

- [ ] **Unpaused master orchestrator**
  - Airflow UI → `etl_master_orchestrator` → Toggle to ON
- [ ] **Triggered master DAG**
  - Click Play button → Trigger DAG
- [ ] **Monitored execution**
  - Watch Graph view or use Web Dashboard
- [ ] **Verified successful completion**
  - All tasks green in Graph view
  - Expected duration: 5-8 minutes
- [ ] **Checked database tables**
  - pgAdmin: etl_output schema should have 5 tables with data
- [ ] **Verified reports generated**
  - Check: `ls data\reports` shows 9 CSV reports
- [ ] **Downloaded sample report**
  - Web Dashboard → Downloads → Preview/Download reports

### Optional: Monitoring Stack

- [ ] **Accessed Grafana**
  - Open: http://localhost:3000
  - Default login: `admin` / `admin`
- [ ] **Accessed Prometheus**
  - Open: http://localhost:9090
- [ ] **Set up custom dashboards** (optional)

### Validation

- [ ] **Customer data loaded**
  - Query: `SELECT COUNT(*) FROM etl_output.customers;`
  - Expected: ~15,000 rows
- [ ] **Product data loaded**
  - Query: `SELECT COUNT(*) FROM etl_output.products;`
  - Expected: ~2,500 rows
- [ ] **Sales data loaded**
  - Query: `SELECT COUNT(*) FROM etl_output.sales;`
  - Expected: ~26,000 rows
- [ ] **Reports accessible**
  - All 9 reports in `data/reports/` directory
- [ ] **API endpoints working**
  - Test: http://localhost:8000/docs (interactive API docs)

### Troubleshooting Reference

If you encounter issues, refer to:

- [ ] **Troubleshooting section** (above)
- [ ] **Docker logs**: `docker-compose logs <service-name>`
- [ ] **Airflow task logs**: Airflow UI → DAG → Task → Logs
- [ ] **Service health**: `docker-compose ps`

---

## 🎓 Understanding the System

### Key Concepts for First-Time Users

**What is Airflow?**

- Apache Airflow is a workflow orchestration tool
- **DAG** (Directed Acyclic Graph) = A workflow with tasks and dependencies
- **Task** = A single unit of work (e.g., extract data, transform data)
- **Operator** = A template for creating tasks (PythonOperator, BashOperator)
- **Scheduler** = Runs DAGs based on schedule or trigger
- **Webserver** = Provides the UI for monitoring

**What is the ETL Process?**

- **Extract**: Read data from CSV files
- **Transform**: Clean, validate, and reshape data
- **Load**: Insert data into PostgreSQL database

**How Does This Project Work?**

1. **Master Orchestrator** triggers individual DAG workflows
2. **Dimension DAGs** (customers, products, stores, exchange_rates) load reference data in parallel
3. **Fact DAG** (sales) loads transaction data after products complete
4. **Reports DAG** generates analytical reports after all data is loaded
5. **Quality DAG** runs validation checks and creates scorecards

**What Technologies Are Used?**

- **Docker**: Containerization for consistent environments
- **PostgreSQL**: Relational database for storing data
- **Airflow**: Workflow orchestration and scheduling
- **Flask**: Web dashboard framework
- **FastAPI**: REST API framework
- **Grafana/Prometheus**: Monitoring and metrics
- **Pandas**: Data processing in Python

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     USER INTERFACES                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Airflow UI  │  │ Web Dashboard│  │   pgAdmin    │      │
│  │  :8080       │  │  :5000       │  │   :5050      │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│                   DOCKER CONTAINER LAYER                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Webserver   │  │  Scheduler   │  │  REST API    │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│         └──────────┬───────┴──────────────────┘              │
│                    │                                          │
│         ┌──────────▼──────────┐  ┌──────────────┐           │
│         │   PostgreSQL        │  │    Redis     │           │
│         │   (Metadata + ETL)  │  │   (Queue)    │           │
│         └─────────────────────┘  └──────────────┘           │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│                     DATA STORAGE LAYER                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  CSV Files   │  │  Processed   │  │   Reports    │      │
│  │  (data/raw)  │  │  (data/proc) │  │  (data/rep)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
┌─────────────┐
│  CSV Files  │ Customers.csv, Products.csv, Stores.csv,
└──────┬──────┘ Sales.csv, Exchange_Rates.csv
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              AIRFLOW ETL PIPELINE                        │
│                                                          │
│  Stage 1: Ingestion (Parallel)                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌──────────────┐  │
│  │Customer │ │Product  │ │ Store   │ │Exchange Rate │  │
│  │  ETL    │ │  ETL    │ │  ETL    │ │     ETL      │  │
│  └────┬────┘ └────┬────┘ └────┬────┘ └──────┬───────┘  │
│       │           │            │             │          │
│       └───────────┴────────────┴─────────────┘          │
│                   │                                      │
│  Stage 2: Facts (Sequential)                            │
│       ┌───────────▼───────────┐                         │
│       │     Sales ETL         │ (Waits for Products)    │
│       └───────────┬───────────┘                         │
│                   │                                      │
│  Stage 3: Analytics                                     │
│       ┌───────────▼───────────┐                         │
│       │   Report Generation   │ (9 Business Reports)    │
│       └───────────┬───────────┘                         │
│                   │                                      │
│  Stage 4: Quality                                       │
│       ┌───────────▼───────────┐                         │
│       │  Data Quality Checks  │                         │
│       └───────────────────────┘                         │
└─────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│           PostgreSQL Database (etl_output schema)        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │Customers │ │ Products │ │  Stores  │ │  Sales   │  │
│  │15K rows  │ │ 2.5K rows│ │ 67 rows  │ │26K rows  │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘  │
└─────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│               Generated Reports (CSV)                    │
│  • Customer Summary        • Sales Trends Daily          │
│  • Customer Segmentation   • Product Performance         │
│  • Order Status            • Store Performance           │
│  • Anomaly Detection       • Data Quality Scorecard      │
│  • DAG Execution Summary                                 │
└─────────────────────────────────────────────────────────┘
```

---

**Last Updated**: January 28, 2026
**Version**: 2.0 (Performance Optimized)
