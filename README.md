# Amazon_dags
**Project Overview**:
This project demonstrates an end-to-end data engineering pipeline implemented using Apache Airflow.
It covers data ingestion, validation, data quality checks, schema management, Bronze–Silver–Gold architecture, scheduling, metadata management, and data lineage following industry-standard practices.
The pipeline processes Amazon order data from multiple sources, ensures schema consistency and data quality, and produces analytics-ready outputs with full traceability.
Technology Stack

Apache Airflow – Workflow orchestration and scheduling

1.**Python** – ETL and validation logic
2.**Pandas** – Data processing
3.**Pydantic** – Schema validation
4.**CSV / Parquet** – Storage formats
5.**JSON** – Metadata, data quality, and lineage reports
**Environment Setup and Pipeline Design:**

Apache Airflow is installed and configured locally or using Docker.
DAG-based architecture is used to design modular and reusable ETL pipelines.
The pipeline is idempotent and restart-safe.Data Sources

**The pipeline supports ingestion from:**

CSV files
JSON files
SQL databases
REST APIs

**Data Ingestion**:

Custom Python scripts extract data from different sources.
File polling logic detects new or updated input files.
Database connection utilities handle SQL-based ingestion.
Exception handling is implemented for missing, corrupt, or malformed data.

**Data Quality and Validation**:

The pipeline performs column-level validation including regex checks for email and phone fields, minimum and maximum checks for numeric columns, null value detection, and uniqueness validation for primary keys such as OrderID.
Basic data profiling is also generated, including total row count, column-wise statistics (minimum, maximum, and average), and null percentage per column.
All validation and profiling results are stored as structured JSON reports.

**Schema Management:**

Schemas are defined using Pydantic models and enforced before data processing.
The pipeline detects schema drifts such as missing or extra columns, automatically corrects minor deviations, logs discrepancies, and maintains schema versions for audit and traceability.

**Bronze–Silver–Gold Architecture:**
The pipeline follows a layered data architecture:

Bronze stores raw, unmodified data in CSV format for audit and reprocessing.

Silver contains cleaned and schema-validated data stored in Parquet for performance.

Gold holds aggregated, business-ready datasets used for analytics and reporting.

**Partitioning, Archival, and Metadata:**

Data is partitioned using sprint-based and logical date-based strategies.
Older data is archived to optimize storage usage.
A centralized metadata log captures record counts, data formats, execution timestamps, and pipeline status, stored as JSON for monitoring and traceability.

**Scheduling and Pipeline Strategy:**

The pipeline supports hourly, daily, and weekly execution using cron scheduling with explicit timezone configuration.
Delayed starts, interval scheduling, and SLA miss alerts are implemented to handle upstream dependencies and detect delayed runs.
All scheduling strategies are managed through a single merged DAG using conditional execution logic.

**Data Lineage:**

The pipeline tracks dataset origin, records each transformation step, builds a lineage metadata table, and generates a lineage report.
A text-based visual lineage representation provides clear end-to-end traceability for governance and impact analysis.
Python-based ingestion scripts handle multiple formats uniformly.

**Key Highlights:**
Complete data engineering lifecycle implementation

Production-style Airflow pipelines

Strong data quality and governance controls

Schema drift detection and handling

Metadata and lineage tracking

Easy extensibility to cloud or Spark-based platforms
