**Project Overview:**

This project implements an end-to-end data engineering pipeline using Apache Airflow.
It covers data ingestion, validation, data quality checks, schema management, Bronze–Silver–Gold architecture, scheduling, metadata management, and data lineage following industry-standard practices.

The pipeline processes Amazon order data, ensures schema consistency and data quality, and produces analytics-ready outputs with full traceability.

**Technology Stack:**

Apache Airflow – Workflow orchestration and scheduling

Python – ETL and validation logic

Pandas – Data processing

Pydantic – Schema validation and drift handling

CSV / Parquet – Data storage formats

JSON – Metadata, data quality, and lineage reports

**Pipeline Design:**

DAG-based architecture for modular and reusable pipelines

Idempotent and restart-safe execution

Supports ingestion from CSV, JSON, SQL databases, and REST APIs

**Data Ingestion and Quality:**

Custom Python scripts for multi-format ingestion

File polling logic for new or updated data

Exception handling for missing or corrupt inputs

Column-level validation (regex, min/max, null, uniqueness checks)

Data profiling including row counts, column statistics, and null percentages

Validation and profiling outputs stored as JSON reports

**Schema Management:**

Schemas defined and enforced using Pydantic

Detection and auto-correction of minor schema drifts

Discrepancy logging and schema version tracking for audit

**Data Architecture:**

Bronze: Raw data stored in CSV for audit and replay

Silver: Cleaned and validated data stored in Parquet

Gold: Aggregated, business-ready datasets for analytics

**Scheduling and Orchestration:**

Hourly, daily, and weekly scheduling using cron expressions

Explicit timezone configuration

Delayed starts, interval scheduling, and SLA miss alerts

All schedules managed through a single merged DAG

**Metadata and Data Lineage:**

Central metadata log capturing record counts, formats, timestamps, and status

End-to-end data lineage tracking from source to target

Transformation steps recorded in lineage metadata

Visual, text-based lineage report for governance and traceability

**Key Highlights:**

Complete data engineering lifecycle implementation

Production-style Airflow pipelines

Strong data quality, schema, and governance controls

Metadata and lineage-driven observability

Easily extensible to cloud or Spark-based platforms
