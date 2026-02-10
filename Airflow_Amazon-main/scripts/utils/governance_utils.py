# ═══════════════════════════════════════════════════════════════════════
# PHASE 4 - Security & Compliance Utilities
# Tasks: Data Lineage, Classification/Tagging, Audit Logging
# ═══════════════════════════════════════════════════════════════════════

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine


CLASSIFICATION_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "classification_config.yaml"


def ensure_governance_tables(engine: Engine, schema: str = "etl_output") -> None:
    """Create governance tables if they do not exist."""
    ddl_statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_log (
            audit_id UUID PRIMARY KEY,
            event_type VARCHAR(50),
            entity_type VARCHAR(50),
            entity_id VARCHAR(200),
            status VARCHAR(20),
            details JSONB,
            dag_id VARCHAR(250),
            run_id VARCHAR(250),
            created_at TIMESTAMP DEFAULT NOW()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.data_lineage (
            lineage_id UUID PRIMARY KEY,
            source_type VARCHAR(50),
            source_location TEXT,
            target_table VARCHAR(200),
            transformation TEXT,
            row_count INTEGER,
            dag_id VARCHAR(250),
            run_id VARCHAR(250),
            created_at TIMESTAMP DEFAULT NOW()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.data_classification (
            table_name VARCHAR(200) NOT NULL,
            column_name VARCHAR(200) NOT NULL,
            classification VARCHAR(50),
            tags JSONB,
            source VARCHAR(50),
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (table_name, column_name)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.schema_validation_results (
            validation_id UUID PRIMARY KEY,
            table_name VARCHAR(200),
            sample_size INTEGER,
            error_count INTEGER,
            errors JSONB,
            status VARCHAR(20),
            dag_id VARCHAR(250),
            run_id VARCHAR(250),
            created_at TIMESTAMP DEFAULT NOW()
        )
        """,
    ]

    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))


def log_audit_event(
    engine: Engine,
    event_type: str,
    entity_type: str,
    entity_id: str,
    status: str,
    details: Optional[Dict[str, Any]] = None,
    dag_id: Optional[str] = None,
    run_id: Optional[str] = None,
    schema: str = "etl_output",
) -> None:
    payload = json.dumps(details or {})
    audit_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {schema}.audit_log
                    (audit_id, event_type, entity_type, entity_id, status, details, dag_id, run_id, created_at)
                VALUES
                    (:audit_id, :event_type, :entity_type, :entity_id, :status, CAST(:details AS JSONB), :dag_id, :run_id, :created_at)
                """
            ),
            {
                "audit_id": audit_id,
                "event_type": event_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "status": status,
                "details": payload,
                "dag_id": dag_id,
                "run_id": run_id,
                "created_at": datetime.utcnow(),
            },
        )


def log_lineage(
    engine: Engine,
    source_type: str,
    source_location: Optional[str],
    target_table: str,
    transformation: str,
    row_count: int,
    dag_id: Optional[str] = None,
    run_id: Optional[str] = None,
    schema: str = "etl_output",
) -> None:
    lineage_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {schema}.data_lineage
                    (lineage_id, source_type, source_location, target_table, transformation, row_count, dag_id, run_id, created_at)
                VALUES
                    (:lineage_id, :source_type, :source_location, :target_table, :transformation, :row_count, :dag_id, :run_id, :created_at)
                """
            ),
            {
                "lineage_id": lineage_id,
                "source_type": source_type,
                "source_location": source_location,
                "target_table": target_table,
                "transformation": transformation,
                "row_count": row_count,
                "dag_id": dag_id,
                "run_id": run_id,
                "created_at": datetime.utcnow(),
            },
        )


def load_classification_config() -> Dict[str, Any]:
    if not CLASSIFICATION_CONFIG_PATH.exists():
        return {}
    with CLASSIFICATION_CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def upsert_classification(
    engine: Engine,
    table_name: str,
    column_name: str,
    classification: str,
    tags: Optional[list[str]] = None,
    source: str = "config",
    schema: str = "etl_output",
) -> None:
    tags_json = json.dumps(tags or [])
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {schema}.data_classification
                    (table_name, column_name, classification, tags, source, updated_at)
                VALUES
                    (:table_name, :column_name, :classification, CAST(:tags AS JSONB), :source, :updated_at)
                ON CONFLICT (table_name, column_name)
                DO UPDATE SET
                    classification = EXCLUDED.classification,
                    tags = EXCLUDED.tags,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "table_name": table_name,
                "column_name": column_name,
                "classification": classification,
                "tags": tags_json,
                "source": source,
                "updated_at": datetime.utcnow(),
            },
        )


def apply_classification_from_config(
    engine: Engine,
    table_name: str,
    schema: str = "etl_output",
) -> None:
    config = load_classification_config()
    tables = config.get("tables", {}) if isinstance(config, dict) else {}
    table_cfg = tables.get(table_name, {}) if isinstance(tables, dict) else {}
    columns = table_cfg.get("columns", {}) if isinstance(table_cfg, dict) else {}

    for column, meta in columns.items():
        classification = meta.get("classification", "internal")
        tags = meta.get("tags", [])
        upsert_classification(
            engine=engine,
            table_name=table_name,
            column_name=column,
            classification=classification,
            tags=tags,
            source="config",
            schema=schema,
        )


def log_schema_validation(
    engine: Engine,
    table_name: str,
    sample_size: int,
    error_count: int,
    errors: list[dict[str, Any]],
    status: str,
    dag_id: Optional[str] = None,
    run_id: Optional[str] = None,
    schema: str = "etl_output",
) -> None:
    validation_id = str(uuid.uuid4())
    payload = json.dumps(errors)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {schema}.schema_validation_results
                    (validation_id, table_name, sample_size, error_count, errors, status, dag_id, run_id, created_at)
                VALUES
                    (:validation_id, :table_name, :sample_size, :error_count, CAST(:errors AS JSONB), :status, :dag_id, :run_id, :created_at)
                """
            ),
            {
                "validation_id": validation_id,
                "table_name": table_name,
                "sample_size": sample_size,
                "error_count": error_count,
                "errors": payload,
                "status": status,
                "dag_id": dag_id,
                "run_id": run_id,
                "created_at": datetime.utcnow(),
            },
        )
