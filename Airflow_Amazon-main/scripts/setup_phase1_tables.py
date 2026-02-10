"""
Phase 1 Database Tables Creation Script
TEAM 2 - SPRINT 1 (PHASE 1)
Creates tables for ingestion metadata tracking
"""

import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_ingestion_tables(connection_string: str = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"):
    """
    Create Phase 1 ingestion metadata tables
    
    Tables created:
    1. ingestion_logs - Track all ingestion operations
    2. api_connections - Registry of API connections
    3. file_watch_history - File monitoring history
    """
    
    logger.info("🔨 Creating Phase 1 ingestion tables...")
    
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Create schema if not exists
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        conn.commit()
        
        # ═══════════════════════════════════════════════════════════
        # Table 1: ingestion_logs
        # ═══════════════════════════════════════════════════════════
        
        logger.info("📋 Creating ingestion_logs table...")
        
        create_ingestion_logs = text("""
        CREATE TABLE IF NOT EXISTS etl_output.ingestion_logs (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            source_type VARCHAR(50) NOT NULL,  -- 'CSV', 'JSON', 'SQL', 'API'
            source_path TEXT,
            rows_extracted INTEGER,
            columns_extracted INTEGER,
            extraction_time_seconds NUMERIC(10,2),
            status VARCHAR(20) NOT NULL,  -- 'success', 'failed'
            error_message TEXT,
            extracted_at TIMESTAMP DEFAULT NOW(),
            dag_run_id VARCHAR(255),
            metadata JSONB
        );
        
        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_ingestion_logs_table ON etl_output.ingestion_logs(table_name);
        CREATE INDEX IF NOT EXISTS idx_ingestion_logs_status ON etl_output.ingestion_logs(status);
        CREATE INDEX IF NOT EXISTS idx_ingestion_logs_extracted_at ON etl_output.ingestion_logs(extracted_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ingestion_logs_source_type ON etl_output.ingestion_logs(source_type);
        """)
        
        conn.execute(create_ingestion_logs)
        conn.commit()
        logger.info("✅ ingestion_logs table created")
        
        # ═══════════════════════════════════════════════════════════
        # Table 2: api_connections
        # ═══════════════════════════════════════════════════════════
        
        logger.info("📋 Creating api_connections table...")
        
        create_api_connections = text("""
        CREATE TABLE IF NOT EXISTS etl_output.api_connections (
            id SERIAL PRIMARY KEY,
            connection_name VARCHAR(100) UNIQUE NOT NULL,
            base_url TEXT NOT NULL,
            auth_type VARCHAR(50) DEFAULT 'none',  -- 'none', 'api_key', 'bearer', 'basic', 'oauth2'
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            last_used TIMESTAMP,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            metadata JSONB,
            description TEXT
        );
        
        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_api_connections_name ON etl_output.api_connections(connection_name);
        CREATE INDEX IF NOT EXISTS idx_api_connections_active ON etl_output.api_connections(is_active);
        """)
        
        conn.execute(create_api_connections)
        conn.commit()
        logger.info("✅ api_connections table created")
        
        # ═══════════════════════════════════════════════════════════
        # Table 3: file_watch_history
        # ═══════════════════════════════════════════════════════════
        
        logger.info("📋 Creating file_watch_history table...")
        
        create_file_watch_history = text("""
        CREATE TABLE IF NOT EXISTS etl_output.file_watch_history (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            file_hash VARCHAR(64),
            file_size_bytes BIGINT,
            file_extension VARCHAR(20),
            directory VARCHAR(500),
            detected_at TIMESTAMP NOT NULL,
            processed_at TIMESTAMP,
            status VARCHAR(20) DEFAULT 'detected',  -- 'detected', 'processing', 'completed', 'failed'
            processing_dag_run_id VARCHAR(255),
            error_message TEXT,
            metadata JSONB
        );
        
        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_file_watch_file_hash ON etl_output.file_watch_history(file_hash);
        CREATE INDEX IF NOT EXISTS idx_file_watch_status ON etl_output.file_watch_history(status);
        CREATE INDEX IF NOT EXISTS idx_file_watch_detected_at ON etl_output.file_watch_history(detected_at DESC);
        CREATE INDEX IF NOT EXISTS idx_file_watch_file_name ON etl_output.file_watch_history(file_name);
        """)
        
        conn.execute(create_file_watch_history)
        conn.commit()
        logger.info("✅ file_watch_history table created")
        
        # ═══════════════════════════════════════════════════════════
        # Verify table creation
        # ═══════════════════════════════════════════════════════════
        
        logger.info("🔍 Verifying table creation...")
        
        verify_query = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'etl_output' 
        AND table_name IN ('ingestion_logs', 'api_connections', 'file_watch_history')
        ORDER BY table_name
        """)
        
        result = conn.execute(verify_query)
        tables = [row[0] for row in result]
        
        logger.info(f"📊 Created tables: {tables}")
        
        if len(tables) == 3:
            logger.info("✅ All Phase 1 ingestion tables created successfully!")
        else:
            logger.warning(f"⚠️ Expected 3 tables, found {len(tables)}")
    
    engine.dispose()
    logger.info("🔌 Database connection closed")


def insert_sample_api_connections(connection_string: str = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"):
    """Insert sample API connections for demo"""
    
    logger.info("📝 Inserting sample API connections...")
    
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        insert_query = text("""
        INSERT INTO etl_output.api_connections 
        (connection_name, base_url, auth_type, description, metadata)
        VALUES 
        ('jsonplaceholder', 'https://jsonplaceholder.typicode.com', 'none', 
         'Public JSON API for testing', '{"endpoints": ["/posts", "/users", "/comments"]}'),
        ('github_api', 'https://api.github.com', 'bearer',
         'GitHub REST API', '{"rate_limit": 5000, "version": "v3"}')
        ON CONFLICT (connection_name) DO NOTHING
        """)
        
        conn.execute(insert_query)
        conn.commit()
        
        logger.info("✅ Sample API connections inserted")
    
    engine.dispose()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("PHASE 1 - DATABASE TABLES SETUP")
    print("="*70 + "\n")
    
    # Create tables
    create_ingestion_tables()
    
    # Insert sample data
    insert_sample_api_connections()
    
    print("\n" + "="*70)
    print("✅ PHASE 1 DATABASE SETUP COMPLETE")
    print("="*70 + "\n")
