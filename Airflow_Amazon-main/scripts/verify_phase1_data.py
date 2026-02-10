from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5434/airflow')

with engine.connect() as conn:
    print("\n" + "="*50)
    print("PHASE 1 DATA VERIFICATION")
    print("="*50 + "\n")
    
    # Check json_data table
    result = conn.execute(text("SELECT COUNT(*) FROM etl_output.json_data"))
    json_count = result.scalar()
    print(f"✅ json_data: {json_count} rows")
    
    # Check api_posts table
    result = conn.execute(text("SELECT COUNT(*) FROM etl_output.api_posts"))
    api_count = result.scalar()
    print(f"✅ api_posts: {api_count} rows")
    
    # Check ingestion_logs
    result = conn.execute(text("SELECT COUNT(*) FROM etl_output.ingestion_logs"))
    log_count = result.scalar()
    print(f"✅ ingestion_logs: {log_count} entries")
    
    print("\n" + "="*50)
    print("ALL PHASE 1 TABLES VERIFIED")
    print("="*50 + "\n")
