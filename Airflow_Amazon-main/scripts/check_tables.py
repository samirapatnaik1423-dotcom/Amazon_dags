from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5434/airflow')
conn = engine.connect()
result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'etl_output' ORDER BY table_name"))
print("ETL_OUTPUT SCHEMA TABLES:")
print("="*50)
for row in result:
    print(f"  - {row[0]}")
conn.close()
