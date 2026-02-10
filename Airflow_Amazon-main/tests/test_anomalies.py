import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://airflow:airflow@localhost:5434/airflow')

try:
    df = pd.read_sql('SELECT * FROM etl_output.detected_anomalies LIMIT 5', engine)
    print(f"Anomalies count: {len(df)}")
    if len(df) > 0:
        print(f"Columns: {list(df.columns)}")
        print(df.head())
except Exception as e:
    print(f"Error: {e}")
