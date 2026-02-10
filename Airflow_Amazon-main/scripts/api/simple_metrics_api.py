"""
Simple FastAPI server ONLY for ETL metrics endpoint.
Clean implementation to fix Pipeline Metrics Over Time chart.
"""
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import List, Dict, Any
import uvicorn

app = FastAPI(title="ETL Metrics API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database
DATABASE_URL = "postgresql://airflow:airflow@localhost:5434/airflow"
engine = create_engine(DATABASE_URL)

# Simple API key check
def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key not in ["dev-key-12345", "test-key-67890"]:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "ETL Metrics API"}


@app.get("/api/v1/metadata/metrics")
def get_etl_metrics(x_api_key: str = Header(...)):
    """
    Get ETL metrics from dag_run_summary table.
    Returns array of table-level metrics: rows extracted, loaded, rejected.
    """
    verify_api_key(x_api_key)
    
    print("=" * 80)
    print("[METRICS API] Endpoint called!")
    print("=" * 80)
    
    query = text("""
        SELECT 
            table_name,
            SUM(rows_extracted) as rows_extracted,
            SUM(rows_loaded) as rows_loaded,
            SUM(rows_rejected) as rows_rejected,
            MAX(execution_date) as last_run_date
        FROM etl_output.dag_run_summary
        WHERE table_name IS NOT NULL
        GROUP BY table_name
        ORDER BY table_name
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            metrics = []
            
            for row in result:
                metrics.append({
                    "table_name": row.table_name,
                    "rows_extracted": int(row.rows_extracted or 0),
                    "rows_loaded": int(row.rows_loaded or 0),
                    "rows_rejected": int(row.rows_rejected or 0),
                    "last_run_date": row.last_run_date.isoformat() if row.last_run_date else None
                })
            
            print(f"[METRICS API] Found {len(metrics)} tables")
            for m in metrics:
                print(f"  - {m['table_name']}: {m['rows_extracted']} extracted, {m['rows_loaded']} loaded")
            print("=" * 80)
            
            # Return the array directly (Flask will wrap it)
            return metrics
            
    except Exception as e:
        print(f"[METRICS API ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Starting Simple ETL Metrics API on port 8000")
    print("=" * 80 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
