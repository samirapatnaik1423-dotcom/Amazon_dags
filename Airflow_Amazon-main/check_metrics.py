from sqlalchemy import create_engine, text

engine = create_engine('postgresql://airflow:airflow@localhost:5434/airflow')

with engine.connect() as conn:
    # Check total rows
    result = conn.execute(text('SELECT COUNT(*) as count FROM etl_output.dag_run_summary'))
    count = result.fetchone()[0]
    print(f'Total rows in dag_run_summary: {count}')
    
    # Check sample data
    result2 = conn.execute(text('''
        SELECT table_name, rows_extracted, rows_loaded, rows_rejected, execution_date 
        FROM etl_output.dag_run_summary 
        LIMIT 10
    '''))
    print('\nSample rows:')
    for row in result2:
        print(f"  {row}")
    
    # Run the actual query from get_etl_metrics
    print('\n\nRunning the actual API query:')
    result3 = conn.execute(text('''
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
    '''))
    
    print('Grouped results:')
    for row in result3:
        print(f"  {row}")
