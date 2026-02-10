"""
SQL Transformation Templates
TEAM 3 - SPRINT 2-3 (PHASE 1)
Reusable SQL templates for common data transformations
"""

from typing import Dict, List, Optional


class SQLTemplates:
    """
    Collection of reusable SQL transformation templates
    
    Categories:
    - Aggregations (GROUP BY, SUM, AVG, COUNT, etc.)
    - Window Functions (ROW_NUMBER, RANK, LAG, LEAD)
    - Joins (INNER, LEFT, RIGHT, FULL)
    - Data Quality (Deduplication, NULL handling)
    - Date/Time Operations
    - String Operations
    """
    
    # ═══════════════════════════════════════════════════════════
    # AGGREGATION TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def group_by_aggregate(table: str, group_columns: List[str], 
                          agg_columns: Dict[str, str], schema: str = None) -> str:
        """
        Generate GROUP BY aggregation query
        
        Args:
            table: Table name
            group_columns: Columns to group by
            agg_columns: Dict of {column: agg_function} e.g., {'revenue': 'SUM', 'orders': 'COUNT'}
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        group_cols = ", ".join(group_columns)
        
        agg_exprs = []
        for col, func in agg_columns.items():
            agg_exprs.append(f"{func}({col}) as {col}_{func.lower()}")
        agg_str = ", ".join(agg_exprs)
        
        return f"""
SELECT 
    {group_cols},
    {agg_str}
FROM {full_table}
GROUP BY {group_cols}
ORDER BY {group_columns[0]}
        """.strip()
    
    @staticmethod
    def pivot_table(table: str, row_col: str, pivot_col: str, 
                   value_col: str, agg_func: str = 'SUM', schema: str = None) -> str:
        """
        Generate PIVOT table query (PostgreSQL CROSSTAB style)
        
        Args:
            table: Table name
            row_col: Row identifier column
            pivot_col: Column to pivot on
            value_col: Column with values to aggregate
            agg_func: Aggregation function
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        return f"""
SELECT 
    {row_col},
    {agg_func}(CASE WHEN {pivot_col} = 'value1' THEN {value_col} END) as value1,
    {agg_func}(CASE WHEN {pivot_col} = 'value2' THEN {value_col} END) as value2
    -- Add more CASE statements for each pivot value
FROM {full_table}
GROUP BY {row_col}
ORDER BY {row_col}
        """.strip()
    
    # ═══════════════════════════════════════════════════════════
    # WINDOW FUNCTION TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def row_number_ranking(table: str, partition_col: str, order_col: str,
                          order_desc: bool = True, schema: str = None) -> str:
        """
        Generate ROW_NUMBER ranking query
        
        Args:
            table: Table name
            partition_col: Column to partition by
            order_col: Column to order by
            order_desc: DESC if True, ASC if False
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        order_direction = "DESC" if order_desc else "ASC"
        
        return f"""
SELECT 
    *,
    ROW_NUMBER() OVER (
        PARTITION BY {partition_col} 
        ORDER BY {order_col} {order_direction}
    ) as row_num
FROM {full_table}
        """.strip()
    
    @staticmethod
    def running_total(table: str, partition_col: str, order_col: str,
                     sum_col: str, schema: str = None) -> str:
        """
        Generate running total (cumulative sum) query
        
        Args:
            table: Table name
            partition_col: Column to partition by
            order_col: Column to order by
            sum_col: Column to sum
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        return f"""
SELECT 
    *,
    SUM({sum_col}) OVER (
        PARTITION BY {partition_col} 
        ORDER BY {order_col}
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM {full_table}
        """.strip()
    
    @staticmethod
    def lag_lead_comparison(table: str, partition_col: str, order_col: str,
                           value_col: str, lag_offset: int = 1, schema: str = None) -> str:
        """
        Generate LAG/LEAD comparison query
        
        Args:
            table: Table name
            partition_col: Column to partition by
            order_col: Column to order by
            value_col: Value column to compare
            lag_offset: Number of rows to offset
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        return f"""
SELECT 
    *,
    LAG({value_col}, {lag_offset}) OVER (
        PARTITION BY {partition_col} 
        ORDER BY {order_col}
    ) as previous_value,
    LEAD({value_col}, {lag_offset}) OVER (
        PARTITION BY {partition_col} 
        ORDER BY {order_col}
    ) as next_value,
    {value_col} - LAG({value_col}, {lag_offset}) OVER (
        PARTITION BY {partition_col} 
        ORDER BY {order_col}
    ) as change_from_previous
FROM {full_table}
        """.strip()
    
    # ═══════════════════════════════════════════════════════════
    # JOIN TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def inner_join(left_table: str, right_table: str, join_col: str,
                  left_cols: List[str] = None, right_cols: List[str] = None,
                  left_schema: str = None, right_schema: str = None) -> str:
        """
        Generate INNER JOIN query
        
        Args:
            left_table: Left table name
            right_table: Right table name
            join_col: Column to join on
            left_cols: Columns to select from left table
            right_cols: Columns to select from right table
            left_schema: Left table schema
            right_schema: Right table schema
            
        Returns:
            SQL query string
        """
        left_full = f"{left_schema}.{left_table}" if left_schema else left_table
        right_full = f"{right_schema}.{right_table}" if right_schema else right_table
        
        left_select = f"l.{', l.'.join(left_cols)}" if left_cols else "l.*"
        right_select = f"r.{', r.'.join(right_cols)}" if right_cols else "r.*"
        
        return f"""
SELECT 
    {left_select},
    {right_select}
FROM {left_full} l
INNER JOIN {right_full} r ON l.{join_col} = r.{join_col}
        """.strip()
    
    # ═══════════════════════════════════════════════════════════
    # DATA QUALITY TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def deduplicate_with_priority(table: str, partition_col: str, 
                                  priority_col: str, order_desc: bool = True,
                                  schema: str = None) -> str:
        """
        Deduplicate rows keeping highest/lowest priority
        
        Args:
            table: Table name
            partition_col: Column to deduplicate on
            priority_col: Column to determine which record to keep
            order_desc: Keep highest if True, lowest if False
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        order_direction = "DESC" if order_desc else "ASC"
        
        return f"""
WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY {partition_col} 
            ORDER BY {priority_col} {order_direction}
        ) as rn
    FROM {full_table}
)
SELECT * 
FROM ranked 
WHERE rn = 1
        """.strip()
    
    @staticmethod
    def null_summary_report(table: str, schema: str = None) -> str:
        """
        Generate NULL count report for all columns
        
        Args:
            table: Table name
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        return f"""
SELECT 
    column_name,
    COUNT(*) as total_rows,
    COUNT(column_name) as non_null_count,
    COUNT(*) - COUNT(column_name) as null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(column_name)) / COUNT(*), 2) as null_percentage
FROM {full_table}
CROSS JOIN (
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table}'
    {f"AND table_schema = '{schema}'" if schema else ''}
) cols
GROUP BY column_name
ORDER BY null_percentage DESC
        """.strip()
    
    # ═══════════════════════════════════════════════════════════
    # DATE/TIME TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def date_range_aggregation(table: str, date_col: str, group_by: str,
                              agg_col: str, agg_func: str = 'SUM',
                              schema: str = None) -> str:
        """
        Aggregate by date ranges (daily, monthly, yearly)
        
        Args:
            table: Table name
            date_col: Date column
            group_by: 'day', 'month', 'year', 'quarter'
            agg_col: Column to aggregate
            agg_func: Aggregation function
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        date_trunc = {
            'day': f"DATE_TRUNC('day', {date_col})",
            'month': f"DATE_TRUNC('month', {date_col})",
            'year': f"DATE_TRUNC('year', {date_col})",
            'quarter': f"DATE_TRUNC('quarter', {date_col})"
        }.get(group_by, f"DATE_TRUNC('day', {date_col})")
        
        return f"""
SELECT 
    {date_trunc} as period,
    {agg_func}({agg_col}) as {agg_col}_{agg_func.lower()}
FROM {full_table}
GROUP BY period
ORDER BY period
        """.strip()
    
    # ═══════════════════════════════════════════════════════════
    # INCREMENTAL LOAD TEMPLATES
    # ═══════════════════════════════════════════════════════════
    
    @staticmethod
    def incremental_extract(table: str, timestamp_col: str, 
                           last_extracted: str, schema: str = None) -> str:
        """
        Extract records modified after last extraction
        
        Args:
            table: Table name
            timestamp_col: Timestamp column for incremental load
            last_extracted: Last extraction timestamp (ISO format)
            schema: Schema name
            
        Returns:
            SQL query string
        """
        full_table = f"{schema}.{table}" if schema else table
        
        return f"""
SELECT *
FROM {full_table}
WHERE {timestamp_col} > '{last_extracted}'
ORDER BY {timestamp_col}
        """.strip()
    
    @staticmethod
    def upsert_template(target_table: str, source_table: str, 
                       primary_keys: List[str], update_cols: List[str],
                       schema: str = None) -> str:
        """
        Generate UPSERT (INSERT ... ON CONFLICT) query
        
        Args:
            target_table: Target table name
            source_table: Source table name
            primary_keys: List of primary key columns
            update_cols: List of columns to update on conflict
            schema: Schema name
            
        Returns:
            SQL query string
        """
        target_full = f"{schema}.{target_table}" if schema else target_table
        source_full = f"{schema}.{source_table}" if schema else source_table
        
        pk_constraint = ", ".join(primary_keys)
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        
        return f"""
INSERT INTO {target_full}
SELECT * FROM {source_full}
ON CONFLICT ({pk_constraint})
DO UPDATE SET {update_set}
        """.strip()


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    templates = SQLTemplates()
    
    # Example 1: Group by aggregation
    query1 = templates.group_by_aggregate(
        table='sales',
        group_columns=['store_key', 'product_category'],
        agg_columns={'quantity': 'SUM', 'order_number': 'COUNT'},
        schema='etl_output'
    )
    print("GROUP BY Query:\n", query1, "\n")
    
    # Example 2: Row number ranking
    query2 = templates.row_number_ranking(
        table='customers',
        partition_col='country',
        order_col='age',
        order_desc=True,
        schema='etl_output'
    )
    print("ROW_NUMBER Query:\n", query2, "\n")
    
    # Example 3: Deduplication
    query3 = templates.deduplicate_with_priority(
        table='sales',
        partition_col='order_number',
        priority_col='order_date',
        order_desc=True,
        schema='etl_output'
    )
    print("DEDUPLICATE Query:\n", query3, "\n")
