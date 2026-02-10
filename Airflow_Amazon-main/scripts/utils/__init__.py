# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - ETL Pipeline Utilities Package
# Tasks: T0008, T0012-T0022, T0031 (Sprints 2-4, 6)
# ═══════════════════════════════════════════════════════════════════════

"""
ETL Pipeline Utilities Package - REUSABLE UTILITIES ONLY

This package contains GENERIC, REUSABLE utilities that can be applied 
to any ETL project. Project-specific logic is in the main scripts:
- Extract.py: Data extraction from sources
- Transform.py: Data cleaning & transformation
- Load.py: Database loading
- ReportGenerator.py: Report generation

REUSABLE UTILITIES:

Phase 2 - Data Quality & Validation:
- validation_utils: Generic data validation framework (T0008, T0012)

Phase 3 - Transformations:
- aggregation_utils: Generic GroupBy aggregations (T0013)
- normalization_utils: Z-score normalization (T0014)
- feature_engineering_utils: Derived feature creation (T0015)
- datetime_utils: Date/time transformations (T0016)
- transformation_orchestrator: Pipeline orchestration (T0017)

Phase 4 - Loading:
- bulk_loader: High-performance batch database loading (T0018)
- load_strategy: Full vs Incremental load logic (T0019)
- constraint_handler: Pre-load constraint validation (T0020)
- upsert_handler: Insert/Update/Upsert logic (T0021)
- rejected_records_handler: Error table & tracking (T0022)

Phase 6 - Pipeline Tracking:
- dag_execution_tracker: DAG execution summary (T0031)

MOVED TO MAIN SCRIPTS:
- table_cleaners → Transform.py (project-specific)
- report_generators → ReportGenerator.py (project-specific)
"""

# Phase 2 - Data Quality (Generic)
# NOTE: Lazy imports to prevent DAG import timeout
# Import these modules only when actually needed in your code:
#   from utils.validation_utils import DataValidator
#   from utils.aggregation_utils import AggregationEngine
# etc.

# Uncomment below for direct imports (may cause DAG import timeouts):
# from .validation_utils import DataValidator
# from .aggregation_utils import AggregationEngine
# from .normalization_utils import NormalizationEngine
# from .feature_engineering_utils import FeatureEngine
# from .datetime_utils import DateTimeEngine
from .transformation_orchestrator import TransformationOrchestrator

# Phase 4 - Loading (Generic)
from .bulk_loader import BulkLoader
from .load_strategy import LoadStrategy, LoadType
from .constraint_handler import ConstraintHandler
from .upsert_handler import UpsertHandler
from .rejected_records_handler import RejectedRecordsHandler

# Phase 6 - Pipeline Tracking (Generic)
from .dag_execution_tracker import DAGExecutionSummary

__all__ = [
    # Phase 2 - Generic Validation
    'DataValidator',
    
    # Phase 3 - Generic Transformations
    'AggregationEngine',
    'NormalizationEngine',
    'FeatureEngine',
    'DateTimeEngine',
    'TransformationOrchestrator',
    
    # Phase 4 - Generic Loading
    'BulkLoader',
    'LoadStrategy', 'LoadType',
    'ConstraintHandler',
    'UpsertHandler',
    'RejectedRecordsHandler',
    
    # Phase 6 - Generic Tracking
    'DAGExecutionSummary',
]
