"""
JSON Data Ingestion Module
TEAM 2 - SPRINT 1 (PHASE 1)
Handles JSON and JSONL file ingestion with nested structure support
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class JSONIngester:
    """
    Handles JSON and JSONL file ingestion with support for:
    - Standard JSON files (.json)
    - JSON Lines files (.jsonl)
    - Nested structure flattening
    - Encoding detection
    - Streaming for large files
    """
    
    def __init__(self, encoding: str = 'utf-8'):
        """
        Initialize JSON ingester
        
        Args:
            encoding: File encoding (default: utf-8)
        """
        self.encoding = encoding
        logger.info("✅ JSONIngester initialized")
    
    def read_json(self, file_path: Union[str, Path], normalize: bool = True) -> pd.DataFrame:
        """
        Read standard JSON file into DataFrame
        
        Args:
            file_path: Path to JSON file
            normalize: Flatten nested JSON structures
            
        Returns:
            DataFrame with JSON data
        """
        file_path = Path(file_path)
        logger.info(f"📖 Reading JSON file: {file_path}")
        
        try:
            if normalize:
                # Flatten nested JSON structures
                with open(file_path, 'r', encoding=self.encoding) as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                elif isinstance(data, dict):
                    df = pd.json_normalize([data])
                else:
                    raise ValueError(f"Unsupported JSON structure type: {type(data)}")
            else:
                # Simple read without normalization
                df = pd.read_json(file_path, encoding=self.encoding)
            
            logger.info(f"✅ Successfully read {len(df)} records from JSON")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading JSON file: {e}")
            raise
    
    def read_jsonl(self, file_path: Union[str, Path], chunk_size: Optional[int] = None) -> pd.DataFrame:
        """
        Read JSON Lines file (.jsonl) into DataFrame
        
        Args:
            file_path: Path to JSONL file
            chunk_size: Number of lines to read at once (for streaming)
            
        Returns:
            DataFrame with JSONL data
        """
        file_path = Path(file_path)
        logger.info(f"📖 Reading JSONL file: {file_path}")
        
        try:
            if chunk_size:
                # Stream large files in chunks
                records = []
                with open(file_path, 'r', encoding=self.encoding) as f:
                    for i, line in enumerate(f):
                        if i % chunk_size == 0 and i > 0:
                            logger.info(f"🔄 Processing chunk: {i} records")
                        records.append(json.loads(line))
                df = pd.DataFrame(records)
            else:
                # Read entire file
                df = pd.read_json(file_path, lines=True, encoding=self.encoding)
            
            logger.info(f"✅ Successfully read {len(df)} records from JSONL")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading JSONL file: {e}")
            raise
    
    def ingest_from_string(self, json_string: str, normalize: bool = True) -> pd.DataFrame:
        """
        Ingest JSON from string
        
        Args:
            json_string: JSON string
            normalize: Flatten nested structures
            
        Returns:
            DataFrame with JSON data
        """
        logger.info("📖 Ingesting JSON from string")
        
        try:
            data = json.loads(json_string)
            
            if normalize:
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                elif isinstance(data, dict):
                    df = pd.json_normalize([data])
                else:
                    raise ValueError(f"Unsupported JSON structure type: {type(data)}")
            else:
                df = pd.DataFrame(data)
            
            logger.info(f"✅ Successfully ingested {len(df)} records from JSON string")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error ingesting JSON string: {e}")
            raise
    
    def flatten_nested_json(self, df: pd.DataFrame, max_level: int = 3) -> pd.DataFrame:
        """
        Flatten nested columns in DataFrame
        
        Args:
            df: DataFrame with nested columns
            max_level: Maximum nesting level to flatten
            
        Returns:
            Flattened DataFrame
        """
        logger.info("🔄 Flattening nested JSON columns")
        
        try:
            # Identify nested columns
            nested_cols = []
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    nested_cols.append(col)
            
            if nested_cols:
                logger.info(f"📋 Found {len(nested_cols)} nested columns: {nested_cols}")
                df = pd.json_normalize(df.to_dict('records'), max_level=max_level)
            
            logger.info(f"✅ Flattening complete: {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error flattening JSON: {e}")
            raise
    
    def validate_json_schema(self, df: pd.DataFrame, required_fields: List[str]) -> bool:
        """
        Validate JSON DataFrame has required fields
        
        Args:
            df: DataFrame to validate
            required_fields: List of required column names
            
        Returns:
            True if valid, False otherwise
        """
        logger.info(f"🔍 Validating JSON schema for {len(required_fields)} required fields")
        
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            logger.warning(f"⚠️ Missing required fields: {missing_fields}")
            return False
        
        logger.info("✅ JSON schema validation passed")
        return True
    
    def save_to_csv(self, df: pd.DataFrame, output_path: Union[str, Path]) -> None:
        """
        Save ingested JSON data to CSV
        
        Args:
            df: DataFrame to save
            output_path: Output CSV path
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Saving to CSV: {output_path}")
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved {len(df)} records")
    
    def get_ingestion_metadata(self, df: pd.DataFrame, source_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate metadata about ingested JSON data
        
        Args:
            df: Ingested DataFrame
            source_file: Source file path
            
        Returns:
            Metadata dictionary
        """
        metadata = {
            'ingestion_timestamp': datetime.now().isoformat(),
            'source_file': str(source_file) if source_file else 'string',
            'format': 'JSON',
            'total_records': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'null_counts': df.isnull().sum().to_dict()
        }
        
        logger.info(f"📊 Ingestion metadata: {metadata['total_records']} records, {metadata['total_columns']} columns")
        return metadata


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    ingester = JSONIngester()
    
    # Example 1: Read standard JSON file
    # df = ingester.read_json('data/raw/sample.json', normalize=True)
    
    # Example 2: Read JSONL file with streaming
    # df = ingester.read_jsonl('data/raw/sample.jsonl', chunk_size=1000)
    
    # Example 3: Ingest from JSON string
    json_str = '''
    [
        {"id": 1, "name": "Product A", "price": 99.99, "category": {"id": 10, "name": "Electronics"}},
        {"id": 2, "name": "Product B", "price": 49.99, "category": {"id": 20, "name": "Books"}}
    ]
    '''
    df = ingester.ingest_from_string(json_str, normalize=True)
    print(df)
    
    # Get metadata
    metadata = ingester.get_ingestion_metadata(df, source_file="example.json")
    print("\nMetadata:", json.dumps(metadata, indent=2))
