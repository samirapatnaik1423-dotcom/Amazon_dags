"""
Phase 1 Ingestion Tests
TEAM 2 - SPRINT 1 (PHASE 1)
Comprehensive test suite for multi-format ingestion
"""

import os
import sys
import pytest
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.ingest_json import JSONIngestion
from scripts.ingest_sql import SQLIngestion
from scripts.ingest_api import APIIngestion
from scripts.file_watcher import FileWatcher


class TestJSONIngestion:
    """Test JSON/JSONL ingestion functionality"""
    
    def test_json_ingestion_basic(self, tmp_path):
        """Test basic JSON file ingestion"""
        
        # Create test JSON file
        test_data = [
            {"id": 1, "name": "Test1", "value": 100},
            {"id": 2, "name": "Test2", "value": 200}
        ]
        
        json_file = tmp_path / "test.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Test ingestion
        ingestion = JSONIngestion()
        df = ingestion.ingest_json(str(json_file), table_name="test_json")
        
        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ['id', 'name', 'value']
        print("✅ JSON basic ingestion test passed")
    
    def test_jsonl_ingestion(self, tmp_path):
        """Test JSONL file ingestion"""
        
        # Create test JSONL file
        test_data = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"}
        ]
        
        jsonl_file = tmp_path / "test.jsonl"
        with open(jsonl_file, 'w') as f:
            for record in test_data:
                f.write(json.dumps(record) + '\n')
        
        # Test ingestion
        ingestion = JSONIngestion()
        df = ingestion.ingest_json(str(jsonl_file), table_name="test_jsonl", is_jsonl=True)
        
        assert df is not None
        assert len(df) == 2
        print("✅ JSONL ingestion test passed")
    
    def test_nested_json_normalization(self, tmp_path):
        """Test nested JSON structure normalization"""
        
        test_data = {
            "user": {
                "id": 1,
                "name": "John",
                "address": {
                    "city": "New York",
                    "zip": "10001"
                }
            }
        }
        
        json_file = tmp_path / "nested.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Test ingestion with normalization
        ingestion = JSONIngestion()
        df = ingestion.ingest_json(str(json_file), table_name="test_nested", normalize=True)
        
        assert df is not None
        assert 'user.address.city' in df.columns or 'city' in df.columns
        print("✅ Nested JSON normalization test passed")


class TestSQLIngestion:
    """Test SQL database ingestion functionality"""
    
    def test_sql_extraction(self):
        """Test SQL database extraction"""
        
        # Test with Airflow database
        connection_string = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"
        
        ingestion = SQLIngestion()
        
        # Test simple extraction
        df = ingestion.extract_table(
            connection_string=connection_string,
            table_name="ab_user",
            schema="public",
            output_path="data/staging/test_sql_extract.csv"
        )
        
        assert df is not None
        assert len(df) > 0
        print(f"✅ SQL extraction test passed - {len(df)} rows extracted")
    
    def test_sql_with_query(self):
        """Test SQL extraction with custom query"""
        
        connection_string = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"
        
        ingestion = SQLIngestion()
        
        query = "SELECT id, username FROM ab_user LIMIT 5"
        
        df = ingestion.extract_with_query(
            connection_string=connection_string,
            query=query,
            output_path="data/staging/test_sql_query.csv"
        )
        
        assert df is not None
        assert len(df) <= 5
        print(f"✅ SQL query extraction test passed - {len(df)} rows")


class TestAPIIngestion:
    """Test API ingestion functionality"""
    
    def test_api_ingestion_public(self):
        """Test API ingestion with public endpoint"""
        
        ingestion = APIIngestion()
        
        # Test JSONPlaceholder API
        df = ingestion.ingest_from_api(
            base_url="https://jsonplaceholder.typicode.com",
            endpoint="/posts",
            table_name="test_api_posts"
        )
        
        assert df is not None
        assert len(df) > 0
        assert 'id' in df.columns
        assert 'title' in df.columns
        print(f"✅ API ingestion test passed - {len(df)} records fetched")
    
    def test_api_with_pagination(self):
        """Test API with pagination"""
        
        ingestion = APIIngestion()
        
        # Test with pagination parameters
        df = ingestion.ingest_from_api(
            base_url="https://jsonplaceholder.typicode.com",
            endpoint="/posts",
            table_name="test_api_paginated",
            pagination_config={
                "enabled": True,
                "page_param": "_page",
                "limit_param": "_limit",
                "page_size": 10,
                "max_pages": 2
            }
        )
        
        assert df is not None
        assert len(df) >= 10
        print(f"✅ API pagination test passed - {len(df)} records")


class TestFileWatcher:
    """Test file watcher functionality"""
    
    def test_file_detection(self, tmp_path):
        """Test file detection"""
        
        # Create test files
        test_files = [
            tmp_path / "test1.csv",
            tmp_path / "test2.json",
            tmp_path / "test3.jsonl"
        ]
        
        for file_path in test_files:
            file_path.write_text("test content")
        
        # Test file detection
        watcher = FileWatcher()
        detected_files = watcher.scan_directory(str(tmp_path), patterns=['*.csv', '*.json', '*.jsonl'])
        
        assert len(detected_files) == 3
        print(f"✅ File detection test passed - {len(detected_files)} files detected")
    
    def test_duplicate_detection(self, tmp_path):
        """Test duplicate file detection via hash"""
        
        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name\n1,Test")
        
        watcher = FileWatcher()
        
        # First scan
        hash1 = watcher.calculate_file_hash(str(test_file))
        
        # Second scan (same content)
        hash2 = watcher.calculate_file_hash(str(test_file))
        
        assert hash1 == hash2
        print("✅ Duplicate detection test passed")


class TestEndToEnd:
    """End-to-end integration tests"""
    
    def test_json_to_database_flow(self, tmp_path):
        """Test complete JSON ingestion to database flow"""
        
        # Create test JSON
        test_data = [
            {"product_id": 1, "product_name": "Test Product", "price": 99.99},
            {"product_id": 2, "product_name": "Another Product", "price": 149.99}
        ]
        
        json_file = tmp_path / "products.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Ingest JSON
        ingestion = JSONIngestion()
        df = ingestion.ingest_json(str(json_file), table_name="test_products")
        
        # Verify data
        assert df is not None
        assert len(df) == 2
        assert df['price'].sum() == 249.98
        
        print("✅ End-to-end JSON flow test passed")


def run_all_tests():
    """Run all Phase 1 tests"""
    
    print("\n" + "="*70)
    print("PHASE 1 - INGESTION TESTS")
    print("="*70 + "\n")
    
    # Run pytest
    pytest.main([
        __file__,
        '-v',
        '--tb=short',
        '-s'
    ])


if __name__ == "__main__":
    run_all_tests()
