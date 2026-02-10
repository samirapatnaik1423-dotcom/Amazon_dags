"""
Unit Tests for ETL Pipeline Components
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from cleaning_utils import DataCleaner
from config_loader import ConfigLoader


class TestDataCleaner:
    """Tests for DataCleaner utility class"""
    
    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing"""
        data = {
            'name': ['  John  ', 'Jane', '  Bob  ', 'Alice'],
            'age': [25, np.nan, 35, 28],
            'email': ['john@test.com', 'jane@test.com', 'invalid', 'alice@test.com'],
            'salary': [50000, 60000, 55000, 200000]
        }
        return pd.DataFrame(data)
    
    def test_trim_whitespace(self, sample_df):
        """Test trimming whitespace from string columns"""
        df = DataCleaner.trim_whitespace(sample_df)
        assert df['name'].iloc[0] == 'John'
        assert df['name'].iloc[2] == 'Bob'
        assert not any(df['name'].str.startswith(' '))
        assert not any(df['name'].str.endswith(' '))
    
    def test_fill_missing_mean(self, sample_df):
        """Test filling missing values with mean"""
        df = DataCleaner.fill_missing_mean(sample_df, ['age'])
        assert df['age'].isnull().sum() == 0
        expected_mean = (25 + 35 + 28) / 3
        assert df['age'].iloc[1] == expected_mean
    
    def test_fill_missing_median(self, sample_df):
        """Test filling missing values with median"""
        df = DataCleaner.fill_missing_median(sample_df, ['age'])
        assert df['age'].isnull().sum() == 0
        assert df['age'].iloc[1] == 28  # median of [25, 28, 35]
    
    def test_remove_duplicates(self):
        """Test duplicate removal"""
        data = {
            'email': ['a@test.com', 'b@test.com', 'a@test.com'],
            'name': ['Alice', 'Bob', 'Alice']
        }
        df = pd.DataFrame(data)
        df_clean = DataCleaner.remove_duplicates(df, subset=['email'])
        assert len(df_clean) == 2
        assert list(df_clean['email']) == ['a@test.com', 'b@test.com']
    
    def test_typecast_column(self, sample_df):
        """Test type casting"""
        df = sample_df.copy()
        df['age'] = df['age'].fillna(30)
        df = DataCleaner.typecast_column(df, 'age', 'int')
        assert df['age'].dtype == 'Int64' or df['age'].dtype == 'int64'
    
    def test_validate_email(self, sample_df):
        """Test email validation"""
        df = DataCleaner.validate_email(sample_df, 'email', drop_invalid=True)
        assert len(df) == 3  # Should drop the 'invalid' email
        assert 'invalid' not in df['email'].values
    
    def test_remove_empty_strings(self):
        """Test empty string removal"""
        data = {
            'name': ['John', '  ', 'Jane', ''],
            'age': [25, 30, 35, 40]
        }
        df = pd.DataFrame(data)
        df = DataCleaner.remove_empty_strings(df)
        assert df['name'].isnull().sum() == 2
    
    def test_detect_outliers_iqr(self, sample_df):
        """Test outlier detection"""
        outliers = DataCleaner.detect_outliers_iqr(sample_df, 'salary')
        # 200000 should be detected as outlier
        assert outliers.iloc[3] == True
        assert outliers.iloc[0] == False
    
    def test_remove_outliers_iqr(self, sample_df):
        """Test outlier removal"""
        df = DataCleaner.remove_outliers_iqr(sample_df, ['salary'])
        assert len(df) == 3  # Should remove 200000
        assert 200000 not in df['salary'].values
    
    def test_handle_missing_data_drop(self, sample_df):
        """Test dropping missing data"""
        df = DataCleaner.handle_missing_data(sample_df, strategy='drop', columns=['age'])
        assert df['age'].isnull().sum() == 0
        assert len(df) == 3  # One row should be dropped
    
    def test_handle_missing_data_mean(self, sample_df):
        """Test filling with mean"""
        df = DataCleaner.handle_missing_data(sample_df, strategy='mean', columns=['age'])
        assert df['age'].isnull().sum() == 0
        expected_mean = (25 + 35 + 28) / 3
        assert df['age'].iloc[1] == expected_mean


class TestConfigLoader:
    """Tests for ConfigLoader utility class"""
    
    def test_load_yaml(self, tmp_path):
        """Test loading YAML configuration"""
        config_file = tmp_path / "test_config.yaml"
        config_content = """
        test_key: test_value
        nested:
          key1: value1
          key2: value2
        """
        config_file.write_text(config_content)
        
        config = ConfigLoader.load_yaml(config_file)
        assert config['test_key'] == 'test_value'
        assert config['nested']['key1'] == 'value1'
    
    def test_load_json(self, tmp_path):
        """Test loading JSON configuration"""
        config_file = tmp_path / "test_config.json"
        config_content = '{"test_key": "test_value", "number": 42}'
        config_file.write_text(config_content)
        
        config = ConfigLoader.load_json(config_file)
        assert config['test_key'] == 'test_value'
        assert config['number'] == 42
    
    def test_load_nonexistent_file(self):
        """Test loading non-existent file raises error"""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_yaml("nonexistent.yaml")


class TestDataTransformation:
    """Integration tests for data transformation pipeline"""
    
    @pytest.fixture
    def customer_data(self):
        """Create sample customer data"""
        return pd.DataFrame({
            'name': ['  John Doe  ', 'Jane Smith', '  Bob  ', 'John Doe'],
            'email': ['john@test.com', 'jane@test.com', 'invalid', 'john@test.com'],
            'age': [25, np.nan, 35, 25],
            'last_order_date': ['2025-01-15', '2025-02-20', '2025-03-10', '2025-01-15']
        })
    
    def test_complete_cleaning_pipeline(self, customer_data):
        """Test complete cleaning pipeline"""
        df = customer_data.copy()
        
        # Apply cleaning steps
        df = DataCleaner.trim_whitespace(df)
        df = DataCleaner.fill_missing_mean(df, ['age'])
        df = DataCleaner.validate_email(df, 'email', drop_invalid=True)
        df = DataCleaner.remove_duplicates(df, subset=['email'])
        
        # Assertions
        assert len(df) == 2  # After removing duplicates and invalid email
        assert df['age'].isnull().sum() == 0
        assert not any(df['name'].str.startswith(' '))
        assert all(df['email'].str.contains('@'))
    
    def test_data_quality_improvement(self, customer_data):
        """Test that cleaning improves data quality"""
        df_original = customer_data.copy()
        df_cleaned = customer_data.copy()
        
        # Apply cleaning
        df_cleaned = DataCleaner.trim_whitespace(df_cleaned)
        df_cleaned = DataCleaner.fill_missing_mean(df_cleaned, ['age'])
        df_cleaned = DataCleaner.remove_duplicates(df_cleaned, subset=['email'])
        
        # Quality metrics
        original_missing = df_original.isnull().sum().sum()
        cleaned_missing = df_cleaned.isnull().sum().sum()
        
        assert cleaned_missing < original_missing
        assert len(df_cleaned) <= len(df_original)


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
