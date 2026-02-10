"""
Data Models for ETL Pipeline
Pydantic models for data validation and schema enforcement
"""

from pydantic import BaseModel, Field, EmailStr, validator, field_validator
from typing import Optional, List
from datetime import datetime, date
from enum import Enum


class CustomerStatus(str, Enum):
    """Customer status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class CustomerModel(BaseModel):
    """
    Customer data model with validation
    """
    name: str = Field(..., min_length=1, max_length=100, description="Customer full name")
    email: EmailStr = Field(..., description="Customer email address")
    age: int = Field(..., ge=0, le=120, description="Customer age")
    last_order_date: Optional[datetime] = Field(None, description="Date of last order")
    total_spent: Optional[float] = Field(None, ge=0, description="Total amount spent")
    status: Optional[CustomerStatus] = Field(CustomerStatus.ACTIVE, description="Customer status")
    
    @field_validator('name')
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        """Validate that name is not empty or only whitespace"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip().upper()
    
    @field_validator('age')
    @classmethod
    def validate_age(cls, v: int) -> int:
        """Validate age is within reasonable range"""
        if v < 0:
            raise ValueError('Age cannot be negative')
        if v > 120:
            raise ValueError('Age cannot exceed 120')
        return v
    
    class Config:
        """Pydantic configuration"""
        json_schema_extra = {
            "example": {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "age": 30,
                "last_order_date": "2025-01-15T10:30:00",
                "total_spent": 1500.50,
                "status": "active"
            }
        }


class ProductModel(BaseModel):
    """
    Product data model with validation
    """
    product_id: str = Field(..., min_length=1, description="Product ID")
    product_name: str = Field(..., min_length=1, max_length=200, description="Product name")
    category: str = Field(..., min_length=1, description="Product category")
    price: float = Field(..., gt=0, description="Product price")
    stock_quantity: int = Field(..., ge=0, description="Available stock")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    
    @field_validator('price')
    @classmethod
    def validate_price(cls, v: float) -> float:
        """Validate price is positive"""
        if v <= 0:
            raise ValueError('Price must be greater than 0')
        return round(v, 2)
    
    @field_validator('product_name')
    @classmethod
    def clean_product_name(cls, v: str) -> str:
        """Clean and validate product name"""
        return v.strip().title()


class OrderModel(BaseModel):
    """
    Order data model with validation
    """
    order_id: str = Field(..., min_length=1, description="Order ID")
    customer_email: EmailStr = Field(..., description="Customer email")
    product_id: str = Field(..., description="Product ID")
    quantity: int = Field(..., gt=0, description="Order quantity")
    order_date: datetime = Field(..., description="Order date")
    total_amount: float = Field(..., gt=0, description="Total order amount")
    
    @field_validator('quantity')
    @classmethod
    def validate_quantity(cls, v: int) -> int:
        """Validate quantity is positive"""
        if v <= 0:
            raise ValueError('Quantity must be greater than 0')
        return v
    
    @field_validator('total_amount')
    @classmethod
    def validate_total_amount(cls, v: float) -> float:
        """Validate and round total amount"""
        if v <= 0:
            raise ValueError('Total amount must be greater than 0')
        return round(v, 2)


class DataQualityMetrics(BaseModel):
    """
    Data quality metrics model
    """
    total_records: int = Field(..., ge=0, description="Total number of records")
    valid_records: int = Field(..., ge=0, description="Number of valid records")
    invalid_records: int = Field(..., ge=0, description="Number of invalid records")
    duplicate_records: int = Field(0, ge=0, description="Number of duplicate records")
    missing_values: dict = Field(default_factory=dict, description="Missing values per column")
    validation_errors: List[str] = Field(default_factory=list, description="List of validation errors")
    
    @property
    def validity_rate(self) -> float:
        """Calculate validity rate percentage"""
        if self.total_records == 0:
            return 0.0
        return round((self.valid_records / self.total_records) * 100, 2)
    
    @property
    def duplicate_rate(self) -> float:
        """Calculate duplicate rate percentage"""
        if self.total_records == 0:
            return 0.0
        return round((self.duplicate_records / self.total_records) * 100, 2)


class ETLJobMetadata(BaseModel):
    """
    ETL job metadata model
    """
    job_id: str = Field(..., description="Unique job identifier")
    job_name: str = Field(..., description="Job name")
    start_time: datetime = Field(..., description="Job start time")
    end_time: Optional[datetime] = Field(None, description="Job end time")
    status: str = Field(..., description="Job status (running, completed, failed)")
    records_processed: int = Field(0, ge=0, description="Number of records processed")
    records_failed: int = Field(0, ge=0, description="Number of records failed")
    error_message: Optional[str] = Field(None, description="Error message if job failed")
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate job duration in seconds"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return round((self.records_processed / total) * 100, 2)


# Validation functions
def validate_customer_data(data: dict) -> tuple[bool, Optional[CustomerModel], Optional[str]]:
    """
    Validate customer data against CustomerModel
    
    Args:
        data: Dictionary containing customer data
        
    Returns:
        Tuple of (is_valid, model_instance, error_message)
    """
    try:
        customer = CustomerModel(**data)
        return True, customer, None
    except Exception as e:
        return False, None, str(e)


def validate_product_data(data: dict) -> tuple[bool, Optional[ProductModel], Optional[str]]:
    """
    Validate product data against ProductModel
    
    Args:
        data: Dictionary containing product data
        
    Returns:
        Tuple of (is_valid, model_instance, error_message)
    """
    try:
        product = ProductModel(**data)
        return True, product, None
    except Exception as e:
        return False, None, str(e)


def validate_dataframe_customers(df) -> DataQualityMetrics:
    """
    Validate entire DataFrame against CustomerModel
    
    Args:
        df: Pandas DataFrame with customer data
        
    Returns:
        DataQualityMetrics object with validation results
    """
    import pandas as pd
    
    total_records = len(df)
    valid_records = 0
    invalid_records = 0
    validation_errors = []
    
    for idx, row in df.iterrows():
        is_valid, _, error = validate_customer_data(row.to_dict())
        if is_valid:
            valid_records += 1
        else:
            invalid_records += 1
            validation_errors.append(f"Row {idx}: {error}")
    
    # Calculate missing values
    missing_values = df.isnull().sum().to_dict()
    
    # Count duplicates
    duplicate_records = df.duplicated().sum()
    
    return DataQualityMetrics(
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        duplicate_records=duplicate_records,
        missing_values=missing_values,
        validation_errors=validation_errors[:10]  # Limit to first 10 errors
    )


# Example usage and testing
if __name__ == "__main__":
    print("="*70)
    print("Data Models - Pydantic Validation Demo")
    print("="*70 + "\n")
    
    # Test CustomerModel
    print("Testing CustomerModel:")
    print("-" * 50)
    
    # Valid customer
    valid_customer_data = {
        "name": "  john doe  ",
        "email": "john@example.com",
        "age": 30,
        "last_order_date": "2025-01-15T10:30:00",
        "total_spent": 1500.50
    }
    
    try:
        customer = CustomerModel(**valid_customer_data)
        print(f"✓ Valid customer created:")
        print(f"  Name: {customer.name}")
        print(f"  Email: {customer.email}")
        print(f"  Age: {customer.age}")
        print(f"  Status: {customer.status}")
    except Exception as e:
        print(f"✗ Validation error: {e}")
    
    # Invalid customer (bad email)
    print("\n\nTesting invalid customer data:")
    print("-" * 50)
    
    invalid_customer_data = {
        "name": "Jane Doe",
        "email": "invalid-email",  # Invalid email format
        "age": 25
    }
    
    try:
        customer = CustomerModel(**invalid_customer_data)
        print(f"✓ Valid customer created: {customer.name}")
    except Exception as e:
        print(f"✗ Validation error (expected): {e}")
    
    # Test ProductModel
    print("\n\nTesting ProductModel:")
    print("-" * 50)
    
    product_data = {
        "product_id": "PROD001",
        "product_name": "  laptop computer  ",
        "category": "Electronics",
        "price": 999.99,
        "stock_quantity": 50
    }
    
    try:
        product = ProductModel(**product_data)
        print(f"✓ Valid product created:")
        print(f"  ID: {product.product_id}")
        print(f"  Name: {product.product_name}")
        print(f"  Price: ${product.price}")
        print(f"  Stock: {product.stock_quantity}")
    except Exception as e:
        print(f"✗ Validation error: {e}")
    
    # Test DataQualityMetrics
    print("\n\nTesting DataQualityMetrics:")
    print("-" * 50)
    
    metrics = DataQualityMetrics(
        total_records=100,
        valid_records=85,
        invalid_records=15,
        duplicate_records=5,
        missing_values={"age": 10, "email": 3}
    )
    
    print(f"Total Records: {metrics.total_records}")
    print(f"Valid Records: {metrics.valid_records}")
    print(f"Invalid Records: {metrics.invalid_records}")
    print(f"Validity Rate: {metrics.validity_rate}%")
    print(f"Duplicate Rate: {metrics.duplicate_rate}%")
    print(f"Missing Values: {metrics.missing_values}")
    
    # Test ETLJobMetadata
    print("\n\nTesting ETLJobMetadata:")
    print("-" * 50)
    
    job_metadata = ETLJobMetadata(
        job_id="JOB_20250112_001",
        job_name="Customer ETL",
        start_time=datetime(2025, 1, 12, 10, 0, 0),
        end_time=datetime(2025, 1, 12, 10, 15, 30),
        status="completed",
        records_processed=950,
        records_failed=50
    )
    
    print(f"Job ID: {job_metadata.job_id}")
    print(f"Job Name: {job_metadata.job_name}")
    print(f"Status: {job_metadata.status}")
    print(f"Duration: {job_metadata.duration_seconds} seconds")
    print(f"Success Rate: {job_metadata.success_rate}%")
    
    print("\n" + "="*70)
    print("✓ All data model tests completed!")
    print("="*70 + "\n")
