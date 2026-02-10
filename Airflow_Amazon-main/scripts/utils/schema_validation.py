# ═══════════════════════════════════════════════════════════════════════
# PHASE 4 - Schema Validation (Pydantic)
# ═══════════════════════════════════════════════════════════════════════

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class CustomersSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    customer_key: int = Field(alias="CustomerKey")
    name: Optional[str] = Field(default=None, alias="Name")
    email: Optional[str] = Field(default=None, alias="Email")
    birthday: Optional[date] = Field(default=None, alias="Birthday")
    age: Optional[int] = Field(default=None, alias="Age")
    city: Optional[str] = Field(default=None, alias="City")
    country: Optional[str] = Field(default=None, alias="Country")
    loyalty_category: Optional[str] = Field(default=None, alias="loyalty_category")


class SalesSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    order_number: int = Field(alias="Order Number")
    customer_key: Optional[int] = Field(default=None, alias="CustomerKey")
    product_key: Optional[int] = Field(default=None, alias="ProductKey")
    store_key: Optional[int] = Field(default=None, alias="StoreKey")
    quantity: Optional[int] = Field(default=None, alias="Quantity")
    total_amount_usd: Optional[float] = Field(default=None, alias="Total_Amount_USD")
    delivery_status: Optional[str] = Field(default=None, alias="Delivery_Status")
    order_date: Optional[date] = Field(default=None, alias="Order Date")
    delivery_date: Optional[date] = Field(default=None, alias="Delivery Date")


class ProductsSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    product_key: int = Field(alias="ProductKey")
    product_name: Optional[str] = Field(default=None, alias="Product Name")
    category: Optional[str] = Field(default=None, alias="Category")
    unit_price_usd: Optional[float] = Field(default=None, alias="Unit Price USD")


class StoresSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    store_key: int = Field(alias="StoreKey")
    country: Optional[str] = Field(default=None, alias="Country")
    state: Optional[str] = Field(default=None, alias="State")


class ExchangeRatesSchema(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    date_value: date = Field(alias="Date")
    currency: str = Field(alias="Currency")
    exchange: Optional[float] = Field(default=None, alias="Exchange")


MODEL_MAP: Dict[str, Type[BaseModel]] = {
    "customers_cleaned": CustomersSchema,
    "sales_cleaned": SalesSchema,
    "products_cleaned": ProductsSchema,
    "stores_cleaned": StoresSchema,
    "exchange_rates_cleaned": ExchangeRatesSchema,
}


def validate_dataframe(
    table_key: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    model = MODEL_MAP.get(table_key)
    if not model:
        return {
            "status": "skipped",
            "error_count": 0,
            "errors": [],
        }

    errors: List[Dict[str, Any]] = []
    for row_index, row in enumerate(rows):
        try:
            model.model_validate(row)
        except ValidationError as exc:
            for err in exc.errors():
                errors.append({
                    "row_index": row_index,
                    "field": ".".join(str(part) for part in err.get("loc", [])),
                    "message": err.get("msg", "validation error"),
                    "type": err.get("type", "validation_error"),
                })

    status = "pass" if not errors else "fail"
    return {
        "status": status,
        "error_count": len(errors),
        "errors": errors,
    }
