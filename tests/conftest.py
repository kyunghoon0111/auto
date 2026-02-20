"""Shared test fixtures for SCM analytics tests."""
import os
import sys
from pathlib import Path

import pytest
import duckdb
import polars as pl

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import init_db
from src.config import AppConfig


@pytest.fixture
def config():
    """Load application config from project root."""
    return AppConfig(config_dir=Path(__file__).parent.parent / "config")


@pytest.fixture
def con(tmp_path):
    """In-memory-like DuckDB with full schema initialized."""
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    init_db(connection)
    yield connection
    connection.close()


@pytest.fixture
def sample_order_df():
    """Minimal valid fact_order DataFrame."""
    return pl.DataFrame({
        "source_system": ["OMS", "OMS"],
        "channel_order_id": ["ORD-001", "ORD-002"],
        "line_no": [1, 1],
        "order_date": ["2024-01-15", "2024-01-16"],
        "channel_store_id": ["STORE-A", "STORE-A"],
        "item_id": ["SKU-001", "SKU-002"],
        "qty_ordered": ["10", "20"],
    })


@pytest.fixture
def sample_order_korean_df():
    """fact_order with Korean column names."""
    return pl.DataFrame({
        "system": ["OMS", "OMS"],
        "주문번호": ["ORD-001", "ORD-002"],
        "라인": [1, 1],
        "주문일": ["2024-01-15", "2024-01-16"],
        "스토어id": ["STORE-A", "STORE-A"],
        "상품코드": ["SKU-001", "SKU-002"],
        "주문수량": ["10", "20"],
    })


@pytest.fixture
def sample_shipment_df():
    """Minimal valid fact_shipment DataFrame."""
    return pl.DataFrame({
        "source_system": ["WMS"],
        "shipment_id": ["SHP-001"],
        "ship_date": ["2024-01-16"],
        "warehouse_id": ["WH-01"],
        "item_id": ["SKU-001"],
        "qty_shipped": ["10"],
        "lot_id": ["LOT-A"],
    })


@pytest.fixture
def sample_inventory_df():
    """Minimal valid fact_inventory_snapshot DataFrame."""
    return pl.DataFrame({
        "source_system": ["WMS"],
        "snapshot_date": ["2024-01-20"],
        "warehouse_id": ["WH-01"],
        "item_id": ["SKU-001"],
        "lot_id": ["LOT-A"],
        "onhand_qty": ["100"],
        "expiry_date": ["2024-06-01"],
    })


@pytest.fixture
def sample_charge_df():
    """Minimal valid fact_charge_actual DataFrame."""
    return pl.DataFrame({
        "source_system": ["ERP"],
        "invoice_no": ["INV-001"],
        "invoice_line_no": [1],
        "charge_type": ["LAST_MILE_PARCEL"],
        "amount": ["1000"],
        "currency": ["KRW"],
        "period": ["2024-01"],
    })


@pytest.fixture
def sample_fx_df():
    """Minimal fact_exchange_rate DataFrame."""
    return pl.DataFrame({
        "period": ["2024-01"],
        "currency": ["USD"],
        "rate_to_krw": ["1300"],
    })


@pytest.fixture
def sample_cost_structure_df():
    """Minimal fact_cost_structure DataFrame."""
    return pl.DataFrame({
        "item_id": ["SKU-001", "SKU-001"],
        "cost_component": ["MATERIAL", "MATERIAL"],
        "effective_from": ["2024-01-01", "2024-02-01"],
        "cost_per_unit_krw": ["5000", "5500"],
    })


@pytest.fixture
def sample_po_df():
    """Minimal fact_po DataFrame."""
    return pl.DataFrame({
        "source_system": ["ERP"],
        "po_id": ["PO-001"],
        "po_date": ["2024-01-10"],
        "supplier_id": ["SUP-001"],
        "item_id": ["SKU-001"],
        "qty_ordered": ["100"],
        "eta_date": ["2024-01-20"],
    })


@pytest.fixture
def sample_receipt_df():
    """Minimal fact_receipt DataFrame."""
    return pl.DataFrame({
        "source_system": ["WMS"],
        "receipt_id": ["RCP-001"],
        "receipt_date": ["2024-01-22"],
        "warehouse_id": ["WH-01"],
        "item_id": ["SKU-001"],
        "qty_received": ["100"],
        "po_id": ["PO-001"],
    })
