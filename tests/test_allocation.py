"""Tests for cost allocation: conservation + determinism."""
import polars as pl
import pytest

from src.allocation import allocate_charge, largest_fraction_round
from src.config import AppConfig


class TestConservation:
    """SUM(allocated) must equal invoice total."""

    def test_three_way_allocation(self, config):
        """1000 KRW allocated across 3 items must sum to 1000."""
        targets = pl.DataFrame({
            "item_id": ["SKU-001", "SKU-002", "SKU-003"],
            "warehouse_id": ["WH-01", "WH-01", "WH-01"],
            "channel_store_id": ["STORE-A", "STORE-A", "STORE-A"],
            "lot_id": ["L1", "L2", "L3"],
            "qty": [3.0, 5.0, 2.0],
            "order_count": [1, 1, 1],
            "line_count": [1, 1, 1],
            "weight": [1.0, 2.0, 1.0],
            "value": [100.0, 200.0, 50.0],
            "revenue": [100.0, 200.0, 50.0],
        })

        result = allocate_charge(
            invoice_no="INV-001",
            invoice_line_no=1,
            charge_type="LAST_MILE_PARCEL",
            amount=1000.0,
            currency="KRW",
            period="2024-01",
            targets=targets,
            config=config,
        )

        total_allocated = result["allocated_amount"].sum()
        assert total_allocated == 1000.0, f"Conservation violated: {total_allocated} != 1000"

    def test_single_target(self, config):
        """Single target should receive full amount."""
        targets = pl.DataFrame({
            "item_id": ["SKU-001"],
            "warehouse_id": ["WH-01"],
            "channel_store_id": ["STORE-A"],
            "lot_id": ["L1"],
            "order_count": [1],
            "line_count": [1],
            "qty": [10.0],
            "weight": [5.0],
            "value": [100.0],
            "revenue": [100.0],
        })

        result = allocate_charge(
            invoice_no="INV-001", invoice_line_no=1,
            charge_type="LAST_MILE_PARCEL", amount=500.0,
            currency="KRW", period="2024-01",
            targets=targets, config=config,
        )

        assert result["allocated_amount"][0] == 500.0

    def test_large_amount_conservation(self, config):
        """Large amounts should still conserve perfectly."""
        targets = pl.DataFrame({
            "item_id": [f"SKU-{i:03d}" for i in range(7)],
            "warehouse_id": ["WH-01"] * 7,
            "channel_store_id": ["STORE-A"] * 7,
            "lot_id": [f"L{i}" for i in range(7)],
            "order_count": [1] * 7,
            "line_count": [1] * 7,
            "qty": [3.0, 7.0, 11.0, 2.0, 5.0, 1.0, 9.0],
            "weight": [1.0] * 7,
            "value": [100.0] * 7,
            "revenue": [100.0] * 7,
        })

        result = allocate_charge(
            invoice_no="INV-001", invoice_line_no=1,
            charge_type="LAST_MILE_PARCEL", amount=999999.0,
            currency="KRW", period="2024-01",
            targets=targets, config=config,
        )

        assert result["allocated_amount"].sum() == 999999.0


class TestDeterminism:
    """Same inputs must produce same outputs."""

    def test_repeated_allocation_identical(self, config):
        """Running allocation twice with same inputs must give same results."""
        targets = pl.DataFrame({
            "item_id": ["SKU-A", "SKU-B", "SKU-C"],
            "warehouse_id": ["WH-01", "WH-01", "WH-01"],
            "channel_store_id": ["S1", "S1", "S1"],
            "lot_id": ["L1", "L2", "L3"],
            "order_count": [1, 1, 1],
            "line_count": [1, 1, 1],
            "qty": [5.0, 3.0, 2.0],
            "weight": [1.0, 1.0, 1.0],
            "value": [100.0, 60.0, 40.0],
            "revenue": [100.0, 60.0, 40.0],
        })

        result1 = allocate_charge(
            "INV-X", 1, "LAST_MILE_PARCEL", 1000.0, "KRW", "2024-01", targets, config
        )
        result2 = allocate_charge(
            "INV-X", 1, "LAST_MILE_PARCEL", 1000.0, "KRW", "2024-01", targets, config
        )

        assert result1["allocated_amount"].to_list() == result2["allocated_amount"].to_list()


class TestLargestFractionRound:
    """Hare-Niemeyer rounding guarantees."""

    def test_basic_rounding(self):
        """Simple case: remainder distributed to largest fraction."""
        result = largest_fraction_round([333.33, 333.33, 333.34], 1000.0, 0)
        assert sum(result) == 1000.0

    def test_zero_decimals(self):
        """KRW: 0 decimals, must still conserve."""
        result = largest_fraction_round([100.1, 200.3, 199.6], 500.0, 0)
        assert sum(result) == 500.0

    def test_single_item(self):
        """Single item gets full amount."""
        result = largest_fraction_round([1000.0], 1000.0, 0)
        assert result == [1000.0]

    def test_empty_list(self):
        """Empty input returns empty output."""
        assert largest_fraction_round([], 0.0, 0) == []


class TestUnsupportedBasis:
    """Unsupported allocation basis must raise."""

    def test_no_valid_basis_raises(self, config):
        """Targets with no matching basis columns should raise ValueError."""
        targets = pl.DataFrame({
            "item_id": ["SKU-001"],
            "warehouse_id": ["WH-01"],
            "channel_store_id": ["STORE-A"],
            "lot_id": ["L1"],
            # No qty, weight, order_count, etc.
        })

        with pytest.raises(ValueError, match="Cannot resolve allocation basis"):
            allocate_charge(
                "INV-001", 1, "LAST_MILE_PARCEL", 1000.0, "KRW", "2024-01",
                targets, config,
            )
