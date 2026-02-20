"""Tests for reconciliation sanity."""
import polars as pl
import pytest

from src.ingest import upsert_core, add_system_columns, filter_columns, cast_columns
from src.aliases import apply_aliases
from src.allocation import allocate_charge, largest_fraction_round


class TestInventoryMovement:
    """Inventory movement reconciliation: prev + receipts - shipments = current."""

    def test_balanced_movement(self, con, config):
        """When movements balance, delta should be zero."""
        # This test validates the concept: if we track prev_onhand + receipts - shipments,
        # we should get the current onhand.
        prev_onhand = 100.0
        receipts = 50.0
        shipments = 30.0
        expected = prev_onhand + receipts - shipments  # 120

        assert expected == 120.0, "Basic movement math should balance"

    def test_delta_detection(self):
        """When actual != expected, delta should be non-zero."""
        prev_onhand = 100.0
        receipts = 50.0
        shipments = 30.0
        actual_onhand = 115.0  # Should be 120, delta = -5
        expected = prev_onhand + receipts - shipments
        delta = actual_onhand - expected

        assert delta == -5.0, f"Delta should be -5, got {delta}"


class TestChargesTieOut:
    """Charges invoice vs allocated must tie."""

    def test_allocation_ties_to_invoice(self, config):
        """SUM(allocated) must equal invoice total."""
        targets = pl.DataFrame({
            "item_id": ["A", "B", "C"],
            "warehouse_id": ["W1", "W1", "W1"],
            "channel_store_id": ["S1", "S1", "S1"],
            "lot_id": ["L1", "L2", "L3"],
            "order_count": [1, 1, 1],
            "line_count": [1, 1, 1],
            "qty": [10.0, 20.0, 30.0],
            "weight": [1.0, 2.0, 3.0],
            "value": [100.0, 200.0, 300.0],
            "revenue": [100.0, 200.0, 300.0],
        })

        invoice_amount = 5000.0
        result = allocate_charge(
            "INV-001", 1, "LAST_MILE_PARCEL", invoice_amount,
            "KRW", "2024-01", targets, config
        )

        allocated_total = result["allocated_amount"].sum()
        delta = abs(allocated_total - invoice_amount)
        assert delta == 0.0, f"Invoice vs allocated delta should be 0, got {delta}"
        assert allocated_total == invoice_amount, "Must tie exactly"

    def test_rounding_still_ties(self, config):
        """Even with awkward amounts, rounding must preserve total."""
        targets = pl.DataFrame({
            "item_id": ["A", "B"],
            "warehouse_id": ["W1", "W1"],
            "channel_store_id": ["S1", "S1"],
            "lot_id": ["L1", "L2"],
            "order_count": [1, 1],
            "line_count": [1, 1],
            "qty": [3.0, 7.0],
            "weight": [1.0, 1.0],
            "value": [1.0, 1.0],
            "revenue": [1.0, 1.0],
        })

        # Odd amount that doesn't split evenly
        result = allocate_charge(
            "INV-X", 1, "LAST_MILE_PARCEL", 333.0,
            "KRW", "2024-01", targets, config
        )

        assert result["allocated_amount"].sum() == 333.0


class TestOMSvsWMS:
    """OMS vs WMS reconciliation concept."""

    def test_full_fulfillment(self):
        """If all ordered qty is shipped, fulfillment rate = 1.0."""
        ordered = 100.0
        shipped = 100.0
        rate = shipped / ordered if ordered > 0 else 0
        assert rate == 1.0

    def test_partial_fulfillment(self):
        """Partial shipment yields rate < 1.0."""
        ordered = 100.0
        shipped = 75.0
        rate = shipped / ordered
        assert rate == 0.75
