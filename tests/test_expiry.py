"""Tests for expiry management: sellable/blocked/expired + CRITICAL issue."""
import polars as pl
import pytest
from datetime import date

from src.expiry import compute_sellable_qty, detect_expired_issues, compute_fefo_rank


class TestSellableQty:
    """Sellable qty computation based on expiry, QC, hold status."""

    def test_expired_stock_classified(self, config):
        """Items past expiry date should have expired_qty > 0."""
        df = pl.DataFrame({
            "snapshot_date": [date(2024, 6, 1)],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": [100.0],
            "final_expiry_date": [date(2024, 5, 1)],  # Already expired
            "hold_flag": [False],
            "qc_status": ["released"],
            "qc_required_flag": [False],
            "expiry_control_flag": [True],
            "effective_min_sellable_days": [30],
        })

        result = compute_sellable_qty(df, config)
        assert result["expired_qty"][0] == 100.0, "All stock should be expired"
        assert result["sellable_qty"][0] == 0.0, "No stock should be sellable"

    def test_sellable_with_sufficient_shelf_life(self, config):
        """Items with enough shelf life remaining should be sellable."""
        df = pl.DataFrame({
            "snapshot_date": [date(2024, 1, 1)],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": [100.0],
            "final_expiry_date": [date(2024, 12, 31)],  # Far future
            "hold_flag": [False],
            "qc_status": ["released"],
            "qc_required_flag": [False],
            "expiry_control_flag": [True],
            "effective_min_sellable_days": [30],
        })

        result = compute_sellable_qty(df, config)
        assert result["expired_qty"][0] == 0.0
        assert result["sellable_qty"][0] == 100.0

    def test_held_stock_blocked(self, config):
        """Held stock should be blocked, not sellable."""
        df = pl.DataFrame({
            "snapshot_date": [date(2024, 1, 1)],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": [50.0],
            "final_expiry_date": [date(2024, 12, 31)],
            "hold_flag": [True],  # On hold
            "qc_status": ["released"],
            "qc_required_flag": [False],
            "expiry_control_flag": [True],
            "effective_min_sellable_days": [30],
        })

        result = compute_sellable_qty(df, config)
        assert result["blocked_qty"][0] == 50.0
        assert result["sellable_qty"][0] == 0.0


class TestExpiredIssues:
    """Expired stock must generate CRITICAL issues."""

    def test_expired_creates_critical_issue(self, config):
        """Expired qty > 0 should produce a CRITICAL issue dict."""
        df = pl.DataFrame({
            "snapshot_date": [date(2024, 6, 1)],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": [100.0],
            "expired_qty": [100.0],
            "final_expiry_date": [date(2024, 5, 1)],
        })

        issues = detect_expired_issues(df)
        assert len(issues) == 1
        assert issues[0]["severity"] == "CRITICAL"
        assert issues[0]["issue_type"] == "EXPIRED_STOCK"

    def test_no_expired_no_issues(self, config):
        """No expired stock should produce no issues."""
        df = pl.DataFrame({
            "snapshot_date": [date(2024, 1, 1)],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": [100.0],
            "expired_qty": [0.0],
            "final_expiry_date": [date(2024, 12, 31)],
        })

        issues = detect_expired_issues(df)
        assert len(issues) == 0


class TestFEFO:
    """FEFO ranking should order by expiry date."""

    def test_fefo_ranking(self):
        """Earlier expiry should have lower (better) rank."""
        df = pl.DataFrame({
            "warehouse_id": ["WH-01", "WH-01", "WH-01"],
            "item_id": ["SKU-001", "SKU-001", "SKU-001"],
            "lot_id": ["LOT-C", "LOT-A", "LOT-B"],
            "final_expiry_date": [date(2024, 9, 1), date(2024, 3, 1), date(2024, 6, 1)],
            "onhand_qty": [10.0, 20.0, 15.0],
        })

        result = compute_fefo_rank(df)
        assert "fefo_rank" in result.columns
        # LOT-A (March) should have rank 1, LOT-B (June) rank 2, LOT-C (Sept) rank 3
        sorted_result = result.sort("fefo_rank")
        lots_in_order = sorted_result["lot_id"].to_list()
        assert lots_in_order == ["LOT-A", "LOT-B", "LOT-C"]
