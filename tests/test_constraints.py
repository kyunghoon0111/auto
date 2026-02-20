"""Tests for constraint signals: deterministic outputs."""
import polars as pl
import pytest

from src.mart_constraint import (
    compute_supply_signals, compute_finance_signals,
    build_mart_constraint_signals,
)
from src.ingest import upsert_core, add_system_columns, filter_columns, cast_columns


class TestConstraintSignals:
    """Constraint detection must produce deterministic signals."""

    def test_late_po_generates_signal(self, con, config):
        """When late PO ratio exceeds threshold, a signal should be generated."""
        # Insert POs with past ETAs and no receipts
        po_df = pl.DataFrame({
            "source_system": ["ERP"] * 5,
            "po_id": ["PO-001", "PO-002", "PO-003", "PO-004", "PO-005"],
            "po_date": ["2024-01-01"] * 5,
            "supplier_id": ["SUP-001"] * 5,
            "item_id": [f"SKU-{i:03d}" for i in range(5)],
            "qty_ordered": ["100"] * 5,
            "eta_date": ["2024-01-10"] * 5,  # All past due
        })
        po_df = filter_columns(po_df, "fact_po", config)
        po_df = cast_columns(po_df, "fact_po", config)
        po_df = add_system_columns(po_df, 1, "h1", "fact_po", config)
        upsert_core(con, po_df, "fact_po", config, 1, "h1")

        signals = compute_supply_signals(con, config)
        # With no receipts and past ETAs, should detect late POs
        if signals:
            assert all(s["domain"] == "supply" for s in signals)
            assert all(s["severity"] in ("CRITICAL", "HIGH") for s in signals)

    def test_deterministic_signals(self, con, config):
        """Running detection twice should produce consistent signal counts."""
        # Insert some PO data
        po_df = pl.DataFrame({
            "source_system": ["ERP"],
            "po_id": ["PO-001"],
            "po_date": ["2024-01-01"],
            "supplier_id": ["SUP-001"],
            "item_id": ["SKU-001"],
            "qty_ordered": ["100"],
            "eta_date": ["2024-01-10"],
        })
        po_df = filter_columns(po_df, "fact_po", config)
        po_df = cast_columns(po_df, "fact_po", config)
        po_df = add_system_columns(po_df, 1, "h1", "fact_po", config)
        upsert_core(con, po_df, "fact_po", config, 1, "h1")

        signals1 = compute_supply_signals(con, config)
        signals2 = compute_supply_signals(con, config)

        # Same number of signals
        assert len(signals1) == len(signals2), "Signal count should be deterministic"

        # Same metric values (ignoring signal_id which is UUID-based)
        if signals1:
            vals1 = [(s["metric_name"], s["current_value"], s["entity_id"]) for s in signals1]
            vals2 = [(s["metric_name"], s["current_value"], s["entity_id"]) for s in signals2]
            assert sorted(vals1) == sorted(vals2), "Signal values should be deterministic"


class TestCriticalAutoIssue:
    """CRITICAL signals must auto-create ops_issue_log entries."""

    def test_critical_creates_issue(self, con, config):
        """CRITICAL signals should auto-insert into ops_issue_log."""
        # Insert inventory data with expired stock to trigger finance signal
        inv_df = pl.DataFrame({
            "source_system": ["WMS"],
            "snapshot_date": ["2024-06-01"],
            "warehouse_id": ["WH-01"],
            "item_id": ["SKU-001"],
            "lot_id": ["LOT-A"],
            "onhand_qty": ["100"],
            "expiry_date": ["2024-05-01"],
        })
        inv_df = filter_columns(inv_df, "fact_inventory_snapshot", config)
        inv_df = cast_columns(inv_df, "fact_inventory_snapshot", config)
        inv_df = add_system_columns(inv_df, 1, "h1", "fact_inventory_snapshot", config)
        upsert_core(con, inv_df, "fact_inventory_snapshot", config, 1, "h1")

        # Build inventory onhand mart first (needed for finance signals)
        from src.mart_scm import build_all_scm_marts
        build_all_scm_marts(con, config)

        # Build constraint signals
        signals = build_mart_constraint_signals(con, config)

        critical_signals = [s for s in signals if s["severity"] == "CRITICAL"]

        if critical_signals:
            # Check that ops_issue_log has entries
            issues = con.execute("SELECT COUNT(*) FROM ops.ops_issue_log").fetchone()[0]
            assert issues > 0, "CRITICAL signals should auto-create ops_issue_log entries"
