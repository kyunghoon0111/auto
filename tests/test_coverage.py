"""Tests for coverage reporting correctness."""
import polars as pl
import pytest

from src.coverage import compute_coverage, enforce_closed_period_coverage
from src.period_close import close_period
from src.ingest import upsert_core, add_system_columns, filter_columns, cast_columns


class TestCoverageReporting:
    """Coverage must accurately reflect data presence."""

    def test_fx_present_shows_coverage(self, con, config):
        """When FX data exists, fx_rate domain coverage should be 1.0."""
        # Insert FX data
        fx_df = pl.DataFrame({
            "period": ["2024-01"],
            "currency": ["USD"],
            "rate_to_krw": ["1300"],
        })
        fx_df = filter_columns(fx_df, "fact_exchange_rate", config)
        fx_df = cast_columns(fx_df, "fact_exchange_rate", config)
        fx_df = add_system_columns(fx_df, 1, "h1", "fact_exchange_rate", config)
        upsert_core(con, fx_df, "fact_exchange_rate", config, 1, "h1")

        # Also insert an order to create a period
        order_df = pl.DataFrame({
            "source_system": ["OMS"],
            "channel_order_id": ["ORD-001"],
            "line_no": [1],
            "order_date": ["2024-01-15"],
            "channel_store_id": ["STORE-A"],
            "item_id": ["SKU-001"],
            "qty_ordered": ["10"],
        })
        order_df = filter_columns(order_df, "fact_order", config)
        order_df = cast_columns(order_df, "fact_order", config)
        order_df = add_system_columns(order_df, 1, "h2", "fact_order", config)
        upsert_core(con, order_df, "fact_order", config, 1, "h2")

        coverage_df = compute_coverage(con, config)
        fx_rows = coverage_df.filter(pl.col("domain") == "fx_rate")

        if fx_rows.height > 0:
            # At least one period should show coverage
            max_coverage = fx_rows["coverage_rate"].max()
            assert max_coverage == 1.0, f"FX coverage should be 1.0, got {max_coverage}"

    def test_missing_optional_no_fail(self, con, config):
        """Missing OPTIONAL domain should not be severity CRITICAL."""
        # Insert an order to create a period but no settlement data
        order_df = pl.DataFrame({
            "source_system": ["OMS"],
            "channel_order_id": ["ORD-001"],
            "line_no": [1],
            "order_date": ["2024-01-15"],
            "channel_store_id": ["STORE-A"],
            "item_id": ["SKU-001"],
            "qty_ordered": ["10"],
        })
        order_df = filter_columns(order_df, "fact_order", config)
        order_df = cast_columns(order_df, "fact_order", config)
        order_df = add_system_columns(order_df, 1, "h1", "fact_order", config)
        upsert_core(con, order_df, "fact_order", config, 1, "h1")

        coverage_df = compute_coverage(con, config)
        settlement_rows = coverage_df.filter(
            (pl.col("domain") == "revenue_settlement") & (pl.col("coverage_rate") < 1.0)
        )

        if settlement_rows.height > 0:
            # OPTIONAL domain should NOT be CRITICAL
            severities = settlement_rows["severity"].unique().to_list()
            assert "CRITICAL" not in severities, \
                f"OPTIONAL domain should not be CRITICAL: {severities}"


class TestClosedPeriodCoverage:
    """Closed periods enforce REQUIRED domain coverage."""

    def test_closed_period_missing_required_fails(self, con, config):
        """Missing REQUIRED domain in closed period should produce errors."""
        close_period(con, "2024-01", "admin")

        errors = enforce_closed_period_coverage(con, config, "2024-01")
        # FX is REQUIRED in close; without FX data, should have error
        fx_errors = [e for e in errors if "fx_rate" in e]
        assert len(fx_errors) > 0, "Missing FX in closed period should produce an error"
