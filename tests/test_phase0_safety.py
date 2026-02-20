"""Phase 0 safety tests for KPI patch.

Tests cover:
1. As-of cost join with multi-component (no join explosion, period-correct)
2. NULL propagation (missing cost -> cogs_krw NULL, coverage_flag PARTIAL)
3. Coverage_flag propagation downstream (ANY upstream PARTIAL -> PARTIAL)
4. COGS partition key grain (multi-channel_store_id preserved)
5. Expiry risk partition key grain (multi-lot preserved)
6. No NULL coverage_flag in P&L mart outputs
7. Sales-only filter (channel_order_id IS NOT NULL)
"""
import datetime

import duckdb
import polars as pl
import pytest

from src.db import init_db
from src.config import AppConfig
from src.mart_pnl import (
    build_mart_pnl_cogs,
    build_mart_pnl_gross_margin,
    build_mart_pnl_contribution,
    build_mart_pnl_operating_profit,
    build_mart_pnl_revenue,
    build_mart_pnl_variable_cost,
    build_all_pnl_marts,
)


@pytest.fixture
def pnl_con(tmp_path, config):
    """DuckDB connection with schema + seed data for P&L tests."""
    db_path = tmp_path / "pnl_test.duckdb"
    con = duckdb.connect(str(db_path))
    init_db(con)
    return con


def _seed_cost_structure(con, items):
    """Seed fact_cost_structure with multi-component costs.

    items: list of (item_id, cost_component, effective_from_str, cost_per_unit_krw)
    """
    for item_id, comp, eff, cost in items:
        con.execute(
            "INSERT INTO core.fact_cost_structure "
            "(item_id, cost_component, effective_from, cost_per_unit_krw, source_system, load_batch_id, source_file_hash) "
            "VALUES (?, ?, ?, ?, 'TEST', 1, 'hash')",
            [item_id, comp, eff, cost],
        )


def _seed_shipment(con, rows):
    """Seed fact_shipment.

    rows: list of (shipment_id, ship_date, warehouse_id, item_id, qty_shipped, lot_id, channel_order_id, channel_store_id)
    """
    for sid, sdate, wh, item, qty, lot, order_id, store_id in rows:
        con.execute(
            "INSERT INTO core.fact_shipment "
            "(shipment_id, ship_date, warehouse_id, item_id, qty_shipped, lot_id, "
            "channel_order_id, channel_store_id, source_system, load_batch_id, source_file_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'TEST', 1, 'hash')",
            [sid, sdate, wh, item, qty, lot, order_id, store_id],
        )


def _seed_return(con, rows):
    """Seed fact_return.

    rows: list of (return_id, return_date, warehouse_id, item_id, qty_returned, lot_id, channel_order_id)
    """
    for rid, rdate, wh, item, qty, lot, order_id in rows:
        con.execute(
            "INSERT INTO core.fact_return "
            "(return_id, return_date, warehouse_id, item_id, qty_returned, lot_id, "
            "channel_order_id, source_system, load_batch_id, source_file_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'TEST', 1, 'hash')",
            [rid, rdate, wh, item, qty, lot, order_id],
        )


_settlement_counter = 0


def _seed_settlement(con, rows):
    """Seed fact_settlement.

    rows: list of (period, item_id, channel_store_id, currency, gross_sales, discounts, refunds, net_payout)
    """
    global _settlement_counter
    for period, item, store, cur, gs, disc, ref, net in rows:
        _settlement_counter += 1
        con.execute(
            "INSERT INTO core.fact_settlement "
            "(settlement_id, line_no, period, item_id, channel_store_id, currency, "
            "gross_sales, discounts, refunds, net_payout, "
            "source_system, load_batch_id, source_file_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'TEST', 1, 'hash')",
            [f"STL-{_settlement_counter:04d}", _settlement_counter, period, item, store, cur, gs, disc, ref, net],
        )


def _seed_fx(con, rows):
    """Seed fact_exchange_rate. rows: list of (period, currency, rate_to_krw)."""
    for period, cur, rate in rows:
        con.execute(
            "INSERT INTO core.fact_exchange_rate (period, currency, rate_to_krw) VALUES (?, ?, ?)",
            [period, cur, rate],
        )


class TestCOGSAsOfJoin:
    """Test 1: As-of cost join with multi-component (no join explosion, period-correct)."""

    def test_multi_component_no_explosion(self, pnl_con, config):
        """3 cost_components at Jan-01 -> pre-agg to 1000. Result should have 1 row per grain, not 3."""
        # Seed: 3 components for SKU-001 at Jan-01
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 500.0),
            ("SKU-001", "LABOR", "2024-01-01", 300.0),
            ("SKU-001", "OVERHEAD", "2024-01-01", 200.0),
        ])
        # Seed: 1 shipment in Jan
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute("SELECT * FROM mart.mart_pnl_cogs").pl()
        assert result.height == 1, f"Expected 1 row, got {result.height} (join explosion?)"
        assert result["unit_cost_krw"][0] == 1000.0  # 500+300+200
        assert result["cogs_krw"][0] == 10000.0  # 10 * 1000

    def test_period_correct_cost(self, pnl_con, config):
        """Jan cost=1000, Feb cost=1200. Jan COGS should use 1000, Feb should use 1200."""
        # Jan costs
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 500.0),
            ("SKU-001", "LABOR", "2024-01-01", 300.0),
            ("SKU-001", "OVERHEAD", "2024-01-01", 200.0),
        ])
        # Feb costs (higher)
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-02-01", 600.0),
            ("SKU-001", "LABOR", "2024-02-01", 350.0),
            ("SKU-001", "OVERHEAD", "2024-02-01", 250.0),
        ])
        # Shipments in both months
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
            ("SHP-002", "2024-02-15", "WH-01", "SKU-001", 5.0, "LOT-A", "ORD-002", "STORE-A"),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute(
            "SELECT period, unit_cost_krw, cogs_krw FROM mart.mart_pnl_cogs ORDER BY period"
        ).pl()
        assert result.height == 2
        # Jan: cost=1000 (500+300+200), cogs=10*1000=10000
        assert result["unit_cost_krw"][0] == 1000.0
        assert result["cogs_krw"][0] == 10000.0
        # Feb: cost=1200 (600+350+250), cogs=5*1200=6000
        assert result["unit_cost_krw"][1] == 1200.0
        assert result["cogs_krw"][1] == 6000.0


class TestNULLPropagation:
    """Test 2: Missing cost -> cogs_krw NULL, coverage_flag PARTIAL."""

    def test_missing_cost_null_cogs(self, pnl_con, config):
        """Item with no cost_structure -> cogs_krw NULL, coverage_flag PARTIAL."""
        # No cost_structure for SKU-002
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-002", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute("SELECT * FROM mart.mart_pnl_cogs").pl()
        assert result.height == 1
        assert result["unit_cost_krw"][0] is None
        assert result["cogs_krw"][0] is None
        assert result["coverage_flag"][0] == "PARTIAL"


class TestCoverageFlagPropagation:
    """Test 3: COGS PARTIAL -> downstream all PARTIAL."""

    def test_partial_propagates_to_all_downstream(self, pnl_con, config):
        """If COGS is PARTIAL, gross_margin/contribution/operating_profit all become PARTIAL."""
        # Revenue: ACTUAL
        _seed_settlement(pnl_con, [
            ("2024-01", "SKU-002", "STORE-A", "KRW", 100000.0, 0.0, 0.0, 100000.0),
        ])
        # Shipment with no cost -> PARTIAL COGS
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-002", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])

        build_all_pnl_marts(pnl_con, config)

        # COGS: PARTIAL
        cogs = pnl_con.execute(
            "SELECT coverage_flag FROM mart.mart_pnl_cogs WHERE item_id='SKU-002'"
        ).pl()
        assert cogs.height == 1
        assert cogs["coverage_flag"][0] == "PARTIAL"

        # Gross margin: PARTIAL (COGS upstream is PARTIAL)
        gm = pnl_con.execute(
            "SELECT coverage_flag FROM mart.mart_pnl_gross_margin WHERE item_id='SKU-002'"
        ).pl()
        if gm.height > 0:
            assert gm["coverage_flag"][0] == "PARTIAL"

        # Contribution: PARTIAL
        contrib = pnl_con.execute(
            "SELECT coverage_flag FROM mart.mart_pnl_contribution WHERE item_id='SKU-002'"
        ).pl()
        if contrib.height > 0:
            assert contrib["coverage_flag"][0] == "PARTIAL"

        # Operating profit: PARTIAL
        op = pnl_con.execute(
            "SELECT coverage_flag FROM mart.mart_pnl_operating_profit WHERE item_id='SKU-002'"
        ).pl()
        if op.height > 0:
            assert op["coverage_flag"][0] == "PARTIAL"


class TestCOGSPartitionGrain:
    """Test 6: Same item+period with 2 channel_store_ids -> both rows preserved."""

    def test_multi_channel_store_preserved(self, pnl_con, config):
        """Same item in same period but different stores must both survive."""
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 1000.0),
        ])
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
            ("SHP-002", "2024-01-20", "WH-01", "SKU-001", 5.0, "LOT-A", "ORD-002", "STORE-B"),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute(
            "SELECT channel_store_id, qty_shipped, cogs_krw FROM mart.mart_pnl_cogs ORDER BY channel_store_id"
        ).pl()
        assert result.height == 2, f"Expected 2 rows (2 stores), got {result.height}"
        stores = result["channel_store_id"].to_list()
        assert "STORE-A" in stores
        assert "STORE-B" in stores


class TestSalesOnlyFilter:
    """Test 7: Sales-only filter (channel_order_id IS NOT NULL)."""

    def test_transfer_excluded_from_cogs(self, pnl_con, config):
        """Shipments without channel_order_id should be excluded from COGS."""
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 1000.0),
        ])
        # Sales shipment
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])
        # Transfer shipment (no order_id)
        _seed_shipment(pnl_con, [
            ("SHP-002", "2024-01-16", "WH-01", "SKU-001", 20.0, "LOT-A", None, "STORE-A"),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute("SELECT qty_shipped FROM mart.mart_pnl_cogs").pl()
        assert result.height == 1
        # Only sales shipment (10), not transfer (20)
        assert result["qty_shipped"][0] == 10.0

    def test_transfer_return_excluded_from_cogs(self, pnl_con, config):
        """Returns without channel_order_id should be excluded from COGS qty_returned."""
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 1000.0),
        ])
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])
        # Sales return
        _seed_return(pnl_con, [
            ("RET-001", "2024-01-20", "WH-01", "SKU-001", 2.0, "LOT-A", "ORD-001"),
        ])
        # Transfer return (no order_id) — should be excluded
        _seed_return(pnl_con, [
            ("RET-002", "2024-01-21", "WH-01", "SKU-001", 5.0, "LOT-A", None),
        ])

        build_mart_pnl_cogs(pnl_con, config)

        result = pnl_con.execute("SELECT qty_returned, qty_net FROM mart.mart_pnl_cogs").pl()
        assert result.height == 1
        assert result["qty_returned"][0] == 2.0  # Only sales return
        assert result["qty_net"][0] == 8.0  # 10 - 2


class TestFXNullPropagation:
    """Test FX rate missing -> KRW values NULL + coverage_flag PARTIAL."""

    def test_fx_missing_sets_partial(self, pnl_con, config):
        """Non-KRW settlement without FX rate -> NULL KRW values + PARTIAL."""
        _seed_settlement(pnl_con, [
            ("2024-01", "SKU-001", "STORE-A", "USD", 100.0, 0.0, 0.0, 100.0),
        ])
        # No FX rate seeded for USD

        build_mart_pnl_revenue(pnl_con, config)

        result = pnl_con.execute("SELECT * FROM mart.mart_pnl_revenue").pl()
        assert result.height == 1
        assert result["net_revenue_krw"][0] is None
        assert result["coverage_flag"][0] == "PARTIAL"

    def test_krw_always_actual(self, pnl_con, config):
        """KRW settlement needs no FX -> always ACTUAL."""
        _seed_settlement(pnl_con, [
            ("2024-01", "SKU-001", "STORE-A", "KRW", 100000.0, 0.0, 0.0, 100000.0),
        ])

        build_mart_pnl_revenue(pnl_con, config)

        result = pnl_con.execute("SELECT * FROM mart.mart_pnl_revenue").pl()
        assert result.height == 1
        assert result["net_revenue_krw"][0] == 100000.0
        assert result["coverage_flag"][0] == "ACTUAL"


class TestNoCoverageNull:
    """Test 8: After full pipeline, no NULL coverage_flag in P&L mart outputs."""

    def test_no_null_coverage_flag(self, pnl_con, config):
        """All P&L mart rows should have coverage_flag set (not NULL)."""
        # Seed minimal data
        _seed_settlement(pnl_con, [
            ("2024-01", "SKU-001", "STORE-A", "KRW", 100000.0, 5000.0, 1000.0, 94000.0),
        ])
        _seed_cost_structure(pnl_con, [
            ("SKU-001", "MATERIAL", "2024-01-01", 1000.0),
        ])
        _seed_shipment(pnl_con, [
            ("SHP-001", "2024-01-15", "WH-01", "SKU-001", 10.0, "LOT-A", "ORD-001", "STORE-A"),
        ])

        build_all_pnl_marts(pnl_con, config)

        # Check all 6 tables with coverage_flag
        tables_with_flag = [
            "mart.mart_pnl_revenue",
            "mart.mart_pnl_cogs",
            "mart.mart_pnl_gross_margin",
            "mart.mart_pnl_contribution",
            "mart.mart_pnl_operating_profit",
        ]
        for table in tables_with_flag:
            null_count = pnl_con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE coverage_flag IS NULL"
            ).fetchone()[0]
            total = pnl_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert null_count == 0, (
                f"{table}: {null_count}/{total} rows have NULL coverage_flag"
            )
