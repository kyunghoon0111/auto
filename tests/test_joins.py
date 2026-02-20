"""Tests for effective_from versioned joins (strict 1:1)."""
import polars as pl
import pytest

from src.ingest import upsert_core, add_system_columns, filter_columns, cast_columns
from src.aliases import apply_aliases


class TestVersionedJoins:
    """Versioned join must be strictly 1:1 or FAIL."""

    def test_single_effective_from_match(self, con, config):
        """A transaction date should match exactly one cost structure row."""
        # Insert two cost rows with different effective_from
        cost_df = pl.DataFrame({
            "item_id": ["SKU-001", "SKU-001"],
            "cost_component": ["MATERIAL", "MATERIAL"],
            "effective_from": ["2024-01-01", "2024-02-01"],
            "cost_per_unit_krw": ["5000", "5500"],
        })
        cost_df = filter_columns(cost_df, "fact_cost_structure", config)
        cost_df = cast_columns(cost_df, "fact_cost_structure", config)
        cost_df = add_system_columns(cost_df, 1, "h1", "fact_cost_structure", config)
        upsert_core(con, cost_df, "fact_cost_structure", config, 1, "h1")

        # Query with ROW_NUMBER pattern for a date in January -> should get 5000
        result = con.execute("""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY item_id
                    ORDER BY effective_from DESC
                ) AS rn
                FROM core.fact_cost_structure
                WHERE effective_from <= DATE '2024-01-15'
            ) WHERE rn = 1
        """).fetchall()

        assert len(result) == 1, "Should match exactly one row"
        # cost_per_unit_krw should be 5000 (Jan row)
        cost_col_idx = [i for i, desc in enumerate(con.description) if desc[0] == "cost_per_unit_krw"][0]
        assert result[0][cost_col_idx] == 5000.0

    def test_february_date_matches_latest(self, con, config):
        """A February date should pick the Feb effective_from row."""
        cost_df = pl.DataFrame({
            "item_id": ["SKU-001", "SKU-001"],
            "cost_component": ["MATERIAL", "MATERIAL"],
            "effective_from": ["2024-01-01", "2024-02-01"],
            "cost_per_unit_krw": ["5000", "5500"],
        })
        cost_df = filter_columns(cost_df, "fact_cost_structure", config)
        cost_df = cast_columns(cost_df, "fact_cost_structure", config)
        cost_df = add_system_columns(cost_df, 1, "h1", "fact_cost_structure", config)
        upsert_core(con, cost_df, "fact_cost_structure", config, 1, "h1")

        result = con.execute("""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY item_id
                    ORDER BY effective_from DESC
                ) AS rn
                FROM core.fact_cost_structure
                WHERE effective_from <= DATE '2024-02-15'
            ) WHERE rn = 1
        """).fetchall()

        assert len(result) == 1
        cost_col_idx = [i for i, desc in enumerate(con.description) if desc[0] == "cost_per_unit_krw"][0]
        assert result[0][cost_col_idx] == 5500.0

    def test_no_match_before_earliest(self, con, config):
        """A date before any effective_from should return no rows."""
        cost_df = pl.DataFrame({
            "item_id": ["SKU-001"],
            "cost_component": ["MATERIAL"],
            "effective_from": ["2024-06-01"],
            "cost_per_unit_krw": ["5000"],
        })
        cost_df = filter_columns(cost_df, "fact_cost_structure", config)
        cost_df = cast_columns(cost_df, "fact_cost_structure", config)
        cost_df = add_system_columns(cost_df, 1, "h1", "fact_cost_structure", config)
        upsert_core(con, cost_df, "fact_cost_structure", config, 1, "h1")

        result = con.execute("""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY item_id
                    ORDER BY effective_from DESC
                ) AS rn
                FROM core.fact_cost_structure
                WHERE effective_from <= DATE '2024-01-01'
            ) WHERE rn = 1
        """).fetchall()

        assert len(result) == 0, "No row should match before earliest effective_from"
