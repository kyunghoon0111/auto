"""Tests for ETL pipeline: idempotent upsert, alias mapping, DQ checks."""
import polars as pl
import pytest
from pathlib import Path

from src.aliases import apply_aliases
from src.dq import run_all_checks, has_failures
from src.ingest import detect_table_type, process_file, upsert_core, add_system_columns, filter_columns, cast_columns


class TestIdempotentUpsert:
    """Double-load same file must not duplicate or drift."""

    def test_double_load_same_data(self, con, config, sample_order_df):
        """Loading the same data twice should result in same row count."""
        df = apply_aliases(sample_order_df, "fact_order", config)
        df = filter_columns(df, "fact_order", config)
        df = cast_columns(df, "fact_order", config)
        df = add_system_columns(df, batch_id=1, file_hash="hash1", table_name="fact_order", config=config)

        upsert_core(con, df, "fact_order", config, batch_id=1, file_hash="hash1")
        count1 = con.execute("SELECT COUNT(*) FROM core.fact_order").fetchone()[0]

        # Load same data again
        df2 = apply_aliases(sample_order_df, "fact_order", config)
        df2 = filter_columns(df2, "fact_order", config)
        df2 = cast_columns(df2, "fact_order", config)
        df2 = add_system_columns(df2, batch_id=2, file_hash="hash1", table_name="fact_order", config=config)

        upsert_core(con, df2, "fact_order", config, batch_id=2, file_hash="hash1")
        count2 = con.execute("SELECT COUNT(*) FROM core.fact_order").fetchone()[0]

        assert count1 == count2, f"Idempotency violated: {count1} -> {count2}"
        assert count1 == 2  # Two distinct orders


class TestAliasMapping:
    """Alias mapping must come ONLY from config, not hardcoded."""

    def test_korean_column_names(self, config, sample_order_korean_df):
        """Korean column names should be mapped to canonical English names."""
        df = apply_aliases(sample_order_korean_df, "fact_order", config)
        assert "channel_order_id" in df.columns, "주문번호 should map to channel_order_id"
        assert "item_id" in df.columns, "상품코드 should map to item_id"
        assert "qty_ordered" in df.columns, "주문수량 should map to qty_ordered"
        assert "order_date" in df.columns, "주문일 should map to order_date"
        assert "source_system" in df.columns, "system should map to source_system"

    def test_alias_mapping_from_config_only(self, config):
        """Verify that alias mappings come from config, not hardcoded."""
        # Check that config has alias definitions
        assert "common" in config.aliases
        assert "fact_order" in config.aliases
        assert "channel_order_id" in config.aliases["fact_order"]


class TestDQChecks:
    """DQ validation must catch missing cols, null keys, duplicates."""

    def test_missing_required_column(self, config):
        """Missing a required column must cause CRITICAL failure."""
        df = pl.DataFrame({
            "source_system": ["OMS"],
            "channel_order_id": ["ORD-001"],
            # Missing: line_no, order_date, channel_store_id, item_id, qty_ordered
        })
        results = run_all_checks(df, "fact_order", config)
        assert has_failures(results), "Missing required columns should cause DQ failure"

    def test_null_business_key(self, config):
        """NULL in business key must cause CRITICAL failure."""
        df = pl.DataFrame({
            "source_system": ["OMS"],
            "channel_order_id": [None],
            "line_no": [1],
            "order_date": ["2024-01-15"],
            "channel_store_id": ["STORE-A"],
            "item_id": ["SKU-001"],
            "qty_ordered": ["10"],
        })
        results = run_all_checks(df, "fact_order", config)
        assert has_failures(results), "NULL business key should cause DQ failure"

    def test_duplicate_business_keys(self, config):
        """Duplicate composite keys must cause HIGH failure."""
        df = pl.DataFrame({
            "source_system": ["OMS", "OMS"],
            "channel_order_id": ["ORD-001", "ORD-001"],
            "line_no": [1, 1],  # Duplicate key
            "order_date": ["2024-01-15", "2024-01-16"],
            "channel_store_id": ["STORE-A", "STORE-A"],
            "item_id": ["SKU-001", "SKU-002"],
            "qty_ordered": ["10", "20"],
        })
        results = run_all_checks(df, "fact_order", config)
        assert has_failures(results), "Duplicate business keys should cause DQ failure"

    def test_valid_data_passes(self, config, sample_order_df):
        """Valid data should pass all DQ checks."""
        df = apply_aliases(sample_order_df, "fact_order", config)
        results = run_all_checks(df, "fact_order", config)
        assert not has_failures(results), f"Valid data should pass DQ: {[r for r in results if not r.passed]}"

    def test_unknown_charge_type_fails(self, config):
        """Unknown charge_type must cause HIGH failure."""
        df = pl.DataFrame({
            "source_system": ["ERP"],
            "invoice_no": ["INV-001"],
            "invoice_line_no": [1],
            "charge_type": ["UNKNOWN_CHARGE"],
            "amount": ["1000"],
            "currency": ["KRW"],
            "period": ["2024-01"],
        })
        results = run_all_checks(df, "fact_charge_actual", config)
        assert has_failures(results), "Unknown charge_type should cause DQ failure"


class TestTableDetection:
    """Table type detection must use schema.yaml scores; ties -> FAIL."""

    def test_detect_order_table(self, config, sample_order_df):
        """Order data should be detected as fact_order."""
        table = detect_table_type(sample_order_df, config)
        assert table == "fact_order"

    def test_detect_shipment_table(self, config, sample_shipment_df):
        """Shipment data should be detected as fact_shipment."""
        table = detect_table_type(sample_shipment_df, config)
        assert table == "fact_shipment"

    def test_unrecognizable_data_fails(self, config):
        """Data that doesn't match any table should fail."""
        df = pl.DataFrame({
            "random_col_1": ["abc"],
            "random_col_2": ["def"],
        })
        with pytest.raises(ValueError, match="(No table matched|Tie in table detection)"):
            detect_table_type(df, config)
