"""Tests for period close: lock + adjustment behavior."""
import pytest

from src.period_close import (
    close_period, is_period_closed, reopen_period,
    post_close_adjustment, get_adjustments, check_period_for_ingestion,
)


class TestPeriodClose:
    """Period close must lock and prevent direct writes."""

    def test_close_and_lock(self, con):
        """Closing a period should set lock_flag to True."""
        close_period(con, "2024-01", "test_user")
        assert is_period_closed(con, "2024-01") is True

    def test_unclosed_period_is_open(self, con):
        """An unclosed period should return False."""
        assert is_period_closed(con, "2024-02") is False

    def test_closed_period_rejects_ingestion(self, con):
        """Ingesting into a closed period should raise ValueError."""
        close_period(con, "2024-01", "test_user")
        with pytest.raises(ValueError, match="closed"):
            check_period_for_ingestion(con, "2024-01")

    def test_open_period_allows_ingestion(self, con):
        """Ingesting into an open period should not raise."""
        check_period_for_ingestion(con, "2024-02")  # Should not raise


class TestAdjustments:
    """Post-close adjustments must create audit trail."""

    def test_adjustment_creates_log(self, con):
        """Adjustment to closed period should create ops_adjustment_log entry."""
        close_period(con, "2024-01", "admin")

        adj_id = post_close_adjustment(
            con,
            period="2024-01",
            table_name="fact_order",
            business_key="ORD-001|1",
            field_name="qty_ordered",
            old_value="10",
            new_value="12",
            reason="Correction per customer",
            adjusted_by="analyst",
        )

        assert adj_id > 0

        # Verify the log entry
        adjustments = get_adjustments(con, "2024-01")
        assert adjustments.height == 1
        assert adjustments["table_name"][0] == "fact_order"
        assert adjustments["old_value"][0] == "10"
        assert adjustments["new_value"][0] == "12"

    def test_multiple_adjustments(self, con):
        """Multiple adjustments should all be recorded."""
        close_period(con, "2024-01", "admin")

        post_close_adjustment(con, "2024-01", "fact_order", "ORD-001|1",
                              "qty_ordered", "10", "12", "Fix 1", "user1")
        post_close_adjustment(con, "2024-01", "fact_order", "ORD-002|1",
                              "qty_ordered", "5", "8", "Fix 2", "user2")

        adjustments = get_adjustments(con, "2024-01")
        assert adjustments.height == 2


class TestUnlock:
    """Unlocking a period should allow ingestion again."""

    def test_reopen_allows_ingestion(self, con):
        """After reopening, ingestion should be allowed."""
        close_period(con, "2024-01", "admin")
        assert is_period_closed(con, "2024-01") is True

        reopen_period(con, "2024-01", "Emergency correction needed")
        assert is_period_closed(con, "2024-01") is False

        # Should not raise
        check_period_for_ingestion(con, "2024-01")
