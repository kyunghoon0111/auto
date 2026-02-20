"""Period close logic with lock and adjustment log.

Closed periods cannot be silently overwritten.
Post-close changes must create ops_adjustment_log entries.
"""
from datetime import datetime, timezone

import duckdb
import polars as pl


def close_period(con: duckdb.DuckDBPyConnection, period: str, closed_by: str) -> None:
    """Close a period. Sets lock_flag=True."""
    existing = con.execute(
        "SELECT COUNT(*) FROM ops.ops_period_close WHERE period = ?", [period]
    ).fetchone()[0]

    now = datetime.now(timezone.utc)

    if existing > 0:
        con.execute(
            "UPDATE ops.ops_period_close SET lock_flag = true, closed_at = ?, closed_by = ? WHERE period = ?",
            [now, closed_by, period]
        )
    else:
        con.execute(
            "INSERT INTO ops.ops_period_close (period, closed_at, closed_by, lock_flag) VALUES (?, ?, ?, true)",
            [period, now, closed_by]
        )


def is_period_closed(con: duckdb.DuckDBPyConnection, period: str) -> bool:
    """Check if a period is closed (lock_flag=True)."""
    result = con.execute(
        "SELECT lock_flag FROM ops.ops_period_close WHERE period = ?", [period]
    ).fetchone()
    if result is None:
        return False
    return bool(result[0])


def reopen_period(con: duckdb.DuckDBPyConnection, period: str, reason: str) -> None:
    """Reopen a closed period. Only via explicit --unlock."""
    con.execute(
        "UPDATE ops.ops_period_close SET lock_flag = false, notes = ? WHERE period = ?",
        [f"Reopened: {reason}", period]
    )


def post_close_adjustment(
    con: duckdb.DuckDBPyConnection,
    period: str,
    table_name: str,
    business_key: str,
    field_name: str,
    old_value: str | None,
    new_value: str | None,
    reason: str,
    adjusted_by: str,
    batch_id: int | None = None,
) -> int:
    """Create an adjustment log entry for post-close changes.

    Returns the adjustment_id.
    """
    # Get next adjustment_id
    result = con.execute("SELECT COALESCE(MAX(adjustment_id), 0) + 1 FROM ops.ops_adjustment_log").fetchone()
    adj_id = result[0]

    con.execute(
        "INSERT INTO ops.ops_adjustment_log "
        "(adjustment_id, period, table_name, business_key, field_name, old_value, new_value, reason, adjusted_by, batch_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [adj_id, period, table_name, business_key, field_name, old_value, new_value, reason, adjusted_by, batch_id]
    )

    return adj_id


def get_adjustments(con: duckdb.DuckDBPyConnection, period: str) -> pl.DataFrame:
    """Get all adjustments for a period."""
    return con.execute(
        "SELECT * FROM ops.ops_adjustment_log WHERE period = ? ORDER BY adjustment_id", [period]
    ).pl()


def get_closed_periods(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Get all currently closed periods."""
    result = con.execute(
        "SELECT period FROM ops.ops_period_close WHERE lock_flag = true ORDER BY period"
    ).fetchall()
    return [r[0] for r in result]


def check_period_for_ingestion(con: duckdb.DuckDBPyConnection, period: str) -> None:
    """Raise if trying to ingest data into a closed period."""
    if is_period_closed(con, period):
        raise ValueError(
            f"Period '{period}' is closed. Cannot ingest data directly. "
            f"Use post_close_adjustment() or reopen the period with --unlock."
        )
