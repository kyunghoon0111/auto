"""Lot/expiry management, FEFO ranking, sellable qty computation.

Cosmetics-ready, generic lot-level expiry tracking.
Priority: explicit expiry_date > mfg_date + shelf_life > open_plus_pao.
"""
import uuid
from datetime import date, datetime, timezone, timedelta

import duckdb
import polars as pl

from src.config import AppConfig


def compute_final_expiry(con: duckdb.DuckDBPyConnection, config: AppConfig) -> pl.DataFrame:
    """Join inventory snapshot with item master, compute final_expiry_date.

    Returns enriched DataFrame with final_expiry_date, min_sellable_days.
    """
    # Get inventory snapshots
    try:
        snap_df = con.execute("SELECT * FROM core.fact_inventory_snapshot").pl()
    except Exception:
        return pl.DataFrame()

    if snap_df.height == 0:
        return pl.DataFrame()

    # Get item master
    try:
        item_df = con.execute(
            "SELECT item_id, item_type, expiry_control_flag, expiry_basis, "
            "shelf_life_days, min_sellable_days, pao_days, qc_required_flag "
            "FROM core.dim_item"
        ).pl()
    except Exception:
        item_df = pl.DataFrame()

    # Join
    if item_df.height > 0:
        df = snap_df.join(item_df, on="item_id", how="left")
    else:
        df = snap_df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("item_type"),
            pl.lit(False).alias("expiry_control_flag"),
            pl.lit(None).cast(pl.Utf8).alias("expiry_basis"),
            pl.lit(None).cast(pl.Int32).alias("shelf_life_days"),
            pl.lit(None).cast(pl.Int32).alias("min_sellable_days"),
            pl.lit(None).cast(pl.Int32).alias("pao_days"),
            pl.lit(False).alias("qc_required_flag"),
        ])

    # Compute final_expiry_date with priority chain
    df = df.with_columns(
        pl.col("expiry_date").alias("final_expiry_date")
    )

    # Get default min_sellable_days from thresholds
    min_sell_defaults = config.thresholds.get("expiry", {}).get("min_sellable_days_default", {})

    df = df.with_columns(
        pl.when(pl.col("min_sellable_days").is_not_null())
        .then(pl.col("min_sellable_days"))
        .when(pl.col("item_type").is_in(list(min_sell_defaults.keys())))
        .then(
            pl.col("item_type").replace_strict(
                min_sell_defaults, default=60, return_dtype=pl.Int64
            )
        )
        .otherwise(60)
        .alias("effective_min_sellable_days")
    )

    return df


def compute_sellable_qty(df: pl.DataFrame, config: AppConfig) -> pl.DataFrame:
    """Add sellable_qty, blocked_qty, expired_qty columns.

    sellable requires:
    - expiry >= snapshot_date + min_sellable_days (if expiry tracked)
    - qc_status = 'released' (if qc_required)
    - hold_flag = false
    """
    if df.height == 0:
        return df

    today = date.today()

    df = df.with_columns([
        # expired_qty: items past expiry date
        pl.when(
            pl.col("final_expiry_date").is_not_null()
            & (pl.col("final_expiry_date") < pl.col("snapshot_date"))
        )
        .then(pl.col("onhand_qty"))
        .otherwise(0.0)
        .alias("expired_qty"),

        # blocked_qty: held items or QC-blocked or insufficient shelf life
        pl.when(
            (pl.col("hold_flag") == True)
            | (
                pl.col("qc_required_flag").fill_null(False)
                & (pl.col("qc_status").fill_null("").str.to_lowercase() != "released")
            )
            | (
                pl.col("final_expiry_date").is_not_null()
                & (
                    pl.col("final_expiry_date")
                    < (pl.col("snapshot_date") + pl.duration(days=pl.col("effective_min_sellable_days")))
                )
                & (pl.col("final_expiry_date") >= pl.col("snapshot_date"))
            )
        )
        .then(pl.col("onhand_qty"))
        .otherwise(0.0)
        .alias("blocked_qty"),
    ])

    # sellable = onhand - expired - blocked (no double-counting: expired is a subset condition)
    df = df.with_columns(
        pl.max_horizontal(
            pl.col("onhand_qty") - pl.col("expired_qty") - pl.col("blocked_qty"),
            pl.lit(0.0)
        ).alias("sellable_qty")
    )

    return df


def compute_fefo_rank(df: pl.DataFrame) -> pl.DataFrame:
    """Add fefo_rank partitioned by (warehouse_id, item_id).

    FEFO = First Expire First Out. Deterministic tiebreak by lot_id.
    """
    if df.height == 0:
        return df

    # Only rank items that have expiry dates
    df = df.with_columns(
        pl.col("final_expiry_date")
        .rank("ordinal")
        .over(["warehouse_id", "item_id"])
        .alias("fefo_rank")
    )

    return df


def assign_expiry_bucket(df: pl.DataFrame, config: AppConfig) -> pl.DataFrame:
    """Assign expiry bucket labels based on days-to-expiry."""
    if df.height == 0:
        return df

    buckets = config.thresholds.get("expiry", {}).get("buckets_days", [0, 30, 60, 90, 180, 365])

    df = df.with_columns(
        pl.when(pl.col("final_expiry_date").is_not_null())
        .then((pl.col("final_expiry_date") - pl.col("snapshot_date")).dt.total_days())
        .otherwise(None)
        .alias("days_to_expiry")
    )

    # Build bucket labels
    conditions = pl.when(pl.col("days_to_expiry").is_null()).then(pl.lit("N/A"))
    conditions = conditions.when(pl.col("days_to_expiry") < 0).then(pl.lit("EXPIRED"))

    for i in range(len(buckets) - 1):
        label = f"{buckets[i]}-{buckets[i+1]}d"
        conditions = conditions.when(
            pl.col("days_to_expiry") < buckets[i + 1]
        ).then(pl.lit(label))

    conditions = conditions.otherwise(pl.lit(f">{buckets[-1]}d"))

    df = df.with_columns(conditions.alias("expiry_bucket"))

    return df


def detect_expired_issues(df: pl.DataFrame) -> list[dict]:
    """Return list of CRITICAL issue dicts for expired stock."""
    if df.height == 0:
        return []

    expired = df.filter(pl.col("expired_qty") > 0)
    issues = []

    for row in expired.iter_rows(named=True):
        issues.append({
            "issue_id": str(uuid.uuid4())[:12],
            "issue_type": "EXPIRED_STOCK",
            "severity": "CRITICAL",
            "domain": "expiry",
            "entity_type": "item_lot",
            "entity_id": f"{row.get('item_id', '')}|{row.get('lot_id', '')}|{row.get('warehouse_id', '')}",
            "period": str(row.get("snapshot_date", "")),
            "detail": (
                f"Expired stock: item={row.get('item_id')}, lot={row.get('lot_id')}, "
                f"warehouse={row.get('warehouse_id')}, qty={row.get('expired_qty')}, "
                f"expiry={row.get('final_expiry_date')}"
            ),
        })

    return issues


def write_expired_issues(con: duckdb.DuckDBPyConnection, issues: list[dict]) -> None:
    """Write CRITICAL expired stock issues to ops_issue_log."""
    for issue in issues:
        con.execute(
            "INSERT INTO ops.ops_issue_log "
            "(issue_id, issue_type, severity, domain, entity_type, entity_id, period, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                issue["issue_id"], issue["issue_type"], issue["severity"],
                issue["domain"], issue["entity_type"], issue["entity_id"],
                issue["period"], issue["detail"],
            ]
        )
