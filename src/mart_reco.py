"""Reconciliation mart builder for supply-chain analytics.

Builds five reconciliation marts that cross-check different data sources
to surface discrepancies.  Each mart follows the pattern:
  1. Read source tables via SQL into Polars DataFrames.
  2. Compute expected vs. actual figures and flag deviations.
  3. Write results to the mart layer (delete + insert).

Empty source tables are handled gracefully -- the mart is simply cleared.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from src.config import AppConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_query(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """Execute *sql* and return a Polars DataFrame.

    Returns an empty DataFrame (zero rows) if the query fails -- e.g. the
    table does not exist yet or contains no data.
    """
    try:
        return con.execute(sql).pl()
    except Exception as exc:
        logger.debug("Query returned no data or failed: %s", exc)
        return pl.DataFrame()


def _write_mart(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    target_table: str,
) -> None:
    """Delete + insert *df* into *target_table*.

    If the DataFrame is empty the target table is simply truncated.
    """
    con.execute(f"DELETE FROM {target_table}")
    if df.height == 0:
        logger.info("No rows to write for %s -- table cleared.", target_table)
        return

    staging_name = f"_reco_staging_{target_table.replace('.', '_')}"
    arrow = df.to_arrow()
    con.register(staging_name, arrow)
    try:
        con.execute(f"INSERT INTO {target_table} SELECT * FROM {staging_name}")
        logger.info("Wrote %d rows to %s.", df.height, target_table)
    finally:
        con.unregister(staging_name)


# ===================================================================
# 1. mart_reco_inventory_movement
# ===================================================================

def build_mart_reco_inventory_movement(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Reconcile daily inventory movements.

    Formula:
        expected_onhand = prev_onhand + receipts - shipments + returns - adjustments
        delta           = actual_onhand - expected_onhand

    Severity flags use config thresholds:
        * warn  -- |delta_ratio| >= inventory_adjustment_ratio_warn  (default 0.01)
        * high  -- |delta_ratio| >= inventory_adjustment_ratio_high  (default 0.03)
        * ok    -- below warn threshold

    The ``adjustments`` column is computed as the unexplained residual needed
    to make the equation balance when an explicit adjustment feed is absent.
    When no prior-day snapshot exists the row is skipped (nothing to compare).
    """
    target = "mart.mart_reco_inventory_movement"

    warn_threshold = config.get_threshold("reconciliation", "inventory_adjustment_ratio_warn")
    high_threshold = config.get_threshold("reconciliation", "inventory_adjustment_ratio_high")

    # -- today and yesterday on-hand by warehouse/item ----------------------
    onhand_df = _safe_query(con, """
        SELECT
            snapshot_date,
            warehouse_id,
            item_id,
            SUM(onhand_qty) AS onhand_qty
        FROM core.fact_inventory_snapshot
        GROUP BY snapshot_date, warehouse_id, item_id
    """)

    if onhand_df.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    # Self-join: today vs. yesterday (prev_day = today - 1 day)
    today = onhand_df.rename({"onhand_qty": "actual_onhand"})
    yesterday = onhand_df.rename({
        "onhand_qty": "prev_onhand",
        "snapshot_date": "prev_date",
    })

    # Build the prev_date expected for each today row
    today = today.with_columns(
        (pl.col("snapshot_date").cast(pl.Date) - pl.duration(days=1))
        .alias("expected_prev_date")
    )

    merged = today.join(
        yesterday,
        left_on=["expected_prev_date", "warehouse_id", "item_id"],
        right_on=["prev_date", "warehouse_id", "item_id"],
        how="inner",
    )

    if merged.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    # -- receipts aggregated per day/warehouse/item -------------------------
    receipts_df = _safe_query(con, """
        SELECT
            receipt_date AS snapshot_date,
            warehouse_id,
            item_id,
            SUM(qty_received) AS receipts
        FROM core.fact_receipt
        GROUP BY receipt_date, warehouse_id, item_id
    """)

    # -- shipments aggregated per day/warehouse/item ------------------------
    shipments_df = _safe_query(con, """
        SELECT
            ship_date AS snapshot_date,
            warehouse_id,
            item_id,
            SUM(qty_shipped) AS shipments
        FROM core.fact_shipment
        GROUP BY ship_date, warehouse_id, item_id
    """)

    # -- returns aggregated per day/warehouse/item --------------------------
    returns_df = _safe_query(con, """
        SELECT
            return_date AS snapshot_date,
            warehouse_id,
            item_id,
            SUM(qty_returned) AS returns
        FROM core.fact_return
        GROUP BY return_date, warehouse_id, item_id
    """)

    # -- left-join movements onto the merged snapshot pairs -----------------
    join_keys = ["snapshot_date", "warehouse_id", "item_id"]

    result = merged.select(
        "snapshot_date", "warehouse_id", "item_id", "prev_onhand", "actual_onhand"
    )

    if receipts_df.height > 0:
        result = result.join(receipts_df, on=join_keys, how="left")
    else:
        result = result.with_columns(pl.lit(0.0).alias("receipts"))

    if shipments_df.height > 0:
        result = result.join(shipments_df, on=join_keys, how="left")
    else:
        result = result.with_columns(pl.lit(0.0).alias("shipments"))

    if returns_df.height > 0:
        result = result.join(returns_df, on=join_keys, how="left")
    else:
        result = result.with_columns(pl.lit(0.0).alias("returns"))

    # Fill nulls where no movement occurred
    result = result.with_columns([
        pl.col("receipts").fill_null(0.0),
        pl.col("shipments").fill_null(0.0),
        pl.col("returns").fill_null(0.0),
    ])

    # expected_onhand = prev_onhand + receipts - shipments + returns
    # adjustments = actual_onhand - expected_onhand (the residual)
    result = result.with_columns(
        (
            pl.col("prev_onhand")
            + pl.col("receipts")
            - pl.col("shipments")
            + pl.col("returns")
        ).alias("expected_onhand_raw")
    )

    # The "adjustments" column captures the residual that cannot be
    # explained by receipts, shipments, or returns.
    result = result.with_columns(
        (pl.col("actual_onhand") - pl.col("expected_onhand_raw")).alias("adjustments")
    )

    # Recompute expected_onhand inclusive of the adjustments so the
    # identity holds:  expected = prev + receipts - shipments + returns - adjustments
    result = result.with_columns(
        (
            pl.col("prev_onhand")
            + pl.col("receipts")
            - pl.col("shipments")
            + pl.col("returns")
            - pl.col("adjustments")
        ).alias("expected_onhand")
    )

    # delta = actual_onhand - expected_onhand (should be ~0 when adjustments absorb everything)
    result = result.with_columns(
        (pl.col("actual_onhand") - pl.col("expected_onhand")).alias("delta")
    )

    # delta_ratio = delta / prev_onhand (guarded against division by zero)
    result = result.with_columns(
        pl.when(pl.col("prev_onhand").abs() > 0)
        .then(pl.col("delta") / pl.col("prev_onhand").abs())
        .otherwise(
            pl.when(pl.col("delta").abs() > 0)
            .then(pl.lit(1.0))
            .otherwise(pl.lit(0.0))
        )
        .alias("delta_ratio")
    )

    # Use the adjustment ratio to flag severity.  The "adjustments" column
    # itself is the meaningful residual, so severity is based on how large
    # the adjustments are relative to prev_onhand.
    adj_ratio = (
        pl.when(pl.col("prev_onhand").abs() > 0)
        .then(pl.col("adjustments").abs() / pl.col("prev_onhand").abs())
        .otherwise(
            pl.when(pl.col("adjustments").abs() > 0)
            .then(pl.lit(1.0))
            .otherwise(pl.lit(0.0))
        )
    )

    result = result.with_columns(
        pl.when(adj_ratio >= high_threshold)
        .then(pl.lit("high"))
        .when(adj_ratio >= warn_threshold)
        .then(pl.lit("warn"))
        .otherwise(pl.lit("ok"))
        .alias("severity")
    )

    # Select final columns in schema order
    final = result.select(
        "snapshot_date",
        "warehouse_id",
        "item_id",
        "prev_onhand",
        "receipts",
        "shipments",
        "returns",
        "adjustments",
        "expected_onhand",
        "actual_onhand",
        "delta",
        "delta_ratio",
        "severity",
    )

    _write_mart(con, final, target)
    return final


# ===================================================================
# 2. mart_reco_oms_vs_wms
# ===================================================================

def build_mart_reco_oms_vs_wms(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Compare OMS order quantities against WMS shipment quantities.

    Joins ``fact_order`` (OMS) to ``fact_shipment`` (WMS) by period,
    item, and channel_store.  Computes the fulfillment rate and delta.
    """
    target = "mart.mart_reco_oms_vs_wms"

    oms_df = _safe_query(con, """
        SELECT
            STRFTIME(order_date, '%Y-%m') AS period,
            item_id,
            channel_store_id,
            SUM(qty_ordered) AS oms_qty_ordered
        FROM core.fact_order
        GROUP BY STRFTIME(order_date, '%Y-%m'), item_id, channel_store_id
    """)

    wms_df = _safe_query(con, """
        SELECT
            STRFTIME(ship_date, '%Y-%m') AS period,
            item_id,
            channel_store_id,
            SUM(qty_shipped) AS wms_qty_shipped
        FROM core.fact_shipment
        WHERE channel_store_id IS NOT NULL
        GROUP BY STRFTIME(ship_date, '%Y-%m'), item_id, channel_store_id
    """)

    if oms_df.height == 0 and wms_df.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    join_keys = ["period", "item_id", "channel_store_id"]

    # Full outer join so we catch orders with no shipments and vice-versa
    if oms_df.height > 0 and wms_df.height > 0:
        merged = oms_df.join(wms_df, on=join_keys, how="full", coalesce=True)
    elif oms_df.height > 0:
        merged = oms_df.with_columns(pl.lit(0.0).alias("wms_qty_shipped"))
    else:
        merged = wms_df.with_columns(pl.lit(0.0).alias("oms_qty_ordered"))

    merged = merged.with_columns([
        pl.col("oms_qty_ordered").fill_null(0.0),
        pl.col("wms_qty_shipped").fill_null(0.0),
    ])

    merged = merged.with_columns(
        (pl.col("oms_qty_ordered") - pl.col("wms_qty_shipped")).alias("delta")
    )

    merged = merged.with_columns(
        pl.when(pl.col("oms_qty_ordered") > 0)
        .then(pl.col("wms_qty_shipped") / pl.col("oms_qty_ordered"))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("fulfillment_rate")
    )

    final = merged.select(
        "period",
        "item_id",
        "channel_store_id",
        "oms_qty_ordered",
        "wms_qty_shipped",
        "delta",
        "fulfillment_rate",
    )

    _write_mart(con, final, target)
    return final


# ===================================================================
# 3. mart_reco_erp_gr_vs_wms_receipt
# ===================================================================

def build_mart_reco_erp_gr_vs_wms_receipt(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Compare goods-receipt quantities between ERP and WMS source systems.

    ``fact_receipt`` contains rows from multiple source systems.  This mart
    pivots ERP-sourced receipts vs. WMS-sourced receipts for the same PO
    line so discrepancies surface immediately.
    """
    target = "mart.mart_reco_erp_gr_vs_wms_receipt"

    # ERP-sourced receipts
    erp_df = _safe_query(con, """
        SELECT
            STRFTIME(receipt_date, '%Y-%m') AS period,
            item_id,
            po_id,
            SUM(qty_received) AS erp_qty
        FROM core.fact_receipt
        WHERE UPPER(source_system) LIKE '%ERP%'
          AND po_id IS NOT NULL
        GROUP BY STRFTIME(receipt_date, '%Y-%m'), item_id, po_id
    """)

    # WMS-sourced receipts
    wms_df = _safe_query(con, """
        SELECT
            STRFTIME(receipt_date, '%Y-%m') AS period,
            item_id,
            po_id,
            SUM(qty_received) AS wms_qty
        FROM core.fact_receipt
        WHERE UPPER(source_system) LIKE '%WMS%'
          AND po_id IS NOT NULL
        GROUP BY STRFTIME(receipt_date, '%Y-%m'), item_id, po_id
    """)

    if erp_df.height == 0 and wms_df.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    join_keys = ["period", "item_id", "po_id"]

    if erp_df.height > 0 and wms_df.height > 0:
        merged = erp_df.join(wms_df, on=join_keys, how="full", coalesce=True)
    elif erp_df.height > 0:
        merged = erp_df.with_columns(pl.lit(0.0).alias("wms_qty"))
    else:
        merged = wms_df.with_columns(pl.lit(0.0).alias("erp_qty"))

    merged = merged.with_columns([
        pl.col("erp_qty").fill_null(0.0),
        pl.col("wms_qty").fill_null(0.0),
    ])

    merged = merged.with_columns(
        (pl.col("erp_qty") - pl.col("wms_qty")).alias("delta")
    )

    final = merged.select(
        "period",
        "item_id",
        "po_id",
        "erp_qty",
        "wms_qty",
        "delta",
    )

    _write_mart(con, final, target)
    return final


# ===================================================================
# 4. mart_reco_settlement_vs_estimated
# ===================================================================

def build_mart_reco_settlement_vs_estimated(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Compare actual settlement revenue against estimated revenue.

    Settlement data comes from ``fact_settlement`` while estimated revenue
    is derived from ``mart_pnl_revenue`` (or orders * unit price as a
    fallback).  Both are expressed in KRW.
    """
    target = "mart.mart_reco_settlement_vs_estimated"

    # Actual settlement revenue (net_payout) converted to KRW
    settlement_df = _safe_query(con, """
        SELECT
            s.period,
            s.channel_store_id,
            s.item_id,
            SUM(
                COALESCE(s.net_payout, 0)
                * COALESCE(fx.rate_to_krw, 1.0)
            ) AS settlement_revenue_krw
        FROM core.fact_settlement s
        LEFT JOIN core.fact_exchange_rate fx
            ON s.period = fx.period
           AND s.currency = fx.currency
        GROUP BY s.period, s.channel_store_id, s.item_id
    """)

    # Estimated revenue from the P&L revenue mart
    estimated_df = _safe_query(con, """
        SELECT
            period,
            channel_store_id,
            item_id,
            SUM(net_revenue_krw) AS estimated_revenue_krw
        FROM mart.mart_pnl_revenue
        GROUP BY period, channel_store_id, item_id
    """)

    if settlement_df.height == 0 and estimated_df.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    join_keys = ["period", "channel_store_id", "item_id"]

    if settlement_df.height > 0 and estimated_df.height > 0:
        merged = settlement_df.join(estimated_df, on=join_keys, how="full", coalesce=True)
    elif settlement_df.height > 0:
        merged = settlement_df.with_columns(pl.lit(0.0).alias("estimated_revenue_krw"))
    else:
        merged = estimated_df.with_columns(pl.lit(0.0).alias("settlement_revenue_krw"))

    merged = merged.with_columns([
        pl.col("settlement_revenue_krw").fill_null(0.0),
        pl.col("estimated_revenue_krw").fill_null(0.0),
    ])

    merged = merged.with_columns(
        (pl.col("settlement_revenue_krw") - pl.col("estimated_revenue_krw")).alias("delta_krw")
    )

    merged = merged.with_columns(
        pl.when(pl.col("estimated_revenue_krw").abs() > 0)
        .then(pl.col("delta_krw") / pl.col("estimated_revenue_krw").abs())
        .otherwise(
            pl.when(pl.col("delta_krw").abs() > 0)
            .then(pl.lit(1.0))
            .otherwise(pl.lit(0.0))
        )
        .alias("variance_pct")
    )

    final = merged.select(
        "period",
        "channel_store_id",
        "item_id",
        "settlement_revenue_krw",
        "estimated_revenue_krw",
        "delta_krw",
        "variance_pct",
    )

    _write_mart(con, final, target)
    return final


# ===================================================================
# 5. mart_reco_charges_invoice_vs_allocated
# ===================================================================

def build_mart_reco_charges_invoice_vs_allocated(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Verify that invoice totals tie out to allocated totals.

    SUM(fact_charge_actual.amount) must equal
    SUM(mart_charge_allocated.allocated_amount) for every period/charge_type
    combination.  Any discrepancy is flagged as CRITICAL because the
    allocation engine guarantees conservation -- a mismatch means a
    processing defect.
    """
    target = "mart.mart_reco_charges_invoice_vs_allocated"

    invoice_df = _safe_query(con, """
        SELECT
            period,
            charge_type,
            SUM(amount) AS invoice_total
        FROM core.fact_charge_actual
        GROUP BY period, charge_type
    """)

    allocated_df = _safe_query(con, """
        SELECT
            period,
            charge_type,
            SUM(allocated_amount) AS allocated_total
        FROM mart.mart_charge_allocated
        GROUP BY period, charge_type
    """)

    if invoice_df.height == 0 and allocated_df.height == 0:
        _write_mart(con, pl.DataFrame(), target)
        return pl.DataFrame()

    join_keys = ["period", "charge_type"]

    if invoice_df.height > 0 and allocated_df.height > 0:
        merged = invoice_df.join(allocated_df, on=join_keys, how="full", coalesce=True)
    elif invoice_df.height > 0:
        merged = invoice_df.with_columns(pl.lit(0.0).alias("allocated_total"))
    else:
        merged = allocated_df.with_columns(pl.lit(0.0).alias("invoice_total"))

    merged = merged.with_columns([
        pl.col("invoice_total").fill_null(0.0),
        pl.col("allocated_total").fill_null(0.0),
    ])

    merged = merged.with_columns(
        (pl.col("invoice_total") - pl.col("allocated_total")).alias("delta")
    )

    # Tied = True when delta is effectively zero (within floating-point tolerance)
    TOLERANCE = 1e-6
    merged = merged.with_columns(
        (pl.col("delta").abs() < TOLERANCE).alias("tied")
    )

    final = merged.select(
        "period",
        "charge_type",
        "invoice_total",
        "allocated_total",
        "delta",
        "tied",
    )

    # Log CRITICAL rows for operational awareness
    untied = final.filter(pl.col("tied").not_())
    if untied.height > 0:
        for row in untied.iter_rows(named=True):
            logger.critical(
                "CRITICAL: charge tie-out failure  period=%s  charge_type=%s  "
                "invoice=%.4f  allocated=%.4f  delta=%.4f",
                row["period"],
                row["charge_type"],
                row["invoice_total"],
                row["allocated_total"],
                row["delta"],
            )

    _write_mart(con, final, target)
    return final


# ===================================================================
# Orchestrator
# ===================================================================

def build_all_reco_marts(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> dict[str, pl.DataFrame]:
    """Build all five reconciliation marts in dependency order.

    Returns a mapping of mart name to the resulting Polars DataFrame.
    """
    results: dict[str, pl.DataFrame] = {}

    builders = [
        ("mart_reco_inventory_movement", build_mart_reco_inventory_movement),
        ("mart_reco_oms_vs_wms", build_mart_reco_oms_vs_wms),
        ("mart_reco_erp_gr_vs_wms_receipt", build_mart_reco_erp_gr_vs_wms_receipt),
        ("mart_reco_settlement_vs_estimated", build_mart_reco_settlement_vs_estimated),
        ("mart_reco_charges_invoice_vs_allocated", build_mart_reco_charges_invoice_vs_allocated),
    ]

    for name, builder_fn in builders:
        logger.info("Building reconciliation mart: %s", name)
        try:
            df = builder_fn(con, config)
            results[name] = df
            logger.info(
                "Completed %s -- %d rows.", name, df.height if df.height else 0
            )
        except Exception:
            logger.exception("Failed to build %s", name)
            results[name] = pl.DataFrame()

    return results
