"""SCM mart builder: inventory, PO, stockout, overstock, expiry, FEFO, service-level.

Reads from CORE tables, applies business logic via Polars, and writes to MART schema.
Each builder function follows the DELETE + INSERT pattern so that mart tables are
always fully refreshed (idempotent).
"""
from __future__ import annotations

from datetime import date, timedelta
import logging

import duckdb
import polars as pl

from src.config import AppConfig
from src.expiry import (
    compute_final_expiry,
    compute_sellable_qty,
    compute_fefo_rank,
    assign_expiry_bucket,
    detect_expired_issues,
    write_expired_issues,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_mart(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    table: str,
) -> None:
    """Delete existing rows and insert *df* into *table*.

    Handles the empty-DataFrame case gracefully (just deletes existing rows).
    """
    con.execute(f"DELETE FROM {table}")
    if df.height == 0:
        logger.info("No rows to write for %s", table)
        return
    arrow = df.to_arrow()
    staging = f"_stg_{table.replace('.', '_')}"
    con.register(staging, arrow)
    con.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
    con.unregister(staging)
    logger.info("Wrote %d rows to %s", df.height, table)


def _safe_query(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """Execute *sql* and return a Polars DataFrame; return empty on error."""
    try:
        return con.execute(sql).pl()
    except Exception as exc:
        logger.warning("Query failed (%s): %s", exc, sql[:120])
        return pl.DataFrame()


# ---------------------------------------------------------------------------
# 1. mart_inventory_onhand
# ---------------------------------------------------------------------------

def build_mart_inventory_onhand(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_inventory_onhand from fact_inventory_snapshot + dim_item.

    Uses expiry module to compute final_expiry_date, sellable_qty, FEFO rank
    and expiry buckets.
    """
    # compute_final_expiry already joins snapshot with dim_item
    df = compute_final_expiry(con, config)
    if df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_inventory_onhand")
        return pl.DataFrame()

    df = compute_sellable_qty(df, config)
    df = compute_fefo_rank(df)
    df = assign_expiry_bucket(df, config)

    # Detect expired issues and write to ops log
    issues = detect_expired_issues(df)
    if issues:
        write_expired_issues(con, issues)

    # Select mart columns in the exact DDL order
    mart_cols = [
        "snapshot_date",
        "warehouse_id",
        "item_id",
        "lot_id",
        "onhand_qty",
        "sellable_qty",
        "blocked_qty",
        "expired_qty",
        "final_expiry_date",
        "expiry_bucket",
        "fefo_rank",
    ]

    # Use effective_min_sellable_days as min_sellable_days in the mart
    if "effective_min_sellable_days" in df.columns:
        df = df.with_columns(
            pl.col("effective_min_sellable_days").cast(pl.Int32).alias("min_sellable_days")
        )
    elif "min_sellable_days" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int32).alias("min_sellable_days"))

    mart_cols.append("min_sellable_days")

    # Ensure fefo_rank is INTEGER
    if "fefo_rank" in df.columns:
        df = df.with_columns(pl.col("fefo_rank").cast(pl.Int32))

    # Fill missing mart columns with nulls so SELECT never fails
    for c in mart_cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    result = df.select(mart_cols)
    _write_mart(con, result, "mart.mart_inventory_onhand")
    return result


# ---------------------------------------------------------------------------
# 2. mart_open_po
# ---------------------------------------------------------------------------

def build_mart_open_po(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_open_po.

    LEFT JOIN fact_po with aggregated fact_receipt on (po_id, item_id).
    qty_open = qty_ordered - COALESCE(qty_received, 0).
    delay_days = MAX(0, today - eta_date) when eta_date is in the past.
    """
    po_df = _safe_query(con, """
        SELECT po_id, item_id, supplier_id, po_date, eta_date,
               qty_ordered, currency, unit_price
        FROM core.fact_po
    """)
    if po_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_open_po")
        return pl.DataFrame()

    # Aggregate receipts per (po_id, item_id) — 수량 + 최초입고일
    rcpt_df = _safe_query(con, """
        SELECT po_id, item_id,
               SUM(qty_received) AS qty_received,
               MIN(receipt_date) AS first_receipt_date
        FROM core.fact_receipt
        WHERE po_id IS NOT NULL
        GROUP BY po_id, item_id
    """)

    # Left join
    if rcpt_df.height > 0:
        df = po_df.join(rcpt_df, on=["po_id", "item_id"], how="left")
    else:
        df = po_df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("qty_received"),
            pl.lit(None).cast(pl.Date).alias("first_receipt_date"),
        ])

    df = df.with_columns(
        pl.col("qty_received").fill_null(0.0).alias("qty_received"),
    )

    # qty_open
    df = df.with_columns(
        pl.max_horizontal(
            pl.col("qty_ordered") - pl.col("qty_received"),
            pl.lit(0.0),
        ).alias("qty_open")
    )

    # delay_days: how many days past the ETA (0 if not yet late or no ETA)
    today = date.today()
    df = df.with_columns(
        pl.when(
            pl.col("eta_date").is_not_null()
            & (pl.col("eta_date") < pl.lit(today))
        )
        .then((pl.lit(today) - pl.col("eta_date")).dt.total_days().cast(pl.Int32))
        .otherwise(pl.lit(0).cast(pl.Int32))
        .alias("delay_days")
    )

    # po_lead_days: 발주일 → 최초입고일 (실제 발주 리드타임)
    df = df.with_columns(
        pl.when(pl.col("first_receipt_date").is_not_null())
        .then(
            (pl.col("first_receipt_date") - pl.col("po_date"))
            .dt.total_days().cast(pl.Int32)
        )
        .otherwise(pl.lit(None).cast(pl.Int32))
        .alias("po_lead_days")
    )

    # eta_vs_actual_days: ETA 대비 실제 입고 차이 (양수=지연, 음수=조기입고)
    df = df.with_columns(
        pl.when(
            pl.col("first_receipt_date").is_not_null()
            & pl.col("eta_date").is_not_null()
        )
        .then(
            (pl.col("first_receipt_date") - pl.col("eta_date"))
            .dt.total_days().cast(pl.Int32)
        )
        .otherwise(pl.lit(None).cast(pl.Int32))
        .alias("eta_vs_actual_days")
    )

    # period = YYYY-MM of po_date
    df = df.with_columns(
        pl.col("po_date").dt.strftime("%Y-%m").alias("period")
    )

    result = df.select([
        "po_id", "item_id", "supplier_id", "po_date", "eta_date",
        "first_receipt_date",
        "qty_ordered", "qty_received", "qty_open", "delay_days",
        "po_lead_days", "eta_vs_actual_days", "period",
    ])

    _write_mart(con, result, "mart.mart_open_po")
    return result


# ---------------------------------------------------------------------------
# 3. mart_stockout_risk
# ---------------------------------------------------------------------------

def build_mart_stockout_risk(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_stockout_risk.

    Compare current sellable_qty per (item, warehouse) against average daily
    demand derived from fact_shipment.  days_of_cover = sellable_qty / avg_daily.
    Risk when days_of_cover < threshold from thresholds.yaml.
    """
    # Latest inventory snapshot per (warehouse, item)
    inv_df = _safe_query(con, """
        SELECT warehouse_id, item_id, snapshot_date,
               SUM(onhand_qty) AS onhand_qty
        FROM core.fact_inventory_snapshot
        GROUP BY warehouse_id, item_id, snapshot_date
    """)
    if inv_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_stockout_risk")
        return pl.DataFrame()

    # Keep only the latest snapshot per (warehouse, item)
    inv_df = (
        inv_df
        .sort("snapshot_date", descending=True)
        .group_by(["warehouse_id", "item_id"])
        .agg([
            pl.col("onhand_qty").first().alias("onhand_qty"),
            pl.col("snapshot_date").first().alias("as_of_date"),
        ])
    )

    # Compute sellable qty from expiry module for richer data
    expiry_df = compute_final_expiry(con, config)
    if expiry_df.height > 0:
        expiry_df = compute_sellable_qty(expiry_df, config)
        sellable_agg = (
            expiry_df
            .group_by(["warehouse_id", "item_id"])
            .agg(pl.col("sellable_qty").sum().alias("sellable_qty"))
        )
        inv_df = inv_df.join(sellable_agg, on=["warehouse_id", "item_id"], how="left")
        inv_df = inv_df.with_columns(
            pl.col("sellable_qty").fill_null(pl.col("onhand_qty")).alias("sellable_qty")
        )
    else:
        inv_df = inv_df.with_columns(pl.col("onhand_qty").alias("sellable_qty"))

    # Avg daily demand from sales shipments only (channel_order_id IS NOT NULL)
    demand_df = _safe_query(con, """
        SELECT warehouse_id, item_id,
               SUM(qty_shipped) AS total_shipped,
               COUNT(DISTINCT ship_date) AS active_days,
               MIN(ship_date) AS first_ship,
               MAX(ship_date) AS last_ship
        FROM core.fact_shipment
        WHERE channel_order_id IS NOT NULL
        GROUP BY warehouse_id, item_id
    """)

    if demand_df.height > 0:
        # avg_daily_demand = total_shipped / span in calendar days (min 1)
        demand_df = demand_df.with_columns(
            pl.max_horizontal(
                (pl.col("last_ship") - pl.col("first_ship")).dt.total_days() + 1,
                pl.lit(1),
            ).alias("span_days")
        )
        demand_df = demand_df.with_columns(
            (pl.col("total_shipped") / pl.col("span_days")).alias("avg_daily_demand")
        ).select(["warehouse_id", "item_id", "avg_daily_demand"])

        df = inv_df.join(demand_df, on=["warehouse_id", "item_id"], how="left")
    else:
        df = inv_df.with_columns(pl.lit(0.0).alias("avg_daily_demand"))

    df = df.with_columns(pl.col("avg_daily_demand").fill_null(0.0))

    # days_of_cover
    df = df.with_columns(
        pl.when(pl.col("avg_daily_demand") > 0)
        .then(pl.col("sellable_qty") / pl.col("avg_daily_demand"))
        .otherwise(pl.lit(float("inf")))
        .alias("days_of_cover")
    )

    # Threshold from config
    default_threshold = int(
        config.get_threshold("inventory", "stockout_days_cover", "default")
    )
    df = df.with_columns(pl.lit(default_threshold).alias("threshold_days"))

    # Risk flag
    df = df.with_columns(
        (pl.col("days_of_cover") < pl.col("threshold_days")).alias("risk_flag")
    )

    result = df.select([
        "item_id", "warehouse_id", "sellable_qty", "avg_daily_demand",
        "days_of_cover", "threshold_days", "risk_flag", "as_of_date",
    ])

    _write_mart(con, result, "mart.mart_stockout_risk")
    return result


# ---------------------------------------------------------------------------
# 4. mart_overstock
# ---------------------------------------------------------------------------

def build_mart_overstock(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_overstock.

    days_on_hand = onhand_qty / avg_daily_demand.
    Overstock when days_on_hand > doh_overstock threshold per item_type.
    """
    # Latest inventory aggregated by (warehouse, item)
    inv_df = _safe_query(con, """
        SELECT i.warehouse_id, i.item_id,
               COALESCE(d.item_type, 'FG') AS item_type,
               SUM(i.onhand_qty) AS onhand_qty,
               MAX(i.snapshot_date) AS as_of_date
        FROM core.fact_inventory_snapshot i
        LEFT JOIN core.dim_item d ON i.item_id = d.item_id
        GROUP BY i.warehouse_id, i.item_id, d.item_type
    """)
    if inv_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_overstock")
        return pl.DataFrame()

    # Avg daily demand from sales shipments only (channel_order_id IS NOT NULL)
    demand_df = _safe_query(con, """
        SELECT warehouse_id, item_id,
               SUM(qty_shipped) AS total_shipped,
               MIN(ship_date) AS first_ship,
               MAX(ship_date) AS last_ship
        FROM core.fact_shipment
        WHERE channel_order_id IS NOT NULL
        GROUP BY warehouse_id, item_id
    """)

    if demand_df.height > 0:
        demand_df = demand_df.with_columns(
            pl.max_horizontal(
                (pl.col("last_ship") - pl.col("first_ship")).dt.total_days() + 1,
                pl.lit(1),
            ).alias("span_days")
        )
        demand_df = demand_df.with_columns(
            (pl.col("total_shipped") / pl.col("span_days")).alias("avg_daily_demand")
        ).select(["warehouse_id", "item_id", "avg_daily_demand"])

        df = inv_df.join(demand_df, on=["warehouse_id", "item_id"], how="left")
    else:
        df = inv_df.with_columns(pl.lit(0.0).alias("avg_daily_demand"))

    df = df.with_columns(pl.col("avg_daily_demand").fill_null(0.0))

    # days_on_hand
    df = df.with_columns(
        pl.when(pl.col("avg_daily_demand") > 0)
        .then(pl.col("onhand_qty") / pl.col("avg_daily_demand"))
        .otherwise(pl.lit(float("inf")))
        .alias("days_on_hand")
    )

    # DOH thresholds per item_type from config
    doh_map: dict = config.thresholds.get("inventory", {}).get("doh_overstock", {})

    # Build threshold column by mapping item_type
    default_doh = 90  # fallback if item_type not in map
    df = df.with_columns(
        pl.col("item_type")
        .replace_strict(doh_map, default=default_doh, return_dtype=pl.Int64)
        .cast(pl.Int32)
        .alias("doh_threshold")
    )

    # Overstock flag
    df = df.with_columns(
        (pl.col("days_on_hand") > pl.col("doh_threshold")).alias("overstock_flag")
    )

    # Overstock qty: excess quantity beyond threshold days of demand
    df = df.with_columns(
        pl.when(pl.col("overstock_flag"))
        .then(
            pl.max_horizontal(
                pl.col("onhand_qty") - pl.col("avg_daily_demand") * pl.col("doh_threshold"),
                pl.lit(0.0),
            )
        )
        .otherwise(0.0)
        .alias("overstock_qty")
    )

    result = df.select([
        "item_id", "warehouse_id", "item_type", "onhand_qty",
        "avg_daily_demand", "days_on_hand", "doh_threshold",
        "overstock_flag", "overstock_qty", "as_of_date",
    ])

    _write_mart(con, result, "mart.mart_overstock")
    return result


# ---------------------------------------------------------------------------
# 5. mart_expiry_risk
# ---------------------------------------------------------------------------

def build_mart_expiry_risk(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_expiry_risk.

    Items where the sellable window is closing.  Enriched with expiry buckets
    from thresholds.yaml.  Only includes lots that have an expiry date.
    """
    df = compute_final_expiry(con, config)
    if df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_expiry_risk")
        return pl.DataFrame()

    df = assign_expiry_bucket(df, config)

    # Filter to only expiry-tracked lots (with a final_expiry_date)
    df = df.filter(pl.col("final_expiry_date").is_not_null())
    if df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_expiry_risk")
        return pl.DataFrame()

    # Ensure days_to_expiry is present (assign_expiry_bucket adds it)
    if "days_to_expiry" not in df.columns:
        df = df.with_columns(
            (pl.col("final_expiry_date") - pl.col("snapshot_date"))
            .dt.total_days()
            .alias("days_to_expiry")
        )

    # Approximate risk value using cost_structure if available.
    # Pre-aggregate cost_components per (item_id, effective_from) to avoid join explosion.
    # As-of join: pick latest effective_from <= snapshot_date per grain.
    cost_df = _safe_query(con, """
        SELECT item_id, effective_from,
               SUM(cost_per_unit_krw) as cost_per_unit_krw
        FROM core.fact_cost_structure
        GROUP BY item_id, effective_from
    """)

    if cost_df.height > 0:
        # As-of join in Polars: for each (item_id), pick latest effective_from <= snapshot_date
        # Grain for expiry_risk = (item_id, warehouse_id, lot_id, snapshot_date)
        # Since cost only varies by item_id, we can do item-level as-of first then join back.
        if "snapshot_date" in df.columns:
            # Cross join then filter: effective_from <= snapshot_date
            items_with_dates = df.select(["item_id", "snapshot_date"]).unique()
            # Use SQL for the as-of join to be safe
            con.register("_expiry_items", items_with_dates.to_arrow())
            con.register("_cost_agg", cost_df.to_arrow())
            cost_matched = con.execute("""
                WITH ranked AS (
                    SELECT i.item_id, i.snapshot_date, c.cost_per_unit_krw,
                           ROW_NUMBER() OVER (
                               PARTITION BY i.item_id, i.snapshot_date
                               ORDER BY c.effective_from DESC
                           ) as rn
                    FROM _expiry_items i
                    LEFT JOIN _cost_agg c
                        ON i.item_id = c.item_id
                        AND c.effective_from <= i.snapshot_date
                )
                SELECT item_id, snapshot_date, cost_per_unit_krw
                FROM ranked WHERE rn = 1
            """).pl()
            con.unregister("_expiry_items")
            con.unregister("_cost_agg")

            df = df.join(cost_matched, on=["item_id", "snapshot_date"], how="left")
        else:
            # Fallback: just use latest cost per item
            latest = cost_df.sort("effective_from", descending=True).group_by("item_id").first()
            df = df.join(latest.select(["item_id", "cost_per_unit_krw"]), on="item_id", how="left")

        # NULL cost stays NULL -> risk_value_krw = NULL (no fill_null(0))
        df = df.with_columns(
            (pl.col("onhand_qty") * pl.col("cost_per_unit_krw"))
            .alias("risk_value_krw")
        )
        # Log coverage
        null_cost = df.filter(pl.col("cost_per_unit_krw").is_null())
        if null_cost.height > 0:
            import logging
            logging.getLogger(__name__).warning(
                "Expiry risk: cost missing for %d items — risk_value_krw set to NULL",
                null_cost.height,
            )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("risk_value_krw"))

    # as_of_date = snapshot_date
    df = df.with_columns(pl.col("snapshot_date").alias("as_of_date"))

    result = df.select([
        "item_id", "warehouse_id", "lot_id", "onhand_qty",
        "final_expiry_date",
        pl.col("days_to_expiry").cast(pl.Int32),
        "expiry_bucket", "risk_value_krw", "as_of_date",
    ])

    _write_mart(con, result, "mart.mart_expiry_risk")
    return result


# ---------------------------------------------------------------------------
# 6. mart_fefo_pick_list
# ---------------------------------------------------------------------------

def build_mart_fefo_pick_list(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_fefo_pick_list.

    Ordered by fefo_rank per (warehouse_id, item_id).
    """
    df = compute_final_expiry(con, config)
    if df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_fefo_pick_list")
        return pl.DataFrame()

    df = compute_sellable_qty(df, config)
    df = compute_fefo_rank(df)

    # Ensure fefo_rank is INTEGER
    if "fefo_rank" in df.columns:
        df = df.with_columns(pl.col("fefo_rank").cast(pl.Int32))

    # Sort by warehouse, item, fefo_rank for a usable pick list
    sort_cols = ["warehouse_id", "item_id", "fefo_rank"]
    available_sort = [c for c in sort_cols if c in df.columns]
    if available_sort:
        df = df.sort(available_sort)

    result = df.select([
        "warehouse_id", "item_id", "lot_id", "onhand_qty",
        "sellable_qty", "final_expiry_date", "fefo_rank", "snapshot_date",
    ])

    _write_mart(con, result, "mart.mart_fefo_pick_list")
    return result


# ---------------------------------------------------------------------------
# 7. mart_service_level
# ---------------------------------------------------------------------------

def build_mart_service_level(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_service_level.

    Weekly shipped_on_time / total_orders from fact_order + fact_shipment.
    An order is 'on time' if at least one shipment exists with
    ship_date <= order_date + 3 days (standard promise window).
    """
    orders_df = _safe_query(con, """
        SELECT channel_order_id, line_no, order_date, channel_store_id,
               item_id, qty_ordered, ship_from_warehouse_id
        FROM core.fact_order
    """)
    if orders_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_service_level")
        return pl.DataFrame()

    shipments_df = _safe_query(con, """
        SELECT channel_order_id, MIN(ship_date) AS earliest_ship_date
        FROM core.fact_shipment
        WHERE channel_order_id IS NOT NULL
        GROUP BY channel_order_id
    """)

    # Join orders with earliest shipment date
    if shipments_df.height > 0:
        df = orders_df.join(shipments_df, on="channel_order_id", how="left")
    else:
        df = orders_df.with_columns(
            pl.lit(None).cast(pl.Date).alias("earliest_ship_date")
        )

    # Determine on-time: shipped within 3 days of order_date
    df = df.with_columns(
        pl.when(
            pl.col("earliest_ship_date").is_not_null()
            & (pl.col("earliest_ship_date") <= (pl.col("order_date") + pl.duration(days=3)))
        )
        .then(True)
        .otherwise(False)
        .alias("is_on_time")
    )

    # Week start = Monday of the order week (ISO week start)
    df = df.with_columns(
        (pl.col("order_date") - pl.col("order_date").dt.weekday().cast(pl.Duration("d")))
        .alias("week_start")
    )

    # Aggregate weekly by channel_store_id
    weekly = (
        df
        .group_by(["week_start", "channel_store_id"])
        .agg([
            pl.len().alias("total_orders"),
            pl.col("is_on_time").sum().alias("shipped_on_time"),
        ])
    )

    weekly = weekly.with_columns([
        pl.col("total_orders").cast(pl.Int64),
        pl.col("shipped_on_time").cast(pl.Int64),
    ])

    weekly = weekly.with_columns(
        pl.when(pl.col("total_orders") > 0)
        .then(pl.col("shipped_on_time") / pl.col("total_orders"))
        .otherwise(0.0)
        .alias("service_level_pct")
    )

    result = weekly.select([
        "week_start", "channel_store_id",
        "total_orders", "shipped_on_time", "service_level_pct",
    ])

    _write_mart(con, result, "mart.mart_service_level")
    return result


# ---------------------------------------------------------------------------
# 8. mart_shipment_performance
# ---------------------------------------------------------------------------

def build_mart_shipment_performance(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_shipment_performance.

    출고 현황을 기간/창고/채널별로 집계합니다.
    주문 대비 리드타임(주문일→출고일) 및 정시출고율 포함.
    """
    shipments_df = _safe_query(con, """
        SELECT s.shipment_id, s.ship_date, s.warehouse_id, s.item_id,
               s.qty_shipped, s.weight, s.volume_cbm,
               s.channel_order_id, s.channel_store_id,
               STRFTIME(s.ship_date, '%Y-%m') AS period
        FROM core.fact_shipment s
    """)
    if shipments_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_shipment_performance")
        return pl.DataFrame()

    # 주문 데이터에서 order_date 가져오기 (리드타임 계산용)
    orders_df = _safe_query(con, """
        SELECT channel_order_id, MIN(order_date) AS order_date
        FROM core.fact_order
        WHERE channel_order_id IS NOT NULL
        GROUP BY channel_order_id
    """)

    if orders_df.height > 0:
        shipments_df = shipments_df.join(
            orders_df, on="channel_order_id", how="left"
        )
        shipments_df = shipments_df.with_columns([
            (pl.col("ship_date") - pl.col("order_date")).dt.total_days().alias("lead_days"),
            pl.when(
                pl.col("order_date").is_not_null()
                & (pl.col("ship_date") <= (pl.col("order_date") + pl.duration(days=3)))
            ).then(True).otherwise(False).alias("is_on_time"),
        ])
    else:
        shipments_df = shipments_df.with_columns([
            pl.lit(None).cast(pl.Int64).alias("lead_days"),
            pl.lit(False).alias("is_on_time"),
        ])

    # 기간/창고/채널별 집계
    agg = (
        shipments_df
        .group_by(["period", "warehouse_id", "channel_store_id"])
        .agg([
            pl.col("shipment_id").n_unique().alias("total_shipments"),
            pl.col("qty_shipped").sum().alias("total_qty_shipped"),
            pl.col("weight").sum().alias("total_weight"),
            pl.col("volume_cbm").sum().alias("total_volume_cbm"),
            pl.col("qty_shipped").mean().alias("avg_qty_per_shipment"),
            pl.col("lead_days").mean().alias("avg_lead_days"),
            pl.col("is_on_time").sum().alias("on_time_count"),
        ])
    )

    agg = agg.with_columns([
        pl.col("total_shipments").cast(pl.Int64),
        pl.col("on_time_count").cast(pl.Int64),
        pl.when(pl.col("total_shipments") > 0)
        .then(pl.col("on_time_count") / pl.col("total_shipments"))
        .otherwise(0.0)
        .alias("on_time_pct"),
    ])

    result = agg.select([
        "period", "warehouse_id", "channel_store_id",
        "total_shipments", "total_qty_shipped",
        "total_weight", "total_volume_cbm",
        "avg_qty_per_shipment", "avg_lead_days",
        "on_time_count", "on_time_pct",
    ])

    _write_mart(con, result, "mart.mart_shipment_performance")
    return result


# ---------------------------------------------------------------------------
# 9. mart_shipment_daily
# ---------------------------------------------------------------------------

def build_mart_shipment_daily(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_shipment_daily.

    일별 출고 추이를 집계합니다.
    """
    shipments_df = _safe_query(con, """
        SELECT ship_date, warehouse_id, shipment_id,
               item_id, qty_shipped, weight, volume_cbm,
               channel_order_id
        FROM core.fact_shipment
    """)
    if shipments_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_shipment_daily")
        return pl.DataFrame()

    agg = (
        shipments_df
        .group_by(["ship_date", "warehouse_id"])
        .agg([
            pl.col("shipment_id").n_unique().alias("shipment_count"),
            pl.col("qty_shipped").sum().alias("qty_shipped"),
            pl.col("weight").sum().alias("weight"),
            pl.col("volume_cbm").sum().alias("volume_cbm"),
            pl.col("channel_order_id").n_unique().alias("unique_orders"),
            pl.col("item_id").n_unique().alias("unique_items"),
        ])
    )

    result = agg.select([
        "ship_date", "warehouse_id",
        "shipment_count", "qty_shipped", "weight", "volume_cbm",
        "unique_orders", "unique_items",
    ]).with_columns([
        pl.col("shipment_count").cast(pl.Int64),
        pl.col("unique_orders").cast(pl.Int64),
        pl.col("unique_items").cast(pl.Int64),
    ])

    _write_mart(con, result, "mart.mart_shipment_daily")
    return result


# ---------------------------------------------------------------------------
# 10. mart_return_analysis
# ---------------------------------------------------------------------------

def build_mart_return_analysis(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_return_analysis.

    반품을 기간/품목/사유/처분별로 집계하고 출고 대비 반품율을 계산합니다.
    """
    returns_df = _safe_query(con, """
        SELECT r.return_id, r.return_date, r.warehouse_id, r.item_id,
               r.qty_returned, r.channel_order_id, r.reason, r.disposition,
               STRFTIME(r.return_date, '%Y-%m') AS period
        FROM core.fact_return r
    """)
    if returns_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_return_analysis")
        return pl.DataFrame()

    # 채널 정보 추가 (주문을 통해)
    orders_df = _safe_query(con, """
        SELECT channel_order_id, channel_store_id
        FROM core.fact_order
        WHERE channel_order_id IS NOT NULL
        GROUP BY channel_order_id, channel_store_id
    """)

    if orders_df.height > 0:
        returns_df = returns_df.join(orders_df, on="channel_order_id", how="left")
    else:
        returns_df = returns_df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("channel_store_id")
        )

    # 같은 기간 품목별 출고 수량 (반품율 계산용)
    shipped_df = _safe_query(con, """
        SELECT STRFTIME(ship_date, '%Y-%m') AS period,
               item_id, warehouse_id,
               SUM(qty_shipped) AS qty_shipped
        FROM core.fact_shipment
        GROUP BY 1, 2, 3
    """)

    # 기간/품목/창고/채널/사유/처분별 집계
    agg = (
        returns_df
        .group_by(["period", "item_id", "warehouse_id", "channel_store_id", "reason", "disposition"])
        .agg([
            pl.col("return_id").n_unique().alias("return_count"),
            pl.col("qty_returned").sum().alias("qty_returned"),
        ])
    )

    agg = agg.with_columns(pl.col("return_count").cast(pl.Int64))

    # 출고 수량 조인해서 반품율 계산
    if shipped_df.height > 0:
        agg = agg.join(
            shipped_df,
            on=["period", "item_id", "warehouse_id"],
            how="left",
        )
    else:
        agg = agg.with_columns(pl.lit(0.0).alias("qty_shipped"))

    agg = agg.with_columns(
        pl.when(pl.col("qty_shipped") > 0)
        .then(pl.col("qty_returned") / pl.col("qty_shipped"))
        .otherwise(0.0)
        .alias("return_rate")
    )

    result = agg.select([
        "period", "item_id", "warehouse_id", "channel_store_id",
        "reason", "disposition",
        "return_count", "qty_returned", "qty_shipped", "return_rate",
    ])

    _write_mart(con, result, "mart.mart_return_analysis")
    return result


# ---------------------------------------------------------------------------
# 11. mart_return_daily
# ---------------------------------------------------------------------------

def build_mart_return_daily(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> pl.DataFrame:
    """Build mart.mart_return_daily.

    일별 반품 추이를 집계합니다.
    """
    returns_df = _safe_query(con, """
        SELECT return_date, warehouse_id, return_id,
               item_id, qty_returned, channel_order_id, reason
        FROM core.fact_return
    """)
    if returns_df.height == 0:
        _write_mart(con, pl.DataFrame(), "mart.mart_return_daily")
        return pl.DataFrame()

    agg = (
        returns_df
        .group_by(["return_date", "warehouse_id"])
        .agg([
            pl.col("return_id").n_unique().alias("return_count"),
            pl.col("qty_returned").sum().alias("qty_returned"),
            pl.col("channel_order_id").n_unique().alias("unique_orders"),
            pl.col("item_id").n_unique().alias("unique_items"),
            pl.col("reason").mode().first().alias("top_reason"),
        ])
    )

    result = agg.select([
        "return_date", "warehouse_id",
        "return_count", "qty_returned",
        "unique_orders", "unique_items", "top_reason",
    ]).with_columns([
        pl.col("return_count").cast(pl.Int64),
        pl.col("unique_orders").cast(pl.Int64),
        pl.col("unique_items").cast(pl.Int64),
    ])

    _write_mart(con, result, "mart.mart_return_daily")
    return result


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

_MART_BUILDERS = [
    ("mart_inventory_onhand",      build_mart_inventory_onhand),
    ("mart_open_po",               build_mart_open_po),
    ("mart_stockout_risk",         build_mart_stockout_risk),
    ("mart_overstock",             build_mart_overstock),
    ("mart_expiry_risk",           build_mart_expiry_risk),
    ("mart_fefo_pick_list",        build_mart_fefo_pick_list),
    ("mart_service_level",         build_mart_service_level),
    ("mart_shipment_performance",  build_mart_shipment_performance),
    ("mart_shipment_daily",        build_mart_shipment_daily),
    ("mart_return_analysis",       build_mart_return_analysis),
    ("mart_return_daily",          build_mart_return_daily),
]


def build_all_scm_marts(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
) -> dict[str, int]:
    """Build all SCM mart tables in dependency order.

    Returns a dict mapping mart name to row count written.
    """
    results: dict[str, int] = {}

    for name, builder in _MART_BUILDERS:
        logger.info("Building %s ...", name)
        try:
            df = builder(con, config)
            results[name] = df.height if isinstance(df, pl.DataFrame) else 0
            logger.info("  -> %s: %d rows", name, results[name])
        except Exception as exc:
            logger.error("Failed to build %s: %s", name, exc, exc_info=True)
            results[name] = -1

    return results
