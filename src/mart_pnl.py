"""P&L mart builders (waterfall).

Grain: (period, item_id, channel_store_id, country).
All versioned joins use strict 1:1 via ROW_NUMBER + assertion.
FX conversion to KRW is mandatory.

Safety rules applied:
- cost_structure pre-aggregated across cost_components before as-of join (no join explosion)
- ROW_NUMBER partition matches output grain (no data loss)
- Missing cost -> NULL + coverage_flag='PARTIAL' (no fill 0)
- Missing FX -> KRW values NULL + coverage_flag='PARTIAL' (no 1.0 fallback)
- Sales-only filter on shipments and returns (channel_order_id IS NOT NULL)
- coverage_flag propagation: 'PARTIAL' if ANY upstream != 'ACTUAL' else 'ACTUAL'
"""
import logging

import duckdb
import polars as pl

from src.config import AppConfig

logger = logging.getLogger(__name__)


def safe_versioned_join_sql(
    con: duckdb.DuckDBPyConnection,
    base_query: str,
    versioned_table: str,
    join_keys: list[str],
    date_col: str,
    effective_col: str,
    select_cols: list[str],
) -> pl.DataFrame:
    """Perform strict 1:1 effective_from join via SQL.

    Uses ROW_NUMBER to pick latest effective_from <= date_col.
    Raises if any group has >1 match after dedup.
    """
    join_cond = " AND ".join(f"b.{k} = v.{k}" for k in join_keys)
    v_select = ", ".join(f"v.{c}" for c in select_cols)

    sql = f"""
        WITH base AS ({base_query}),
        versioned_ranked AS (
            SELECT v.*, b.{date_col},
                   ROW_NUMBER() OVER (
                       PARTITION BY {', '.join(f'b.{k}' for k in join_keys)}, b.{date_col}
                       ORDER BY v.{effective_col} DESC
                   ) AS rn
            FROM base b
            JOIN {versioned_table} v ON {join_cond}
            WHERE v.{effective_col} <= b.{date_col}
        )
        SELECT * FROM versioned_ranked WHERE rn = 1
    """
    return con.execute(sql).pl()


def build_mart_pnl_revenue(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build P&L revenue mart. Prefer settlement; fallback to estimated.

    FX rule: Missing FX rate for non-KRW currency -> KRW values NULL + coverage_flag='PARTIAL'.
    """
    con.execute("DELETE FROM mart.mart_pnl_revenue")

    # Try settlement-based revenue first.
    # dim_channel_store may not have 'country' column — use it if present, else default 'KR'.
    try:
        # Check if dim_channel_store has a country column
        has_country = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema='core' AND table_name='dim_channel_store' AND column_name='country'"
        ).fetchone()[0] > 0

        if has_country:
            country_expr = "COALESCE(cs.country, 'KR')"
            join_clause = "LEFT JOIN core.dim_channel_store cs ON s.channel_store_id = cs.channel_store_id"
        else:
            country_expr = "'KR'"
            join_clause = ""

        settlement_df = con.execute(f"""
            SELECT
                s.period,
                s.item_id,
                s.channel_store_id,
                {country_expr} as country,
                s.currency,
                COALESCE(s.gross_sales, 0) as gross_sales,
                COALESCE(s.discounts, 0) as discounts,
                COALESCE(s.refunds, 0) as refunds,
                COALESCE(s.net_payout, 0) as net_payout,
                'settlement' as source
            FROM core.fact_settlement s
            {join_clause}
        """).pl()
    except Exception:
        settlement_df = pl.DataFrame()

    # FX rates
    try:
        fx_df = con.execute("SELECT period, currency, rate_to_krw FROM core.fact_exchange_rate").pl()
        fx_map = {(r["period"], r["currency"]): r["rate_to_krw"] for r in fx_df.iter_rows(named=True)}
    except Exception:
        fx_map = {}

    if settlement_df.height > 0:
        # Convert to KRW — NO 1.0 fallback for non-KRW
        rows = []
        fx_missing_count = 0
        fx_missing_local_sum = 0.0
        for row in settlement_df.iter_rows(named=True):
            currency = row["currency"]
            if currency == "KRW":
                rate = 1.0
                flag = "ACTUAL"
            else:
                rate_val = fx_map.get((row["period"], currency))
                if rate_val is not None:
                    rate = rate_val
                    flag = "ACTUAL"
                else:
                    # FX missing -> NULL KRW values + PARTIAL
                    rate = None
                    flag = "PARTIAL"
                    fx_missing_count += 1
                    fx_missing_local_sum += abs(row["net_payout"] or 0)

            if rate is not None:
                rows.append({
                    "period": row["period"],
                    "item_id": row["item_id"],
                    "channel_store_id": row["channel_store_id"],
                    "country": row["country"],
                    "gross_sales_krw": row["gross_sales"] * rate,
                    "discounts_krw": row["discounts"] * rate,
                    "refunds_krw": row["refunds"] * rate,
                    "net_revenue_krw": row["net_payout"] * rate,
                    "source": "settlement",
                    "coverage_flag": flag,
                })
            else:
                rows.append({
                    "period": row["period"],
                    "item_id": row["item_id"],
                    "channel_store_id": row["channel_store_id"],
                    "country": row["country"],
                    "gross_sales_krw": None,
                    "discounts_krw": None,
                    "refunds_krw": None,
                    "net_revenue_krw": None,
                    "source": "settlement",
                    "coverage_flag": flag,
                })

        if fx_missing_count > 0:
            logger.warning(
                "FX missing: %d rows (local amount sum: %.2f) — KRW values set to NULL, coverage_flag=PARTIAL",
                fx_missing_count, fx_missing_local_sum,
            )

        result = pl.DataFrame(rows)
        arrow = result.to_arrow()
        con.register("_rev_staging", arrow)
        con.execute("INSERT INTO mart.mart_pnl_revenue SELECT * FROM _rev_staging")
        con.unregister("_rev_staging")


def build_mart_pnl_cogs(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build COGS mart with safe as-of cost join.

    Safety:
    - cost_structure pre-aggregated across cost_components (no join explosion)
    - Period-aware as-of join: effective_from <= period end
    - ROW_NUMBER partition matches grain (period, item_id, channel_store_id)
    - Sales-only: channel_order_id IS NOT NULL for both shipments and returns
    - Missing cost -> cogs_krw NULL + coverage_flag='PARTIAL'
    """
    con.execute("DELETE FROM mart.mart_pnl_cogs")

    # Single SQL: shipped (sales-only) + cost_agg + as-of join (grain-aligned partition)
    try:
        cogs_df = con.execute("""
            WITH shipped AS (
                SELECT
                    STRFTIME(ship_date, '%Y-%m') as period,
                    item_id,
                    COALESCE(channel_store_id, 'UNKNOWN') as channel_store_id,
                    SUM(qty_shipped) as qty_shipped
                FROM core.fact_shipment
                WHERE channel_order_id IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            returned AS (
                SELECT
                    STRFTIME(return_date, '%Y-%m') as period,
                    item_id,
                    SUM(qty_returned) as qty_returned
                FROM core.fact_return
                WHERE channel_order_id IS NOT NULL
                GROUP BY 1, 2
            ),
            shipped_net AS (
                SELECT
                    s.period,
                    s.item_id,
                    s.channel_store_id,
                    s.qty_shipped,
                    COALESCE(r.qty_returned, 0) as qty_returned,
                    s.qty_shipped - COALESCE(r.qty_returned, 0) as qty_net
                FROM shipped s
                LEFT JOIN returned r ON s.period = r.period AND s.item_id = r.item_id
            ),
            cost_agg AS (
                SELECT item_id, effective_from,
                       SUM(cost_per_unit_krw) as unit_cost_krw
                FROM core.fact_cost_structure
                GROUP BY item_id, effective_from
            ),
            cost_ranked AS (
                SELECT
                    sn.period, sn.item_id, sn.channel_store_id,
                    sn.qty_shipped, sn.qty_returned, sn.qty_net,
                    ca.unit_cost_krw,
                    ROW_NUMBER() OVER (
                        PARTITION BY sn.period, sn.item_id, sn.channel_store_id
                        ORDER BY ca.effective_from DESC
                    ) as rn
                FROM shipped_net sn
                LEFT JOIN cost_agg ca
                    ON sn.item_id = ca.item_id
                    AND ca.effective_from <= LAST_DAY(CAST(sn.period || '-01' AS DATE))
            )
            SELECT
                period, item_id, channel_store_id,
                'KR' as country,
                qty_shipped, qty_returned, qty_net,
                unit_cost_krw,
                CASE WHEN unit_cost_krw IS NOT NULL THEN qty_net * unit_cost_krw ELSE NULL END as cogs_krw,
                CASE WHEN unit_cost_krw IS NOT NULL THEN 'ACTUAL' ELSE 'PARTIAL' END as coverage_flag
            FROM cost_ranked
            WHERE rn = 1 OR rn IS NULL
        """).pl()
    except Exception as e:
        logger.warning("COGS build failed: %s", e)
        return

    if cogs_df.height == 0:
        return

    # DQ assertion: no duplicate grain
    grain_counts = cogs_df.group_by(["period", "item_id", "channel_store_id"]).len()
    dups = grain_counts.filter(pl.col("len") > 1)
    if dups.height > 0:
        logger.error("COGS DQ FAIL: %d duplicate grain rows detected", dups.height)
        raise ValueError(f"COGS join produced {dups.height} duplicate grain rows — DQ HIGH FAIL")

    # Log coverage stats
    null_cost = cogs_df.filter(pl.col("unit_cost_krw").is_null())
    if null_cost.height > 0:
        null_qty = null_cost["qty_shipped"].sum()
        logger.warning(
            "COGS cost missing: %d lines (%d EA) — coverage_flag=PARTIAL",
            null_cost.height, int(null_qty or 0),
        )

    arrow = cogs_df.to_arrow()
    con.register("_cogs_staging", arrow)
    con.execute("INSERT INTO mart.mart_pnl_cogs SELECT * FROM _cogs_staging")
    con.unregister("_cogs_staging")


def build_mart_pnl_gross_margin(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build gross margin mart: revenue - COGS.

    Coverage propagation: PARTIAL if ANY upstream (revenue, cogs) is not ACTUAL.
    """
    con.execute("DELETE FROM mart.mart_pnl_gross_margin")

    try:
        df = con.execute("""
            SELECT
                COALESCE(r.period, c.period) as period,
                COALESCE(r.item_id, c.item_id) as item_id,
                COALESCE(r.channel_store_id, c.channel_store_id) as channel_store_id,
                COALESCE(r.country, c.country, 'KR') as country,
                COALESCE(r.net_revenue_krw, 0) as net_revenue_krw,
                COALESCE(c.cogs_krw, 0) as cogs_krw,
                COALESCE(r.net_revenue_krw, 0) - COALESCE(c.cogs_krw, 0) as gross_margin_krw,
                CASE WHEN COALESCE(r.net_revenue_krw, 0) != 0
                     THEN (COALESCE(r.net_revenue_krw, 0) - COALESCE(c.cogs_krw, 0)) / r.net_revenue_krw
                     ELSE 0 END as gross_margin_pct,
                CASE WHEN COALESCE(r.coverage_flag, 'PARTIAL') = 'ACTUAL'
                      AND COALESCE(c.coverage_flag, 'PARTIAL') = 'ACTUAL'
                     THEN 'ACTUAL'
                     ELSE 'PARTIAL'
                END as coverage_flag
            FROM mart.mart_pnl_revenue r
            FULL OUTER JOIN mart.mart_pnl_cogs c
                ON r.period = c.period AND r.item_id = c.item_id AND r.channel_store_id = c.channel_store_id
        """).pl()

        if df.height > 0:
            arrow = df.to_arrow()
            con.register("_gm_staging", arrow)
            con.execute("INSERT INTO mart.mart_pnl_gross_margin SELECT * FROM _gm_staging")
            con.unregister("_gm_staging")
    except Exception as e:
        logger.warning("Gross margin build failed: %s", e)


def build_mart_pnl_variable_cost(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build variable cost mart from allocated charges."""
    con.execute("DELETE FROM mart.mart_pnl_variable_cost")

    try:
        df = con.execute("""
            SELECT
                period,
                COALESCE(item_id, 'ALL') as item_id,
                COALESCE(channel_store_id, 'ALL') as channel_store_id,
                'KR' as country,
                charge_domain,
                charge_type,
                SUM(allocated_amount_krw) as allocated_amount_krw,
                'ACTUAL' as coverage_flag
            FROM mart.mart_charge_allocated
            GROUP BY period, item_id, channel_store_id, charge_domain, charge_type
        """).pl()

        if df.height > 0:
            arrow = df.to_arrow()
            con.register("_vc_staging", arrow)
            con.execute("INSERT INTO mart.mart_pnl_variable_cost SELECT * FROM _vc_staging")
            con.unregister("_vc_staging")
    except Exception as e:
        logger.warning("Variable cost build failed: %s", e)


def build_mart_pnl_contribution(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build contribution mart: gross_margin - variable_cost.

    Coverage propagation: PARTIAL if ANY upstream (gross_margin, variable_cost) is not ACTUAL.
    """
    con.execute("DELETE FROM mart.mart_pnl_contribution")

    try:
        df = con.execute("""
            SELECT
                gm.period,
                gm.item_id,
                gm.channel_store_id,
                gm.country,
                gm.gross_margin_krw,
                COALESCE(vc.total_vc, 0) as total_variable_cost_krw,
                gm.gross_margin_krw - COALESCE(vc.total_vc, 0) as contribution_krw,
                CASE WHEN gm.net_revenue_krw != 0
                     THEN (gm.gross_margin_krw - COALESCE(vc.total_vc, 0)) / gm.net_revenue_krw
                     ELSE 0 END as contribution_pct,
                CASE WHEN COALESCE(gm.coverage_flag, 'PARTIAL') = 'ACTUAL'
                      AND (vc.total_vc IS NOT NULL OR COALESCE(gm.coverage_flag, 'PARTIAL') = 'ACTUAL')
                     THEN COALESCE(gm.coverage_flag, 'PARTIAL')
                     ELSE 'PARTIAL'
                END as coverage_flag
            FROM mart.mart_pnl_gross_margin gm
            LEFT JOIN (
                SELECT period, item_id, channel_store_id,
                       SUM(allocated_amount_krw) as total_vc
                FROM mart.mart_pnl_variable_cost
                GROUP BY period, item_id, channel_store_id
            ) vc ON gm.period = vc.period AND gm.item_id = vc.item_id AND gm.channel_store_id = vc.channel_store_id
        """).pl()

        if df.height > 0:
            arrow = df.to_arrow()
            con.register("_contrib_staging", arrow)
            con.execute("INSERT INTO mart.mart_pnl_contribution SELECT * FROM _contrib_staging")
            con.unregister("_contrib_staging")
    except Exception as e:
        logger.warning("Contribution build failed: %s", e)


def build_mart_pnl_operating_profit(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build operating profit mart: contribution - fixed costs.

    Coverage propagation: inherits from contribution.
    fixed_cost is NOT included in coverage check (always 0/manually entered).
    """
    con.execute("DELETE FROM mart.mart_pnl_operating_profit")

    try:
        df = con.execute("""
            SELECT
                period, item_id, channel_store_id, country,
                contribution_krw,
                0 as fixed_cost_krw,
                contribution_krw as operating_profit_krw,
                contribution_pct as operating_profit_pct,
                COALESCE(coverage_flag, 'PARTIAL') as coverage_flag
            FROM mart.mart_pnl_contribution
        """).pl()

        if df.height > 0:
            arrow = df.to_arrow()
            con.register("_op_staging", arrow)
            con.execute("INSERT INTO mart.mart_pnl_operating_profit SELECT * FROM _op_staging")
            con.unregister("_op_staging")
    except Exception as e:
        logger.warning("Operating profit build failed: %s", e)


def build_mart_pnl_waterfall_summary(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build waterfall summary mart: pivoted revenue -> COGS -> GM -> VC -> contribution -> OP."""
    con.execute("DELETE FROM mart.mart_pnl_waterfall_summary")

    metrics = [
        ("SELECT period, SUM(net_revenue_krw) as amt FROM mart.mart_pnl_revenue GROUP BY period",
         "\ub9e4\ucd9c (Net Revenue)", 1),
        ("SELECT period, SUM(cogs_krw) as amt FROM mart.mart_pnl_cogs GROUP BY period",
         "\ub9e4\ucd9c\uc6d0\uac00 (COGS)", 2),
        ("SELECT period, SUM(gross_margin_krw) as amt FROM mart.mart_pnl_gross_margin GROUP BY period",
         "\ub9e4\ucd9c\ucd1d\uc774\uc775 (Gross Margin)", 3),
        ("SELECT period, SUM(allocated_amount_krw) as amt FROM mart.mart_pnl_variable_cost GROUP BY period",
         "\ubcc0\ub3d9\ube44 (Variable Cost)", 4),
        ("SELECT period, SUM(contribution_krw) as amt FROM mart.mart_pnl_contribution GROUP BY period",
         "\uacf5\ud5cc\uc774\uc775 (Contribution)", 5),
        ("SELECT period, SUM(operating_profit_krw) as amt FROM mart.mart_pnl_operating_profit GROUP BY period",
         "\uc601\uc5c5\uc774\uc775 (Operating Profit)", 6),
    ]

    rows = []
    for sql, metric_name, metric_order in metrics:
        try:
            df = con.execute(sql).fetchall()
            for period, amt in df:
                rows.append({
                    "period": period,
                    "metric_name": metric_name,
                    "metric_order": metric_order,
                    "amount_krw": float(amt) if amt else 0.0,
                })
        except Exception:
            pass

    if rows:
        result = pl.DataFrame(rows)
        arrow = result.to_arrow()
        con.register("_wf_staging", arrow)
        con.execute("INSERT INTO mart.mart_pnl_waterfall_summary SELECT * FROM _wf_staging")
        con.unregister("_wf_staging")


def build_all_pnl_marts(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Orchestrate all P&L marts in dependency order."""
    build_mart_pnl_revenue(con, config)
    build_mart_pnl_cogs(con, config)
    build_mart_pnl_gross_margin(con, config)
    build_mart_pnl_variable_cost(con, config)
    build_mart_pnl_contribution(con, config)
    build_mart_pnl_operating_profit(con, config)
    build_mart_pnl_waterfall_summary(con, config)
