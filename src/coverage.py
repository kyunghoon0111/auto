"""Coverage policy enforcement and reporting.

Tracks data completeness per domain per period.
Never silently assumes missing cost = 0.
"""
import duckdb
import polars as pl

from src.config import AppConfig
from src.period_close import is_period_closed

# Map coverage domains to their source tables and filters
DOMAIN_QUERIES = {
    "fx_rate": {
        "query": "SELECT period, COUNT(*) as cnt FROM core.fact_exchange_rate GROUP BY period",
        "min_rows": 1,
    },
    "revenue_settlement": {
        "query": "SELECT period, COUNT(*) as cnt FROM core.fact_settlement GROUP BY period",
        "min_rows": 1,
    },
    "logistics_transport": {
        "query": """
            SELECT period, COUNT(*) as cnt
            FROM core.fact_charge_actual
            WHERE charge_type IN (
                'LAST_MILE_PARCEL','DOMESTIC_TRUCKING','FREIGHT_INTL_SEA','FREIGHT_INTL_AIR',
                'PORT_TERMINAL_FEE','FORWARDER_FEE','CARGO_INSURANCE'
            )
            GROUP BY period
        """,
        "min_rows": 1,
    },
    "customs": {
        "query": """
            SELECT period, COUNT(*) as cnt
            FROM core.fact_charge_actual
            WHERE charge_type IN ('CUSTOMS_DUTY','CUSTOMS_VAT','BROKER_FEE')
            GROUP BY period
        """,
        "min_rows": 1,
    },
    "3pl_billing": {
        "query": """
            SELECT period, COUNT(*) as cnt
            FROM core.fact_charge_actual
            WHERE charge_type IN (
                '3PL_STORAGE_FEE','3PL_PICK_PACK_FEE','3PL_HANDLING_FEE',
                '3PL_RETURN_PROCESSING_FEE','DISPOSAL_FEE'
            )
            GROUP BY period
        """,
        "min_rows": 1,
    },
    "cost_structure": {
        "query": "SELECT 'ALL' as period, COUNT(*) as cnt FROM core.fact_cost_structure",
        "min_rows": 1,
    },
}


def compute_coverage(con: duckdb.DuckDBPyConnection, config: AppConfig) -> pl.DataFrame:
    """Compute coverage for all domains across all periods.

    Writes results to mart.mart_coverage_period.
    Returns the coverage DataFrame.
    """
    # Get all known periods from various fact tables
    periods = set()
    for table in ["core.fact_order", "core.fact_shipment", "core.fact_charge_actual", "core.fact_settlement"]:
        try:
            if "charge" in table or "settlement" in table:
                col = "period"
            elif "order" in table:
                col = "STRFTIME(order_date, '%Y-%m') as period"
            elif "shipment" in table:
                col = "STRFTIME(ship_date, '%Y-%m') as period"
            else:
                continue
            result = con.execute(f"SELECT DISTINCT {col} FROM {table}").fetchall()
            periods.update(r[0] for r in result if r[0])
        except Exception:
            pass

    if not periods:
        periods = {"ALL"}

    rows = []
    domains = config.coverage_policy.get("domains", {})

    for domain_name, domain_cfg in domains.items():
        dq = DOMAIN_QUERIES.get(domain_name)
        if dq is None:
            continue

        try:
            domain_data = con.execute(dq["query"]).fetchall()
            domain_periods = {r[0]: r[1] for r in domain_data}
        except Exception:
            domain_periods = {}

        for period in sorted(periods):
            is_closed = is_period_closed(con, period)
            cnt = domain_periods.get(period, 0)

            if cnt >= dq["min_rows"]:
                coverage_rate = 1.0
                included = cnt
                missing = 0
            else:
                coverage_rate = 0.0
                included = cnt
                missing = dq["min_rows"] - cnt

            # Determine severity
            is_required = config.is_domain_required(domain_name, is_closed)
            if coverage_rate < 1.0 and is_required:
                severity = "CRITICAL"
            elif coverage_rate < 1.0:
                severity = "INFO"
            else:
                severity = "OK"

            rows.append({
                "period": period,
                "domain": domain_name,
                "coverage_rate": coverage_rate,
                "included_rows": included,
                "missing_rows": missing,
                "severity": severity,
                "is_closed_period": is_closed,
            })

    coverage_df = pl.DataFrame(rows) if rows else pl.DataFrame({
        "period": [], "domain": [], "coverage_rate": [],
        "included_rows": [], "missing_rows": [], "severity": [],
        "is_closed_period": [],
    })

    # Write to mart
    con.execute("DELETE FROM mart.mart_coverage_period")
    if coverage_df.height > 0:
        arrow = coverage_df.to_arrow()
        con.register("_cov_staging", arrow)
        con.execute("INSERT INTO mart.mart_coverage_period SELECT * FROM _cov_staging")
        con.unregister("_cov_staging")

    return coverage_df


def enforce_closed_period_coverage(
    con: duckdb.DuckDBPyConnection, config: AppConfig, period: str
) -> list[str]:
    """Check coverage requirements for a closed period.

    Returns list of error messages for REQUIRED domains that lack coverage.
    """
    errors = []
    close_enforcement = config.coverage_policy.get("close_period_enforcement", {})

    for domain_name, requirement in close_enforcement.items():
        if requirement != "REQUIRED":
            continue

        dq = DOMAIN_QUERIES.get(domain_name)
        if dq is None:
            continue

        try:
            domain_data = con.execute(dq["query"]).fetchall()
            domain_periods = {r[0]: r[1] for r in domain_data}
        except Exception:
            domain_periods = {}

        cnt = domain_periods.get(period, 0)
        if cnt < dq.get("min_rows", 1):
            errors.append(
                f"REQUIRED domain '{domain_name}' has insufficient data for closed period '{period}' "
                f"(found {cnt} rows, need >= {dq['min_rows']})"
            )

    return errors
