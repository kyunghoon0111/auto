"""Coverage policy enforcement and reporting.

Tracks data completeness per domain per period.
Never silently assumes missing cost = 0.
"""
import duckdb
import polars as pl

from src.config import AppConfig
from src.period_close import is_period_closed

def _build_domain_queries(config: AppConfig) -> dict:
    """charge_policy.yaml 기반으로 도메인 쿼리를 동적 생성.

    비-charge 테이블을 직접 조회하는 정적 도메인(fx_rate, revenue_settlement,
    cost_structure)은 고정 쿼리를 사용하고, 나머지 charge 기반 도메인은
    charge_policy.yaml의 charge_domain 분류에서 자동 생성한다.
    새 charge_type을 charge_policy.yaml에 추가하면 커버리지 체크에 자동 반영된다.
    """
    queries = {
        "fx_rate": {
            "query": "SELECT period, COUNT(*) as cnt FROM core.fact_exchange_rate GROUP BY period",
            "min_rows": 1,
        },
        "revenue_settlement": {
            "query": "SELECT period, COUNT(*) as cnt FROM core.fact_settlement GROUP BY period",
            "min_rows": 1,
        },
        "cost_structure": {
            "query": "SELECT 'ALL' as period, COUNT(*) as cnt FROM core.fact_cost_structure",
            "min_rows": 1,
        },
    }
    domain_types = config.get_charge_types_by_domain()
    for domain, charge_types in domain_types.items():
        if domain not in queries:
            types_sql = ", ".join(f"'{ct}'" for ct in sorted(charge_types))
            queries[domain] = {
                "query": (
                    "SELECT period, COUNT(*) as cnt"
                    " FROM core.fact_charge_actual"
                    f" WHERE charge_type IN ({types_sql})"
                    " GROUP BY period"
                ),
                "min_rows": 1,
            }
    return queries


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
    domain_queries = _build_domain_queries(config)

    for domain_name, domain_cfg in domains.items():
        dq = domain_queries.get(domain_name)
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
    domain_queries = _build_domain_queries(config)

    for domain_name, requirement in close_enforcement.items():
        if requirement != "REQUIRED":
            continue

        dq = domain_queries.get(domain_name)
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
