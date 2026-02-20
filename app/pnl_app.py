"""P&L Dashboard - Streamlit (port 8502). UI in Korean.

Enhancements (Phase 1-4):
- Schema fail-fast: required columns checked at startup -> st.error + st.stop
- Coverage-aware aggregation: known_sum (ACTUAL only) + total_sum_min (all rows)
- Revenue decomposition: gross_sales, discounts, refunds, discount_rate%, FX coverage
- Operating profit tab (new)
- Variable cost breakdown: charge_domain + cost_stage bar charts, unit costs
- Profitability ranking tab (new): TOP/BOTTOM 10, PARTIAL flagged
- Tie-out enhancement: delta/abs/ratio
- Coverage traffic lights: 6 domains
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="손익 분석", layout="wide", page_icon="💰")

DB_PATH = Path(__file__).parent.parent / "data" / "scm.duckdb"

# ── Korean label mappings ──
CHARGE_DOMAIN_KR = {
    "logistics_transport": "운송",
    "customs": "통관",
    "3pl_billing": "3PL",
    "platform_fee": "플랫폼",
    "marketing": "마케팅",
}

COST_STAGE_KR = {
    "inbound_landed": "수입/입고",
    "storage": "보관",
    "outbound": "출고",
    "returns": "반품",
    "period": "기간",
}


@st.cache_resource
def get_connection():
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)


def query_df(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()


def format_krw(value) -> str:
    """Format KRW amount to readable Korean units."""
    if value is None or pd.isna(value):
        return "—"
    value = float(value)
    if abs(value) >= 1e8:
        return f"{value/1e8:,.1f}억원"
    elif abs(value) >= 1e4:
        return f"{value/1e4:,.0f}만원"
    return f"{value:,.0f}원"


def format_pct(value) -> str:
    """Format percentage."""
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value)*100:.1f}%"


# ── Schema helpers ──

def _has_column(con, schema: str, table: str, column: str) -> bool:
    """Check if a column exists in a table via information_schema."""
    try:
        return con.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            [schema, table, column],
        ).fetchone()[0] > 0
    except Exception:
        return False


def _table_has_coverage_flag(con, full_table: str) -> bool:
    """Check if table has coverage_flag column. Cache per session."""
    parts = full_table.split(".")
    if len(parts) == 2:
        return _has_column(con, parts[0], parts[1], "coverage_flag")
    return False


def _coverage_agg_sql(metric_col: str, table: str, period_filter: str = "") -> str:
    """Generate SQL for coverage-aware aggregation.

    Only use on tables that HAVE coverage_flag column.
    coverage_flag IS NULL is treated as PARTIAL (fail-safe for migration gaps).
    """
    where = f"WHERE {period_filter}" if period_filter else ""
    return f"""
        SELECT
            SUM(CASE WHEN coverage_flag = 'ACTUAL' THEN {metric_col} END) AS known_sum,
            SUM(COALESCE({metric_col}, 0)) AS total_sum_min,
            COUNT(*) AS total_count,
            SUM(CASE WHEN coverage_flag IS NULL OR coverage_flag <> 'ACTUAL' THEN 1 ELSE 0 END) AS partial_count,
            SUM(CASE WHEN coverage_flag = 'ACTUAL' THEN 1 ELSE 0 END) AS actual_count
        FROM {table} {where}
    """


def _show_coverage_badge(con, table: str, metric_col: str, period_filter: str = ""):
    """Show coverage badge for a metric. Returns (known_sum, total_sum_min) or (total, total)."""
    if _table_has_coverage_flag(con, table):
        sql = _coverage_agg_sql(metric_col, table, period_filter)
        row = query_df(con, sql)
        if row.empty:
            return 0.0, 0.0
        r = row.iloc[0]
        known = r.get("known_sum", 0) or 0
        total = r.get("total_sum_min", 0) or 0
        actual_n = int(r.get("actual_count", 0) or 0)
        partial_n = int(r.get("partial_count", 0) or 0)
        if partial_n > 0:
            st.caption(f"ACTUAL {actual_n}건 | 누락/미확인 {partial_n}건")
        else:
            st.caption(f"전체 {actual_n}건 ACTUAL")
        return float(known), float(total)
    else:
        # Table without coverage_flag — plain SUM
        sql = f"SELECT SUM(COALESCE({metric_col}, 0)) AS total FROM {table}"
        if period_filter:
            sql += f" WHERE {period_filter}"
        row = query_df(con, sql)
        total = float(row.iloc[0]["total"]) if not row.empty else 0.0
        st.caption("coverage 미지원")
        return total, total


def _period_filter_widget(con, table: str, key_prefix: str):
    """Show period selector and return (selected_period, sql_filter_expr)."""
    try:
        periods = con.execute(f"SELECT DISTINCT period FROM {table} ORDER BY period DESC").fetchdf()
        period_list = periods["period"].tolist() if not periods.empty else []
    except Exception:
        period_list = []

    if not period_list:
        return None, ""

    if len(period_list) > 1:
        options = ["전체"] + period_list
        selected = st.selectbox("기간 선택", options, key=f"{key_prefix}_period")
    else:
        selected = period_list[0]

    if selected == "전체":
        return "전체", ""
    return selected, f"period = '{selected}'"


def main():
    st.title("💰 손익 분석 대시보드")

    con = get_connection()
    if con is None:
        st.error("데이터베이스가 없습니다. `python run.py --init` 을 먼저 실행해주세요.")
        st.stop()

    # Schema fail-fast: check critical tables exist
    required_tables = {
        "mart.mart_pnl_revenue": ["period", "net_revenue_krw"],
        "mart.mart_pnl_cogs": ["period", "cogs_krw"],
        "mart.mart_pnl_gross_margin": ["period", "gross_margin_krw"],
        "mart.mart_pnl_variable_cost": ["period", "allocated_amount_krw"],
        "mart.mart_pnl_contribution": ["period", "contribution_krw"],
        "mart.mart_pnl_operating_profit": ["period", "operating_profit_krw"],
    }
    for tbl, cols in required_tables.items():
        schema_name, tbl_name = tbl.split(".")
        for col in cols:
            if not _has_column(con, schema_name, tbl_name, col):
                st.error(f"필수 컬럼 누락: {tbl}.{col} — 파이프라인을 먼저 실행해주세요.")
                st.stop()

    tabs = st.tabs([
        "손익 요약",           # 0: Waterfall
        "매출",               # 1: Revenue decomposition
        "매출원가",            # 2: COGS
        "매출총이익",          # 3: Gross Margin
        "변동비",             # 4: Variable Cost breakdown
        "공헌이익",            # 5: Contribution
        "영업이익",            # 6: Operating Profit (NEW)
        "수익성 순위",         # 7: Profitability Ranking (NEW)
        "비용 배분",           # 8: Cost Allocation
        "정산 검증",           # 9: Settlement + Tie-out
        "커버리지",            # 10: Coverage Traffic Lights
    ])

    # ═══════════════════════════════════════════════════════════════
    # Tab 0: 손익 요약 (Waterfall)
    # ═══════════════════════════════════════════════════════════════
    with tabs[0]:
        st.header("손익 워터폴 요약")
        df = query_df(con, "SELECT * FROM mart.mart_pnl_waterfall_summary ORDER BY metric_order")
        if df.empty:
            st.info("손익 데이터가 없습니다. 데이터를 수집한 후 파이프라인을 실행해주세요.")
        else:
            periods = sorted(df["period"].unique().tolist())
            if periods:
                selected_period = st.selectbox("기간 선택", periods, key="wf_period")
                period_df = df[df["period"] == selected_period]

                cols = st.columns(min(len(period_df), 6))
                for i, (_, row) in enumerate(period_df.iterrows()):
                    with cols[i % len(cols)]:
                        st.metric(row["metric_name"], format_krw(row["amount_krw"]))

                st.bar_chart(period_df.set_index("metric_name")["amount_krw"])

            st.caption("coverage 미지원 (요약 테이블은 coverage_flag 없음 — 개별 탭 참조)")

    # ═══════════════════════════════════════════════════════════════
    # Tab 1: 매출 (Revenue Decomposition)
    # ═══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.header("매출 상세")
        st.caption("기간 기준: 정산 기간(period) | 판매국가(country) 기준")

        sel_period, pfilter = _period_filter_widget(con, "mart.mart_pnl_revenue", "rev")

        where = f"WHERE {pfilter}" if pfilter else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_revenue {where}")

        if df.empty:
            st.info("매출 데이터가 없습니다.")
        else:
            # Country filter if column exists
            if "country" in df.columns:
                countries = sorted(df["country"].dropna().unique().tolist())
                if len(countries) > 1:
                    sel_country = st.selectbox("판매국가 필터", ["전체"] + countries, key="rev_country")
                    if sel_country != "전체":
                        df = df[df["country"] == sel_country]

            # Revenue decomposition KPIs
            st.subheader("매출 구성")
            has_gross = "gross_sales_krw" in df.columns
            has_disc = "discounts_krw" in df.columns
            has_ref = "refunds_krw" in df.columns
            has_net = "net_revenue_krw" in df.columns

            kpi_cols = st.columns(5)
            if has_gross:
                gross = df["gross_sales_krw"].sum()
                kpi_cols[0].metric("총매출(Gross)", format_krw(gross))
            if has_disc:
                disc = df["discounts_krw"].sum()
                kpi_cols[1].metric("할인", format_krw(disc))
            if has_ref:
                ref = df["refunds_krw"].sum()
                kpi_cols[2].metric("환불", format_krw(ref))
            if has_net:
                net = df["net_revenue_krw"].sum()
                kpi_cols[3].metric("순매출(Net)", format_krw(net))
                if has_gross and gross != 0:
                    disc_rate = abs(disc) / abs(gross) if has_disc else 0
                    kpi_cols[4].metric("할인율", f"{disc_rate*100:.1f}%")

            # Coverage distribution
            if "coverage_flag" in df.columns:
                actual_n = len(df[df["coverage_flag"] == "ACTUAL"])
                partial_n = len(df) - actual_n
                if partial_n > 0:
                    st.warning(f"ACTUAL {actual_n}건 / 누락(PARTIAL) {partial_n}건 — FX 환율 누락 가능")
                else:
                    st.success(f"전체 {actual_n}건 ACTUAL")

            # Channel net payout
            if has_net and "channel_store_id" in df.columns:
                st.subheader("채널/스토어별 순매출")
                by_store = df.groupby("channel_store_id")["net_revenue_krw"].sum().reset_index()
                by_store = by_store.sort_values("net_revenue_krw", ascending=False)
                st.bar_chart(by_store.set_index("channel_store_id"))

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 2: 매출원가 (COGS)
    # ═══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.header("매출원가 (COGS)")
        st.caption("판매출고(sales) 기준 | channel_order_id IS NOT NULL")

        sel_period_c, pfilter_c = _period_filter_widget(con, "mart.mart_pnl_cogs", "cogs")

        where_c = f"WHERE {pfilter_c}" if pfilter_c else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_cogs {where_c}")

        if df.empty:
            st.info("매출원가 데이터가 없습니다.")
        else:
            # Coverage-aware KPIs
            known_cogs, total_cogs = 0.0, 0.0
            if _table_has_coverage_flag(con, "mart.mart_pnl_cogs"):
                agg_sql = _coverage_agg_sql("cogs_krw", "mart.mart_pnl_cogs", pfilter_c)
                agg = query_df(con, agg_sql)
                if not agg.empty:
                    r = agg.iloc[0]
                    known_cogs = float(r.get("known_sum", 0) or 0)
                    total_cogs = float(r.get("total_sum_min", 0) or 0)
                    actual_n = int(r.get("actual_count", 0) or 0)
                    partial_n = int(r.get("partial_count", 0) or 0)

            c1, c2, c3 = st.columns(3)
            c1.metric("ACTUAL 매출원가", format_krw(known_cogs))
            c2.metric("전체 최소값 (누락=0 가정)", format_krw(total_cogs))
            total_net_qty = df["qty_net"].sum() if "qty_net" in df.columns else 0
            c3.metric("순 판매수량", f"{total_net_qty:,.0f}")

            if "coverage_flag" in df.columns:
                partial_n = len(df[df["coverage_flag"] != "ACTUAL"])
                if partial_n > 0:
                    st.warning(f"원가 누락 {partial_n}건 — 원가 마스터 미등록 상품")

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 3: 매출총이익 (Gross Margin)
    # ═══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.header("매출총이익")
        st.caption("매출총이익 = 순매출 - 매출원가")

        sel_period_gm, pfilter_gm = _period_filter_widget(con, "mart.mart_pnl_gross_margin", "gm")
        where_gm = f"WHERE {pfilter_gm}" if pfilter_gm else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_gross_margin {where_gm}")

        if df.empty:
            st.info("매출총이익 데이터가 없습니다.")
        else:
            # Coverage-aware
            known_gm, total_gm = 0.0, 0.0
            if _table_has_coverage_flag(con, "mart.mart_pnl_gross_margin"):
                agg = query_df(con, _coverage_agg_sql("gross_margin_krw", "mart.mart_pnl_gross_margin", pfilter_gm))
                if not agg.empty:
                    r = agg.iloc[0]
                    known_gm = float(r.get("known_sum", 0) or 0)
                    total_gm = float(r.get("total_sum_min", 0) or 0)

            # Margin pct (only from ACTUAL rows)
            if _table_has_coverage_flag(con, "mart.mart_pnl_gross_margin"):
                act_sql = f"SELECT AVG(gross_margin_pct) as avg_pct FROM mart.mart_pnl_gross_margin WHERE coverage_flag = 'ACTUAL'"
                if pfilter_gm:
                    act_sql += f" AND {pfilter_gm}"
            else:
                act_sql = f"SELECT AVG(gross_margin_pct) as avg_pct FROM mart.mart_pnl_gross_margin"
                if pfilter_gm:
                    act_sql += f" WHERE {pfilter_gm}"
            avg_pct_df = query_df(con, act_sql)
            avg_pct = avg_pct_df.iloc[0]["avg_pct"] if not avg_pct_df.empty and avg_pct_df.iloc[0]["avg_pct"] is not None else None

            c1, c2, c3 = st.columns(3)
            c1.metric("ACTUAL 매출총이익", format_krw(known_gm))
            c2.metric("전체 최소값", format_krw(total_gm))
            c3.metric("평균 마진율 (ACTUAL)", format_pct(avg_pct))

            _show_coverage_badge(con, "mart.mart_pnl_gross_margin", "gross_margin_krw", pfilter_gm)

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 4: 변동비 (Variable Cost Breakdown)
    # ═══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.header("변동비 상세")
        st.caption("서비스 기간(period) 기준 | 판매출고(sales) 기준")

        sel_period_vc, pfilter_vc = _period_filter_widget(con, "mart.mart_pnl_variable_cost", "vc")
        where_vc = f"WHERE {pfilter_vc}" if pfilter_vc else ""

        # Variable cost from mart_pnl_variable_cost (has charge_domain)
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_variable_cost {where_vc}")

        if df.empty:
            st.info("변동비 데이터가 없습니다.")
        else:
            total_vc = df["allocated_amount_krw"].sum()
            st.metric("총 변동비", format_krw(total_vc))
            _show_coverage_badge(con, "mart.mart_pnl_variable_cost", "allocated_amount_krw", pfilter_vc)

            # Charge domain breakdown
            left_col, right_col = st.columns(2)
            with left_col:
                st.subheader("비용 도메인별")
                if "charge_domain" in df.columns:
                    by_domain = df.groupby("charge_domain")["allocated_amount_krw"].sum().reset_index()
                    # Map Korean labels
                    by_domain["도메인"] = by_domain["charge_domain"].map(
                        lambda x: CHARGE_DOMAIN_KR.get(x, x)
                    )
                    # Warn unmapped
                    unmapped = set(by_domain["charge_domain"]) - set(CHARGE_DOMAIN_KR.keys())
                    if unmapped:
                        st.warning(f"미매핑 도메인: {', '.join(unmapped)}")
                    by_domain = by_domain.sort_values("allocated_amount_krw", ascending=False)
                    st.bar_chart(by_domain.set_index("도메인")["allocated_amount_krw"])

            with right_col:
                # Cost stage breakdown (from mart_charge_allocated which has cost_stage)
                st.subheader("비용 단계별")
                stage_sql = f"SELECT cost_stage, SUM(allocated_amount_krw) as amount FROM mart.mart_charge_allocated"
                if pfilter_vc:
                    stage_sql += f" WHERE {pfilter_vc}"
                stage_sql += " GROUP BY cost_stage ORDER BY amount DESC"
                stage_df = query_df(con, stage_sql)
                if not stage_df.empty:
                    stage_df["단계"] = stage_df["cost_stage"].map(
                        lambda x: COST_STAGE_KR.get(x, x)
                    )
                    unmapped_s = set(stage_df["cost_stage"]) - set(COST_STAGE_KR.keys())
                    if unmapped_s:
                        st.warning(f"미매핑 비용단계: {', '.join(unmapped_s)}")
                    st.bar_chart(stage_df.set_index("단계")["amount"])

            # Logistics cost ratio
            st.subheader("물류비 비율")
            logistics_domains = {"logistics_transport", "3pl_billing"}
            if "charge_domain" in df.columns:
                logistics_amt = df[df["charge_domain"].isin(logistics_domains)]["allocated_amount_krw"].sum()

                # Get revenue for ratio
                rev_sql = "SELECT SUM(COALESCE(net_revenue_krw, 0)) as rev FROM mart.mart_pnl_revenue"
                if pfilter_vc:
                    rev_sql += f" WHERE {pfilter_vc}"
                rev_df = query_df(con, rev_sql)
                rev_total = float(rev_df.iloc[0]["rev"]) if not rev_df.empty and rev_df.iloc[0]["rev"] else 0

                lc1, lc2 = st.columns(2)
                lc1.metric("물류비 합계", format_krw(logistics_amt))
                if rev_total > 0:
                    lc2.metric("물류비율 (물류비/순매출)", f"{logistics_amt / rev_total * 100:.1f}%")
                else:
                    lc2.metric("물류비율", "매출 없음")

            # Unit costs (with gating)
            st.subheader("단위 비용")
            st.caption("0-7: Weight/CBM 커버리지 미충족 시 비표시")

            # Order-based unit cost
            order_count_sql = "SELECT COUNT(DISTINCT channel_order_id) as cnt FROM core.fact_shipment WHERE channel_order_id IS NOT NULL"
            if pfilter_vc:
                order_count_sql += f" AND STRFTIME(ship_date, '%Y-%m') = '{sel_period_vc}'" if sel_period_vc and sel_period_vc != "전체" else ""
            oc_df = query_df(con, order_count_sql)
            order_count = int(oc_df.iloc[0]["cnt"]) if not oc_df.empty and oc_df.iloc[0]["cnt"] else 0

            uc_cols = st.columns(4)
            if order_count > 0:
                uc_cols[0].metric("건당 변동비", format_krw(total_vc / order_count))
            else:
                uc_cols[0].metric("건당 변동비", "주문 없음")

            # EA-based
            ea_sql = "SELECT SUM(qty_shipped) as qty FROM core.fact_shipment WHERE channel_order_id IS NOT NULL"
            if sel_period_vc and sel_period_vc != "전체":
                ea_sql += f" AND STRFTIME(ship_date, '%Y-%m') = '{sel_period_vc}'"
            ea_df = query_df(con, ea_sql)
            ea_count = float(ea_df.iloc[0]["qty"]) if not ea_df.empty and ea_df.iloc[0]["qty"] else 0
            if ea_count > 0:
                uc_cols[1].metric("EA당 변동비", format_krw(total_vc / ea_count))
            else:
                uc_cols[1].metric("EA당 변동비", "수량 없음")

            # Weight/CBM — gated by coverage (check if columns exist)
            has_weight = _has_column(con, "core", "fact_shipment", "weight_kg")
            has_cbm = _has_column(con, "core", "fact_shipment", "cbm")

            if has_weight:
                wt_sql = "SELECT SUM(weight_kg) as wt, COUNT(*) as total, SUM(CASE WHEN weight_kg IS NULL OR weight_kg = 0 THEN 1 ELSE 0 END) as missing FROM core.fact_shipment WHERE channel_order_id IS NOT NULL"
                if sel_period_vc and sel_period_vc != "전체":
                    wt_sql += f" AND STRFTIME(ship_date, '%Y-%m') = '{sel_period_vc}'"
                wt_df = query_df(con, wt_sql)
                if not wt_df.empty:
                    total_wt = float(wt_df.iloc[0]["wt"] or 0)
                    miss_wt = int(wt_df.iloc[0]["missing"] or 0)
                    tot_rows = int(wt_df.iloc[0]["total"] or 0)
                    wt_cov = (tot_rows - miss_wt) / tot_rows * 100 if tot_rows > 0 else 0
                    if wt_cov >= 80 and total_wt > 0:
                        uc_cols[2].metric("kg당 변동비", format_krw(total_vc / total_wt))
                        uc_cols[2].caption(f"weight 커버리지: {wt_cov:.0f}%")
                    else:
                        uc_cols[2].metric("kg당 변동비", "커버리지 부족")
                        uc_cols[2].caption(f"weight 커버리지: {wt_cov:.0f}% (80% 미만)")
            else:
                uc_cols[2].metric("kg당 변동비", "컬럼 없음")

            if has_cbm:
                cbm_sql = "SELECT SUM(cbm) as vol, COUNT(*) as total, SUM(CASE WHEN cbm IS NULL OR cbm = 0 THEN 1 ELSE 0 END) as missing FROM core.fact_shipment WHERE channel_order_id IS NOT NULL"
                if sel_period_vc and sel_period_vc != "전체":
                    cbm_sql += f" AND STRFTIME(ship_date, '%Y-%m') = '{sel_period_vc}'"
                cbm_df = query_df(con, cbm_sql)
                if not cbm_df.empty:
                    total_cbm = float(cbm_df.iloc[0]["vol"] or 0)
                    miss_cbm = int(cbm_df.iloc[0]["missing"] or 0)
                    tot_rows_c = int(cbm_df.iloc[0]["total"] or 0)
                    cbm_cov = (tot_rows_c - miss_cbm) / tot_rows_c * 100 if tot_rows_c > 0 else 0
                    if cbm_cov >= 80 and total_cbm > 0:
                        uc_cols[3].metric("CBM당 변동비", format_krw(total_vc / total_cbm))
                        uc_cols[3].caption(f"CBM 커버리지: {cbm_cov:.0f}%")
                    else:
                        uc_cols[3].metric("CBM당 변동비", "커버리지 부족")
                        uc_cols[3].caption(f"CBM 커버리지: {cbm_cov:.0f}% (80% 미만)")
            else:
                uc_cols[3].metric("CBM당 변동비", "컬럼 없음")

            with st.expander("변동비 상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 5: 공헌이익 (Contribution)
    # ═══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.header("공헌이익")
        st.caption("공헌이익 = 매출총이익 - 변동비")

        sel_period_ct, pfilter_ct = _period_filter_widget(con, "mart.mart_pnl_contribution", "contrib")
        where_ct = f"WHERE {pfilter_ct}" if pfilter_ct else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_contribution {where_ct}")

        if df.empty:
            st.info("공헌이익 데이터가 없습니다.")
        else:
            known_ct, total_ct = 0.0, 0.0
            if _table_has_coverage_flag(con, "mart.mart_pnl_contribution"):
                agg = query_df(con, _coverage_agg_sql("contribution_krw", "mart.mart_pnl_contribution", pfilter_ct))
                if not agg.empty:
                    r = agg.iloc[0]
                    known_ct = float(r.get("known_sum", 0) or 0)
                    total_ct = float(r.get("total_sum_min", 0) or 0)
            else:
                total_ct = df["contribution_krw"].sum()
                known_ct = total_ct

            # ACTUAL-only avg pct
            if _table_has_coverage_flag(con, "mart.mart_pnl_contribution"):
                pct_sql = f"SELECT AVG(contribution_pct) as avg_pct FROM mart.mart_pnl_contribution WHERE coverage_flag = 'ACTUAL'"
                if pfilter_ct:
                    pct_sql += f" AND {pfilter_ct}"
            else:
                pct_sql = f"SELECT AVG(contribution_pct) as avg_pct FROM mart.mart_pnl_contribution"
                if pfilter_ct:
                    pct_sql += f" WHERE {pfilter_ct}"
            pct_df = query_df(con, pct_sql)
            avg_pct = pct_df.iloc[0]["avg_pct"] if not pct_df.empty and pct_df.iloc[0]["avg_pct"] is not None else None

            c1, c2, c3 = st.columns(3)
            c1.metric("ACTUAL 공헌이익", format_krw(known_ct))
            c2.metric("전체 최소값", format_krw(total_ct))
            c3.metric("평균 공헌이익률 (ACTUAL)", format_pct(avg_pct))

            _show_coverage_badge(con, "mart.mart_pnl_contribution", "contribution_krw", pfilter_ct)

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 6: 영업이익 (Operating Profit) — NEW
    # ═══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.header("영업이익")
        st.caption("영업이익 = 공헌이익 - 고정비")

        sel_period_op, pfilter_op = _period_filter_widget(con, "mart.mart_pnl_operating_profit", "op")
        where_op = f"WHERE {pfilter_op}" if pfilter_op else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_operating_profit {where_op}")

        if df.empty:
            st.info("영업이익 데이터가 없습니다.")
        else:
            # Check if fixed_cost_krw is all zero
            if "fixed_cost_krw" in df.columns:
                total_fixed = df["fixed_cost_krw"].sum()
                if total_fixed == 0:
                    st.info("고정비 미입력 — 현재 영업이익 = 공헌이익")

            known_op, total_op = 0.0, 0.0
            if _table_has_coverage_flag(con, "mart.mart_pnl_operating_profit"):
                agg = query_df(con, _coverage_agg_sql("operating_profit_krw", "mart.mart_pnl_operating_profit", pfilter_op))
                if not agg.empty:
                    r = agg.iloc[0]
                    known_op = float(r.get("known_sum", 0) or 0)
                    total_op = float(r.get("total_sum_min", 0) or 0)
                    partial_n = int(r.get("partial_count", 0) or 0)
            else:
                total_op = df["operating_profit_krw"].sum()
                known_op = total_op
                partial_n = 0

            # Operating margin (ACTUAL only)
            if _table_has_coverage_flag(con, "mart.mart_pnl_operating_profit"):
                pct_sql = f"SELECT AVG(operating_profit_pct) as avg_pct FROM mart.mart_pnl_operating_profit WHERE coverage_flag = 'ACTUAL'"
                if pfilter_op:
                    pct_sql += f" AND {pfilter_op}"
            else:
                pct_sql = f"SELECT AVG(operating_profit_pct) as avg_pct FROM mart.mart_pnl_operating_profit"
                if pfilter_op:
                    pct_sql += f" WHERE {pfilter_op}"
            pct_df = query_df(con, pct_sql)
            avg_pct = pct_df.iloc[0]["avg_pct"] if not pct_df.empty and pct_df.iloc[0]["avg_pct"] is not None else None

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("ACTUAL 영업이익", format_krw(known_op))
            c2.metric("전체 최소값", format_krw(total_op))
            c3.metric("영업이익률 (ACTUAL)", format_pct(avg_pct))
            c4.metric("고정비", format_krw(total_fixed) if "fixed_cost_krw" in df.columns else "—")

            if partial_n > 0:
                st.warning(f"불완전 손익: {partial_n}건 PARTIAL — 원가/환율 누락 전파")

            _show_coverage_badge(con, "mart.mart_pnl_operating_profit", "operating_profit_krw", pfilter_op)

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 7: 수익성 순위 (Profitability Ranking) — NEW
    # ═══════════════════════════════════════════════════════════════
    with tabs[7]:
        st.header("수익성 순위")
        st.caption("공헌이익 기준 TOP/BOTTOM 10 | PARTIAL 행 = (추정)")

        sel_period_rk, pfilter_rk = _period_filter_widget(con, "mart.mart_pnl_contribution", "rank")
        where_rk = f"WHERE {pfilter_rk}" if pfilter_rk else ""
        df = query_df(con, f"SELECT * FROM mart.mart_pnl_contribution {where_rk}")

        if df.empty:
            st.info("공헌이익 데이터가 없습니다.")
        else:
            has_cf = "coverage_flag" in df.columns

            # Item ranking
            st.subheader("상품별 수익성")
            item_agg = df.groupby("item_id").agg({
                "contribution_krw": "sum",
                "contribution_pct": "mean",
            }).reset_index()

            if has_cf:
                # Add partial info per item
                item_partial = df.groupby("item_id").apply(
                    lambda g: (g["coverage_flag"] != "ACTUAL").any()
                ).reset_index(name="is_partial")
                item_agg = item_agg.merge(item_partial, on="item_id", how="left")
                item_agg["표시"] = item_agg.apply(
                    lambda r: f"{r['item_id']} (추정)" if r.get("is_partial", False) else r["item_id"],
                    axis=1,
                )
                # Exclude CRITICAL severity items (all-PARTIAL, no cost at all)
                actual_items = df[df["coverage_flag"] == "ACTUAL"]["item_id"].unique()
            else:
                item_agg["표시"] = item_agg["item_id"]
                actual_items = item_agg["item_id"].unique()

            item_agg_sorted = item_agg.sort_values("contribution_krw", ascending=False)

            t_col, b_col = st.columns(2)
            with t_col:
                st.markdown("**TOP 10 (수익)**")
                top10 = item_agg_sorted.head(10)
                for _, row in top10.iterrows():
                    st.markdown(f"- **{row['표시']}**: {format_krw(row['contribution_krw'])} ({row['contribution_pct']*100:.1f}%)")

            with b_col:
                st.markdown("**BOTTOM 10 (손실)**")
                bottom10 = item_agg_sorted.tail(10).sort_values("contribution_krw")
                for _, row in bottom10.iterrows():
                    st.markdown(f"- **{row['표시']}**: {format_krw(row['contribution_krw'])} ({row['contribution_pct']*100:.1f}%)")

            # Channel ranking
            if "channel_store_id" in df.columns:
                st.subheader("채널/스토어별 수익성")
                ch_agg = df.groupby("channel_store_id").agg({
                    "contribution_krw": "sum",
                    "contribution_pct": "mean",
                }).reset_index()
                ch_agg_sorted = ch_agg.sort_values("contribution_krw", ascending=False)

                tc, bc = st.columns(2)
                with tc:
                    st.markdown("**TOP 10**")
                    for _, row in ch_agg_sorted.head(10).iterrows():
                        st.markdown(f"- **{row['channel_store_id']}**: {format_krw(row['contribution_krw'])}")
                with bc:
                    st.markdown("**BOTTOM 10**")
                    for _, row in ch_agg_sorted.tail(10).sort_values("contribution_krw").iterrows():
                        st.markdown(f"- **{row['channel_store_id']}**: {format_krw(row['contribution_krw'])}")

            with st.expander("전체 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 8: 비용 배분 (Cost Allocation)
    # ═══════════════════════════════════════════════════════════════
    with tabs[8]:
        st.header("비용 배분 상세")
        df = query_df(con, "SELECT * FROM mart.mart_charge_allocated")
        if df.empty:
            st.info("비용 배분 데이터가 없습니다.")
        else:
            total = df["allocated_amount_krw"].sum()
            st.metric("총 배분 금액", format_krw(total))

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("비용 도메인별")
                if "charge_domain" in df.columns:
                    by_domain = df.groupby("charge_domain")["allocated_amount_krw"].sum().reset_index()
                    by_domain["도메인"] = by_domain["charge_domain"].map(
                        lambda x: CHARGE_DOMAIN_KR.get(x, x)
                    )
                    st.bar_chart(by_domain.set_index("도메인")["allocated_amount_krw"])

            with c2:
                st.subheader("비용 단계별")
                if "cost_stage" in df.columns:
                    by_stage = df.groupby("cost_stage")["allocated_amount_krw"].sum().reset_index()
                    by_stage["단계"] = by_stage["cost_stage"].map(
                        lambda x: COST_STAGE_KR.get(x, x)
                    )
                    st.bar_chart(by_stage.set_index("단계")["allocated_amount_krw"])

            with st.expander("상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 9: 정산 검증 (Settlement Tie-out)
    # ═══════════════════════════════════════════════════════════════
    with tabs[9]:
        st.header("정산 검증")

        # Settlement vs estimated
        st.subheader("정산 vs 추정 매출")
        df = query_df(con, "SELECT * FROM mart.mart_reco_settlement_vs_estimated")
        if df.empty:
            st.info("정산 검증 데이터가 없습니다.")
        else:
            # Tie-out KPIs: delta, abs, ratio
            if "delta_krw" in df.columns:
                total_delta = df["delta_krw"].sum()
                total_abs_delta = df["delta_krw"].abs().sum()
                settle_sum = df["settlement_revenue_krw"].sum() if "settlement_revenue_krw" in df.columns else 0
                variance_ratio = total_abs_delta / abs(settle_sum) if settle_sum != 0 else 0

                c1, c2, c3 = st.columns(3)
                c1.metric("총 차이 (Delta)", format_krw(total_delta))
                c2.metric("절대차이 합 (|Delta|)", format_krw(total_abs_delta))
                c3.metric("차이율 (|Delta|/정산)", f"{variance_ratio*100:.2f}%")

            st.dataframe(df, use_container_width=True)

        # Invoice vs allocated
        st.subheader("청구 vs 배분 대사")
        charge_reco = query_df(con, "SELECT * FROM mart.mart_reco_charges_invoice_vs_allocated")
        if charge_reco.empty:
            st.info("청구 대사 데이터가 없습니다.")
        else:
            # Tie-out KPIs
            if "delta" in charge_reco.columns:
                inv_delta = charge_reco["delta"].sum()
                inv_abs_delta = charge_reco["delta"].abs().sum()
                inv_total = charge_reco["invoice_total"].sum() if "invoice_total" in charge_reco.columns else 0
                inv_ratio = inv_abs_delta / abs(inv_total) if inv_total != 0 else 0

                c1, c2, c3 = st.columns(3)
                c1.metric("총 차이 (Delta)", format_krw(inv_delta))
                c2.metric("절대차이 합", format_krw(inv_abs_delta))
                c3.metric("차이율", f"{inv_ratio*100:.2f}%")

            untied = charge_reco[charge_reco.get("tied", pd.Series([True]*len(charge_reco))) == False]
            c1, c2 = st.columns(2)
            c1.metric("대사 건수", f"{len(charge_reco)}")
            c2.metric("불일치 건수", f"{len(untied)}")
            st.dataframe(charge_reco, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 10: 커버리지 Traffic Lights
    # ═══════════════════════════════════════════════════════════════
    with tabs[10]:
        st.header("데이터 커버리지")

        # Coverage period table
        df = query_df(con, "SELECT * FROM mart.mart_coverage_period ORDER BY severity, domain")
        if df.empty:
            st.info("커버리지 데이터가 없습니다.")
        else:
            critical = len(df[df["severity"] == "CRITICAL"])
            warn = len(df[df["severity"] == "WARN"]) if "WARN" in df["severity"].values else 0
            ok = len(df[df["severity"] == "OK"])

            c1, c2, c3 = st.columns(3)
            c1.metric("정상 (OK)", f"{ok}", delta_color="off")
            c2.metric("경고 (WARN)", f"{warn}", delta_color="off")
            c3.metric("누락 (CRITICAL)", f"{critical}", delta_color="inverse")

            # Traffic light display
            st.subheader("도메인별 상태")
            for _, row in df.iterrows():
                sev = row.get("severity", "OK")
                domain = row.get("domain", "?")
                cov_rate = row.get("coverage_rate", 0)
                inc = int(row.get("included_rows", 0) or 0)
                miss = int(row.get("missing_rows", 0) or 0)

                if sev == "OK":
                    icon = "🟢"
                elif sev == "WARN":
                    icon = "🟡"
                else:
                    icon = "🔴"

                st.markdown(f"{icon} **{domain}** — 커버리지 {cov_rate*100 if cov_rate else 0:.0f}% (포함: {inc}, 누락: {miss})")

            st.dataframe(df, use_container_width=True)

        # FX coverage (always show)
        st.subheader("FX 환율 커버리지")
        fx_sql = """
            SELECT
                r.period, r.currency,
                CASE WHEN fx.rate_to_krw IS NOT NULL THEN 'OK' ELSE 'MISSING' END as status
            FROM (
                SELECT DISTINCT period, currency
                FROM mart.mart_pnl_revenue
                WHERE currency IS NOT NULL AND currency != 'KRW'
            ) r
            LEFT JOIN core.fact_exchange_rate fx
                ON r.period = fx.period AND r.currency = fx.currency
            ORDER BY r.period, r.currency
        """
        fx_df = query_df(con, fx_sql)
        if fx_df.empty:
            st.info("외화 매출 없음 — FX 불필요")
        else:
            missing_fx = fx_df[fx_df["status"] == "MISSING"]
            if missing_fx.empty:
                st.success("모든 외화에 대해 FX 환율 확인됨")
            else:
                st.error(f"FX 누락: {len(missing_fx)}건")
                st.dataframe(missing_fx, use_container_width=True)

        # P&L mart coverage_flag summary
        st.subheader("P&L 마트 coverage_flag 요약")
        pnl_tables = [
            ("mart.mart_pnl_revenue", "매출"),
            ("mart.mart_pnl_cogs", "매출원가"),
            ("mart.mart_pnl_gross_margin", "매출총이익"),
            ("mart.mart_pnl_variable_cost", "변동비"),
            ("mart.mart_pnl_contribution", "공헌이익"),
            ("mart.mart_pnl_operating_profit", "영업이익"),
        ]
        rows = []
        for tbl, label in pnl_tables:
            if _table_has_coverage_flag(con, tbl):
                agg = query_df(con, f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN coverage_flag = 'ACTUAL' THEN 1 ELSE 0 END) as actual_n,
                        SUM(CASE WHEN coverage_flag IS NULL OR coverage_flag <> 'ACTUAL' THEN 1 ELSE 0 END) as partial_n
                    FROM {tbl}
                """)
                if not agg.empty:
                    r = agg.iloc[0]
                    total = int(r.get("total", 0) or 0)
                    actual = int(r.get("actual_n", 0) or 0)
                    partial = int(r.get("partial_n", 0) or 0)
                    pct = actual / total * 100 if total > 0 else 0
                    rows.append({"마트": label, "전체": total, "ACTUAL": actual, "PARTIAL": partial, "ACTUAL%": f"{pct:.0f}%"})
            else:
                rows.append({"마트": label, "전체": "—", "ACTUAL": "—", "PARTIAL": "—", "ACTUAL%": "미지원"})

        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
