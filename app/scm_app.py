"""SCM Dashboard - Streamlit (port 8501). UI in Korean.

Enhancements (Phase 2-4):
- Inventory value KPIs (cost join), sellable/hold/expired value
- Turnover ratio (shipped qty / avg onhand)
- Overstock value (overstock_qty * unit_cost_krw)
- Expiry risk value, days_to_expiry stats, bucket table
- QC status / hold grouping
- Warehouse type comparison (dim_warehouse join)
- Backlog KPIs (open-order + SLA-late)
- Lead time display (avg_lead_days from mart_shipment_performance)
- Matching rates (order-shipment)
- Reco center (unified tab), constraints, coverage
- Schema fail-fast: st.error + st.stop
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import duckdb
import pandas as pd
import yaml

st.set_page_config(page_title="SCM 운영 분석", layout="wide", page_icon="📦")

DB_PATH = Path(__file__).parent.parent / "data" / "scm.duckdb"
CONFIG_DIR = Path(__file__).parent.parent / "config"


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


@st.cache_data
def load_charge_policy():
    """charge_policy.yaml에서 비용 유형 정의 로드."""
    path = CONFIG_DIR / "policies" / "charge_policy.yaml"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("charge_types", {})


def format_krw(value) -> str:
    """한국 원화 표시 (억원/만원/원)."""
    if value is None or pd.isna(value):
        return "—"
    value = float(value)
    if abs(value) >= 1e8:
        return f"{value/1e8:,.1f}억원"
    elif abs(value) >= 1e4:
        return f"{value/1e4:,.0f}만원"
    return f"{value:,.0f}원"


# ── Korean label mappings ──
CHARGE_TYPE_KR = {
    "LAST_MILE_PARCEL": "택배비 (라스트마일)",
    "DOMESTIC_TRUCKING": "국내 화물운송",
    "FREIGHT_INTL_SEA": "해상운임",
    "FREIGHT_INTL_AIR": "항공운임",
    "PORT_TERMINAL_FEE": "항만/터미널비",
    "FORWARDER_FEE": "포워더 수수료",
    "CUSTOMS_DUTY": "관세",
    "CUSTOMS_VAT": "수입부가세",
    "BROKER_FEE": "관세사 수수료",
    "CARGO_INSURANCE": "적하보험",
    "3PL_STORAGE_FEE": "3PL 보관료",
    "3PL_PICK_PACK_FEE": "3PL 피킹/패킹비",
    "3PL_HANDLING_FEE": "3PL 핸들링비",
    "3PL_RETURN_PROCESSING_FEE": "3PL 반품처리비",
    "DISPOSAL_FEE": "폐기처리비",
    "PLATFORM_FEE": "플랫폼 수수료",
    "PG_FEE": "PG 결제수수료",
    "MARKETING_SPEND": "마케팅비",
}

COST_STAGE_KR = {
    "inbound_landed": "수입/입고",
    "storage": "보관",
    "outbound": "출고/배송",
    "returns": "반품",
    "period": "기간비용",
}


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


def _has_table(con, full_table: str) -> bool:
    """Check if a table exists."""
    parts = full_table.split(".")
    if len(parts) == 2:
        try:
            return con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [parts[0], parts[1]],
            ).fetchone()[0] > 0
        except Exception:
            return False
    return False


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


def render_cost_simulator():
    """비용 시뮬레이터 탭 — 엑셀처럼 단가/수량 바꾸면 즉시 결과 반영."""
    st.header("💰 물류비 시뮬레이터")
    st.caption("단가와 수량을 직접 입력하면 예상 물류비가 실시간으로 계산됩니다. (엑셀처럼!)")

    charge_policy = load_charge_policy()

    # ── 좌측: 입력 / 우측: 결과 ──
    left, right = st.columns([3, 2])

    with left:
        st.subheader("📝 기본 정보 입력")

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            total_orders = st.number_input("월 주문건수", min_value=0, value=1000, step=100, key="sim_orders")
        with col_b:
            total_qty = st.number_input("월 출고수량 (EA)", min_value=0, value=5000, step=500, key="sim_qty")
        with col_c:
            total_weight = st.number_input("월 총 중량 (kg)", min_value=0.0, value=2000.0, step=100.0, key="sim_weight")

        col_d, col_e, col_f = st.columns(3)
        with col_d:
            total_cbm = st.number_input("월 총 부피 (CBM)", min_value=0.0, value=50.0, step=5.0, key="sim_cbm")
        with col_e:
            avg_sku_count = st.number_input("평균 SKU 라인수/주문", min_value=1.0, value=2.0, step=0.5, key="sim_lines")
        with col_f:
            avg_revenue = st.number_input("월 매출 (만원)", min_value=0, value=10000, step=1000, key="sim_rev")

        col_g, col_h = st.columns(2)
        with col_g:
            avg_stock_qty = st.number_input("평균 보관수량 (EA/일)", min_value=0, value=3000, step=500, key="sim_stock")
        with col_h:
            avg_stock_cbm = st.number_input("평균 보관부피 (CBM/일)", min_value=0.0, value=30.0, step=5.0, key="sim_stock_cbm")

        st.divider()
        st.subheader("📋 비용 유형별 단가 설정")
        st.caption("0으로 두면 해당 비용은 계산에서 제외됩니다.")

    # ── 비용 유형별 단가 입력 + 자동 계산 ──
    stages = {}
    for ct_code, ct_info in charge_policy.items():
        stage = ct_info.get("cost_stage", "period")
        if stage not in stages:
            stages[stage] = []
        stages[stage].append((ct_code, ct_info))

    results = []

    with left:
        for stage_code, items in stages.items():
            stage_name = COST_STAGE_KR.get(stage_code, stage_code)
            st.markdown(f"**{stage_name}**")

            for ct_code, ct_info in items:
                kr_name = CHARGE_TYPE_KR.get(ct_code, ct_code)
                basis = ct_info.get("default_allocation_basis", "qty")

                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    unit_price = st.number_input(
                        f"{kr_name}",
                        min_value=0.0,
                        value=0.0,
                        step=10.0,
                        key=f"sim_price_{ct_code}",
                        help=f"배분 기준: {basis}"
                    )
                with col2:
                    st.caption(f"기준: {basis}")
                    if basis in ("order_count",):
                        basis_qty = total_orders
                        basis_label = f"{total_orders:,.0f}건"
                    elif basis in ("line_count",):
                        basis_qty = total_orders * avg_sku_count
                        basis_label = f"{basis_qty:,.0f}라인"
                    elif basis in ("weight",):
                        basis_qty = total_weight
                        basis_label = f"{total_weight:,.0f}kg"
                    elif basis in ("volume_cbm",):
                        basis_qty = total_cbm
                        basis_label = f"{total_cbm:,.1f}CBM"
                    elif basis in ("qty",):
                        basis_qty = total_qty
                        basis_label = f"{total_qty:,.0f}EA"
                    elif basis in ("value",):
                        basis_qty = avg_revenue * 10000
                        basis_label = f"{avg_revenue:,.0f}만원"
                    elif basis in ("revenue",):
                        basis_qty = avg_revenue * 10000
                        basis_label = f"{avg_revenue:,.0f}만원"
                    elif basis in ("onhand_cbm_days",):
                        basis_qty = avg_stock_cbm * 30
                        basis_label = f"{basis_qty:,.0f}CBM·일"
                    elif basis in ("onhand_qty_days",):
                        basis_qty = avg_stock_qty * 30
                        basis_label = f"{basis_qty:,.0f}EA·일"
                    else:
                        basis_qty = total_qty
                        basis_label = f"{total_qty:,.0f}EA"
                    st.caption(basis_label)
                with col3:
                    estimated = unit_price * basis_qty
                    if unit_price > 0:
                        st.metric("예상금액", format_krw(estimated))
                    else:
                        st.caption("-")

                if unit_price > 0:
                    results.append({
                        "비용유형": kr_name,
                        "비용코드": ct_code,
                        "단계": stage_name,
                        "단가": unit_price,
                        "배분기준": basis,
                        "기준수량": basis_qty,
                        "예상금액": estimated,
                    })

    # ── 우측: 결과 요약 ──
    with right:
        st.subheader("📊 시뮬레이션 결과")

        if not results:
            st.info("왼쪽에서 비용 단가를 입력하면 여기에 결과가 표시됩니다.")
        else:
            result_df = pd.DataFrame(results)
            total_cost = result_df["예상금액"].sum()

            st.metric("💰 총 예상 물류비", format_krw(total_cost))

            col1, col2 = st.columns(2)
            if total_orders > 0:
                col1.metric("건당 물류비", format_krw(total_cost / total_orders))
            if total_qty > 0:
                col2.metric("EA당 물류비", format_krw(total_cost / total_qty))

            revenue_won = avg_revenue * 10000
            if revenue_won > 0:
                logistics_ratio = total_cost / revenue_won * 100
                st.metric("물류비율 (대매출)", f"{logistics_ratio:.1f}%",
                          delta=f"{'높음 ⚠️' if logistics_ratio > 15 else '양호'}")

            st.divider()

            st.subheader("비용 단계별 비중")
            stage_summary = result_df.groupby("단계")["예상금액"].sum().reset_index()
            stage_summary["비중(%)"] = (stage_summary["예상금액"] / total_cost * 100).round(1)
            stage_summary["금액"] = stage_summary["예상금액"].apply(format_krw)

            for _, row in stage_summary.iterrows():
                st.markdown(f"**{row['단계']}**: {row['금액']} ({row['비중(%)']}%)")
                st.progress(min(row["비중(%)"] / 100, 1.0))

            st.divider()

            st.subheader("비용 TOP 5")
            top5 = result_df.nlargest(5, "예상금액")
            for _, row in top5.iterrows():
                st.markdown(f"**{row['비용유형']}**: {format_krw(row['예상금액'])}")

            st.divider()

            st.subheader("상세 내역")
            display_df = result_df[["비용유형", "단계", "단가", "기준수량", "예상금액"]].copy()
            display_df["단가"] = display_df["단가"].apply(lambda x: f"{x:,.0f}")
            display_df["기준수량"] = display_df["기준수량"].apply(lambda x: f"{x:,.0f}")
            display_df["예상금액"] = display_df["예상금액"].apply(format_krw)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            st.divider()
            st.subheader("🔍 건당 비용 분석")
            if total_orders > 0:
                per_order = result_df.copy()
                per_order["건당비용"] = per_order["예상금액"] / total_orders
                per_order = per_order[["비용유형", "단계", "건당비용"]].copy()
                per_order["건당비용"] = per_order["건당비용"].apply(format_krw)
                st.dataframe(per_order, use_container_width=True, hide_index=True)


def main():
    st.title("📦 SCM 운영 분석 대시보드")

    con = get_connection()
    if con is None:
        st.error("데이터베이스가 없습니다. `python run.py --init` 을 먼저 실행해주세요.")
        st.stop()

    tabs = st.tabs([
        "재고 현황",           # 0
        "입고/발주",           # 1
        "🚚 출고 현황",        # 2
        "📦 반품 분석",        # 3
        "품절 위험",           # 4
        "과재고",             # 5
        "유통기한 관리",       # 6
        "서비스 레벨",         # 7
        "제약/병목",           # 8
        "📋 대사/검증",        # 9 (unified reco)
        "💰 비용 시뮬레이터",   # 10
    ])

    # ═══════════════════════════════════════════════════════════════
    # Tab 0: 재고 현황 (Inventory — enhanced with value, QC, warehouse)
    # ═══════════════════════════════════════════════════════════════
    with tabs[0]:
        st.header("재고 현황")
        df = query_df(con, "SELECT * FROM mart.mart_inventory_onhand")
        if df.empty:
            st.info("재고 데이터가 없습니다.")
        else:
            # Basic KPIs
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 재고수량", f"{df['onhand_qty'].sum():,.0f}")
            col2.metric("판매가능 수량", f"{df['sellable_qty'].sum():,.0f}")
            col3.metric("차단 수량", f"{df['blocked_qty'].sum():,.0f}")
            col4.metric("만료 수량", f"{df['expired_qty'].sum():,.0f}")

            # --- 2-1: Inventory Value ---
            st.subheader("재고 금액 (원가 기준)")
            st.caption("원가 마스터(fact_cost_structure) 최신 단가 적용")
            inv_value_sql = """
                WITH cost_agg AS (
                    SELECT item_id, effective_from,
                           SUM(cost_per_unit_krw) as unit_cost_krw
                    FROM core.fact_cost_structure
                    GROUP BY item_id, effective_from
                ),
                cost_latest AS (
                    SELECT item_id, unit_cost_krw,
                           ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY effective_from DESC) as rn
                    FROM cost_agg
                )
                SELECT
                    i.item_id,
                    i.warehouse_id,
                    i.onhand_qty,
                    i.sellable_qty,
                    i.blocked_qty,
                    i.expired_qty,
                    c.unit_cost_krw,
                    CASE WHEN c.unit_cost_krw IS NOT NULL THEN i.onhand_qty * c.unit_cost_krw END as total_value,
                    CASE WHEN c.unit_cost_krw IS NOT NULL THEN i.sellable_qty * c.unit_cost_krw END as sellable_value,
                    CASE WHEN c.unit_cost_krw IS NOT NULL THEN i.blocked_qty * c.unit_cost_krw END as hold_value,
                    CASE WHEN c.unit_cost_krw IS NOT NULL THEN i.expired_qty * c.unit_cost_krw END as expired_value
                FROM mart.mart_inventory_onhand i
                LEFT JOIN cost_latest c ON i.item_id = c.item_id AND c.rn = 1
            """
            inv_val = query_df(con, inv_value_sql)
            if not inv_val.empty:
                # Count cost coverage
                has_cost = inv_val["unit_cost_krw"].notna().sum()
                no_cost = inv_val["unit_cost_krw"].isna().sum()

                vc1, vc2, vc3, vc4 = st.columns(4)
                vc1.metric("총 재고금액", format_krw(inv_val["total_value"].sum()))
                vc2.metric("판매가능 금액", format_krw(inv_val["sellable_value"].sum()))
                vc3.metric("차단 금액", format_krw(inv_val["hold_value"].sum()))
                vc4.metric("만료 금액", format_krw(inv_val["expired_value"].sum()))

                if no_cost > 0:
                    st.warning(f"원가 누락 {no_cost}건 / 전체 {has_cost + no_cost}건 ({no_cost/(has_cost+no_cost)*100:.0f}%) — 누락 품목은 금액 미산출")

            # --- 3-1: QC status / hold ---
            st.subheader("QC / 차단 상태")
            has_qc = _has_column(con, "mart", "mart_inventory_onhand", "qc_status")
            has_hold = _has_column(con, "mart", "mart_inventory_onhand", "hold_flag")

            if has_qc:
                qc_df = query_df(con, "SELECT qc_status, SUM(onhand_qty) as qty FROM mart.mart_inventory_onhand GROUP BY qc_status")
                if not qc_df.empty:
                    st.bar_chart(qc_df.set_index("qc_status")["qty"])
            elif has_hold:
                # Use blocked_qty as proxy
                total_blocked = df["blocked_qty"].sum()
                total_all = df["onhand_qty"].sum()
                hc1, hc2 = st.columns(2)
                hc1.metric("차단 수량", f"{total_blocked:,.0f}")
                hc2.metric("차단율", f"{total_blocked/total_all*100:.1f}%" if total_all > 0 else "—")
            else:
                st.caption("QC status / hold_flag 컬럼 없음 — blocked_qty 기준으로 표시")
                total_blocked = df["blocked_qty"].sum()
                total_all = df["onhand_qty"].sum()
                hc1, hc2 = st.columns(2)
                hc1.metric("차단 수량", f"{total_blocked:,.0f}")
                hc2.metric("차단율", f"{total_blocked/total_all*100:.1f}%" if total_all > 0 else "—")

            # --- 3-2: Warehouse type comparison ---
            st.subheader("창고 유형별 재고")
            has_dim_wh = _has_table(con, "core.dim_warehouse")
            if has_dim_wh:
                wh_df = query_df(con, """
                    SELECT
                        COALESCE(w.warehouse_type, 'UNKNOWN') as warehouse_type,
                        COALESCE(w.country, 'KR') as country,
                        SUM(i.onhand_qty) as onhand_qty,
                        SUM(i.sellable_qty) as sellable_qty,
                        SUM(i.blocked_qty) as blocked_qty,
                        COUNT(DISTINCT i.item_id) as sku_count
                    FROM mart.mart_inventory_onhand i
                    LEFT JOIN core.dim_warehouse w ON i.warehouse_id = w.warehouse_id
                    GROUP BY 1, 2
                    ORDER BY onhand_qty DESC
                """)
                if not wh_df.empty:
                    # Country filter (재고국가)
                    countries = sorted(wh_df["country"].unique().tolist())
                    if len(countries) > 1:
                        sel_country = st.selectbox("재고국가 필터", ["전체"] + countries, key="inv_country")
                        if sel_country != "전체":
                            wh_df = wh_df[wh_df["country"] == sel_country]

                    st.bar_chart(wh_df.set_index("warehouse_type")["onhand_qty"])
                    st.dataframe(wh_df, use_container_width=True, hide_index=True)
            else:
                st.caption("dim_warehouse 미등록 — 창고 유형 비교 불가")

            # Expiry bucket
            if "expiry_bucket" in df.columns:
                st.subheader("유통기한 버킷별 재고")
                bucket_df = df.groupby("expiry_bucket")["onhand_qty"].sum().reset_index()
                st.bar_chart(bucket_df.set_index("expiry_bucket"))

            with st.expander("재고 상세 데이터", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 1: 입고/발주 (PO/Receipt)
    # ═══════════════════════════════════════════════════════════════
    with tabs[1]:
        st.header("입고/발주 현황")
        df = query_df(con, "SELECT * FROM mart.mart_open_po")
        if df.empty:
            st.info("발주 데이터가 없습니다.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("미입고 발주 건수", f"{len(df):,}")
            col2.metric("미입고 수량", f"{df['qty_open'].sum():,.0f}")
            if "delay_days" in df.columns:
                delayed = df[df["delay_days"] > 0]
                col3.metric("지연 건수", f"{len(delayed):,}")

            # PO lead time analysis
            if "po_lead_days" in df.columns:
                received = df[df["po_lead_days"].notna()]
                if not received.empty:
                    st.subheader("📦 발주 리드타임 분석")
                    avg_lead = received["po_lead_days"].mean()
                    max_lead = received["po_lead_days"].max()
                    min_lead = received["po_lead_days"].min()

                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("평균 리드타임", f"{avg_lead:.1f}일")
                    col_b.metric("최단 리드타임", f"{min_lead:.0f}일")
                    col_c.metric("최장 리드타임", f"{max_lead:.0f}일")

                    if "eta_vs_actual_days" in df.columns:
                        has_eta = received[received["eta_vs_actual_days"].notna()]
                        if not has_eta.empty:
                            avg_gap = has_eta["eta_vs_actual_days"].mean()
                            on_time = len(has_eta[has_eta["eta_vs_actual_days"] <= 0])
                            late = len(has_eta[has_eta["eta_vs_actual_days"] > 0])

                            st.subheader("⏱️ ETA 정확도")
                            col_d, col_e, col_f = st.columns(3)
                            col_d.metric("평균 ETA 차이", f"{avg_gap:+.1f}일",
                                         help="양수=지연, 음수=조기입고")
                            col_e.metric("정시/조기 입고", f"{on_time}건")
                            col_f.metric("지연 입고", f"{late}건",
                                         delta=f"-{late}" if late > 0 else None,
                                         delta_color="inverse")

                    if "supplier_id" in received.columns:
                        st.subheader("공급업체별 평균 리드타임")
                        by_supplier = received.groupby("supplier_id")["po_lead_days"].agg(
                            ["mean", "count"]
                        ).reset_index()
                        by_supplier.columns = ["supplier_id", "avg_lead_days", "po_count"]
                        by_supplier = by_supplier.sort_values("avg_lead_days", ascending=False)
                        st.bar_chart(by_supplier.set_index("supplier_id")["avg_lead_days"])

            with st.expander("발주 상세", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 2: 출고 현황 (Shipment — enhanced with backlog, lead time, matching)
    # ═══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.header("🚚 출고 현황")

        # Shipment performance
        perf = query_df(con, "SELECT * FROM mart.mart_shipment_performance")
        if perf.empty:
            st.info("출고 데이터가 없습니다. 출고(fact_shipment) 파일을 투입해주세요.")
        else:
            total_shipments = int(perf["total_shipments"].sum())
            total_qty = perf["total_qty_shipped"].sum()
            total_weight = perf["total_weight"].sum()
            avg_on_time = perf["on_time_pct"].mean() * 100

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 출고 건수", f"{total_shipments:,}")
            col2.metric("총 출고 수량", f"{total_qty:,.0f}")
            col3.metric("총 출고 중량(kg)", f"{total_weight:,.1f}")
            col4.metric("정시출고율", f"{avg_on_time:.1f}%")

            # --- 3-4: Lead time display ---
            if "avg_lead_days" in perf.columns:
                avg_lead = perf["avg_lead_days"].mean()
                if pd.notna(avg_lead):
                    st.metric("평균 리드타임 (주문→출고)", f"{avg_lead:.1f}일")

            # Period filter
            periods = sorted(perf["period"].unique().tolist())
            if periods:
                selected = st.selectbox("기간 선택", ["전체"] + periods, key="ship_period")
                if selected != "전체":
                    perf = perf[perf["period"] == selected]

            # --- 3-3: Backlog KPIs ---
            st.subheader("📋 미출고 백로그 (Sales only)")
            st.caption("channel_order_id IS NOT NULL | open-order = 주문 미출고")
            backlog_sql = """
                SELECT
                    COUNT(DISTINCT o.channel_order_id) as open_orders,
                    COALESCE(SUM(o.qty_ordered), 0) as open_qty
                FROM core.fact_order o
                LEFT JOIN core.fact_shipment s
                    ON o.channel_order_id = s.channel_order_id
                    AND o.item_id = s.item_id
                WHERE o.channel_order_id IS NOT NULL
                  AND s.shipment_id IS NULL
            """
            backlog_df = query_df(con, backlog_sql)
            if not backlog_df.empty:
                bc1, bc2 = st.columns(2)
                bc1.metric("미출고 주문", f"{int(backlog_df.iloc[0].get('open_orders', 0)):,}건")
                bc2.metric("미출고 수량", f"{float(backlog_df.iloc[0].get('open_qty', 0)):,.0f}")
            st.caption("정의: fact_order에 있으나 fact_shipment에 매칭 없는 주문 (sales)")

            # --- 0-3: Matching rates ---
            st.subheader("📊 주문-출고 매칭율")
            st.caption("주문 기준: 주문건 매칭율 + 라인수 매칭율")
            match_sql = """
                WITH orders AS (
                    SELECT
                        channel_order_id,
                        COUNT(*) as line_count
                    FROM core.fact_order
                    WHERE channel_order_id IS NOT NULL
                    GROUP BY channel_order_id
                ),
                shipped_orders AS (
                    SELECT DISTINCT channel_order_id
                    FROM core.fact_shipment
                    WHERE channel_order_id IS NOT NULL
                ),
                shipped_lines AS (
                    SELECT channel_order_id, COUNT(*) as line_count
                    FROM core.fact_shipment
                    WHERE channel_order_id IS NOT NULL
                    GROUP BY channel_order_id
                )
                SELECT
                    COUNT(DISTINCT o.channel_order_id) as total_orders,
                    COUNT(DISTINCT so.channel_order_id) as matched_orders,
                    SUM(o.line_count) as total_lines,
                    COALESCE(SUM(sl.line_count), 0) as matched_lines
                FROM orders o
                LEFT JOIN shipped_orders so ON o.channel_order_id = so.channel_order_id
                LEFT JOIN shipped_lines sl ON o.channel_order_id = sl.channel_order_id
            """
            match_df = query_df(con, match_sql)
            if not match_df.empty:
                r = match_df.iloc[0]
                tot_o = int(r.get("total_orders", 0) or 0)
                mat_o = int(r.get("matched_orders", 0) or 0)
                tot_l = int(r.get("total_lines", 0) or 0)
                mat_l = int(r.get("matched_lines", 0) or 0)

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("총 주문건", f"{tot_o:,}")
                mc2.metric("주문 매칭율", f"{mat_o/tot_o*100:.1f}%" if tot_o > 0 else "—")
                mc3.metric("총 라인수", f"{tot_l:,}")
                mc4.metric("라인 매칭율", f"{mat_l/tot_l*100:.1f}%" if tot_l > 0 else "—")

            # Warehouse breakdown
            st.subheader("창고별 출고 현황")
            if "warehouse_id" in perf.columns:
                by_wh = perf.groupby("warehouse_id").agg({
                    "total_shipments": "sum",
                    "total_qty_shipped": "sum",
                    "on_time_pct": "mean"
                }).reset_index()
                st.bar_chart(by_wh.set_index("warehouse_id")["total_shipments"])

            if "channel_store_id" in perf.columns:
                by_ch = perf.groupby("channel_store_id").agg({
                    "total_shipments": "sum",
                    "total_qty_shipped": "sum",
                }).reset_index()
                if len(by_ch) > 1:
                    st.subheader("채널별 출고 현황")
                    st.bar_chart(by_ch.set_index("channel_store_id")["total_qty_shipped"])

            with st.expander("출고 성과 상세", expanded=False):
                st.dataframe(perf, use_container_width=True)

        # Daily shipment trend
        st.subheader("📈 일별 출고 추이")
        daily_ship = query_df(con, "SELECT * FROM mart.mart_shipment_daily ORDER BY ship_date")
        if daily_ship.empty:
            st.info("일별 출고 데이터가 없습니다.")
        else:
            if "ship_date" in daily_ship.columns:
                chart_data = daily_ship.groupby("ship_date")["shipment_count"].sum().reset_index()
                st.line_chart(chart_data.set_index("ship_date")["shipment_count"])

            st.subheader("일별 출고 수량")
            qty_data = daily_ship.groupby("ship_date")["qty_shipped"].sum().reset_index()
            st.area_chart(qty_data.set_index("ship_date")["qty_shipped"])

            with st.expander("일별 출고 데이터", expanded=False):
                st.dataframe(daily_ship, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 3: 반품 분석
    # ═══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.header("📦 반품 분석")

        ret = query_df(con, "SELECT * FROM mart.mart_return_analysis")
        if ret.empty:
            st.info("반품 데이터가 없습니다. 반품(fact_return) 파일을 투입해주세요.")
        else:
            total_returns = int(ret["return_count"].sum())
            total_qty_ret = ret["qty_returned"].sum()
            total_qty_ship = ret["qty_shipped"].sum()
            overall_rate = (total_qty_ret / total_qty_ship * 100) if total_qty_ship > 0 else 0

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 반품 건수", f"{total_returns:,}")
            col2.metric("총 반품 수량", f"{total_qty_ret:,.0f}")
            col3.metric("총 출고 수량", f"{total_qty_ship:,.0f}")
            col4.metric("반품율", f"{overall_rate:.1f}%",
                        delta=f"{overall_rate:.1f}%" if overall_rate > 5 else None,
                        delta_color="inverse")

            periods = sorted(ret["period"].unique().tolist())
            if periods:
                selected = st.selectbox("기간 선택", ["전체"] + periods, key="ret_period")
                if selected != "전체":
                    ret = ret[ret["period"] == selected]

            if "reason" in ret.columns:
                st.subheader("반품 사유별 분석")
                by_reason = ret.groupby("reason")["qty_returned"].sum().reset_index().sort_values("qty_returned", ascending=False)
                st.bar_chart(by_reason.set_index("reason")["qty_returned"])

            if "disposition" in ret.columns:
                st.subheader("반품 처분별 분석")
                by_disp = ret.groupby("disposition")["qty_returned"].sum().reset_index()
                st.bar_chart(by_disp.set_index("disposition")["qty_returned"])

            st.subheader("🔴 품목별 반품율 TOP 10")
            by_item = ret.groupby("item_id").agg({
                "qty_returned": "sum", "qty_shipped": "sum", "return_count": "sum"
            }).reset_index()
            by_item["return_rate"] = by_item.apply(
                lambda r: r["qty_returned"] / r["qty_shipped"] * 100 if r["qty_shipped"] > 0 else 0, axis=1
            )
            top_items = by_item.sort_values("return_rate", ascending=False).head(10)
            st.dataframe(top_items, use_container_width=True)

            with st.expander("반품 상세", expanded=False):
                st.dataframe(ret, use_container_width=True)

        st.subheader("📈 일별 반품 추이")
        daily_ret = query_df(con, "SELECT * FROM mart.mart_return_daily ORDER BY return_date")
        if daily_ret.empty:
            st.info("일별 반품 데이터가 없습니다.")
        else:
            if "return_date" in daily_ret.columns:
                chart_data = daily_ret.groupby("return_date")["return_count"].sum().reset_index()
                st.line_chart(chart_data.set_index("return_date")["return_count"])

                st.subheader("일별 반품 수량")
                qty_data = daily_ret.groupby("return_date")["qty_returned"].sum().reset_index()
                st.area_chart(qty_data.set_index("return_date")["qty_returned"])

            with st.expander("일별 반품 데이터", expanded=False):
                st.dataframe(daily_ret, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 4: 품절 위험 (Stockout Risk)
    # ═══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.header("품절 위험")
        df = query_df(con, "SELECT * FROM mart.mart_stockout_risk")
        if df.empty:
            st.info("품절 위험 데이터가 없습니다.")
        else:
            at_risk = df[df.get("risk_flag", pd.Series([False]*len(df))) == True]
            col1, col2 = st.columns(2)
            col1.metric("총 품목수", f"{len(df):,}")
            col2.metric("위험 품목수", f"{len(at_risk):,}", delta=f"-{len(at_risk)}" if len(at_risk) > 0 else None, delta_color="inverse")

            st.subheader("품절 위험 품목")
            st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 5: 과재고 (Overstock — enhanced with value + turnover)
    # ═══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.header("과재고 현황")
        df = query_df(con, "SELECT * FROM mart.mart_overstock")
        if df.empty:
            st.info("과재고 데이터가 없습니다.")
        else:
            overstock = df[df.get("overstock_flag", pd.Series([False]*len(df))) == True]
            col1, col2 = st.columns(2)
            col1.metric("총 품목수", f"{len(df):,}")
            col2.metric("과재고 품목수", f"{len(overstock):,}")

            if "days_on_hand" in df.columns:
                st.subheader("재고일수(DOH) 분포")
                st.bar_chart(df.set_index("item_id")["days_on_hand"].head(20))

            # --- 2-2: Turnover ---
            st.subheader("📊 재고회전율 (Turnover)")
            st.caption("회전율 = 기간 출고수량(sales) / 기간 평균 재고수량")
            turn_sql = """
                WITH shipped AS (
                    SELECT item_id, SUM(qty_shipped) as shipped_qty
                    FROM core.fact_shipment
                    WHERE channel_order_id IS NOT NULL
                    GROUP BY item_id
                ),
                onhand AS (
                    SELECT item_id, AVG(onhand_qty) as avg_onhand
                    FROM mart.mart_inventory_onhand
                    GROUP BY item_id
                )
                SELECT
                    o.item_id,
                    o.avg_onhand,
                    COALESCE(s.shipped_qty, 0) as shipped_qty,
                    CASE WHEN o.avg_onhand > 0
                         THEN COALESCE(s.shipped_qty, 0) / o.avg_onhand
                         ELSE NULL
                    END as turnover_ratio
                FROM onhand o
                LEFT JOIN shipped s ON o.item_id = s.item_id
                ORDER BY turnover_ratio ASC NULLS FIRST
            """
            turn_df = query_df(con, turn_sql)
            if not turn_df.empty:
                avg_turn = turn_df["turnover_ratio"].mean()
                tc1, tc2 = st.columns(2)
                tc1.metric("평균 회전율", f"{avg_turn:.2f}" if pd.notna(avg_turn) else "—")
                slow = turn_df[turn_df["turnover_ratio"] < 0.5] if "turnover_ratio" in turn_df.columns else pd.DataFrame()
                tc2.metric("저회전 품목 (<0.5)", f"{len(slow)}건")

                st.markdown("**회전율 하위 10 (체류 품목)**")
                st.dataframe(turn_df.head(10), use_container_width=True, hide_index=True)

            # --- 2-3: Overstock value ---
            st.subheader("과재고 금액")
            if not overstock.empty and "overstock_qty" in overstock.columns:
                ov_value_sql = """
                    WITH cost_agg AS (
                        SELECT item_id, effective_from,
                               SUM(cost_per_unit_krw) as unit_cost_krw
                        FROM core.fact_cost_structure
                        GROUP BY item_id, effective_from
                    ),
                    cost_latest AS (
                        SELECT item_id, unit_cost_krw,
                               ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY effective_from DESC) as rn
                        FROM cost_agg
                    )
                    SELECT
                        o.item_id,
                        o.warehouse_id,
                        o.overstock_qty,
                        c.unit_cost_krw,
                        CASE WHEN c.unit_cost_krw IS NOT NULL THEN o.overstock_qty * c.unit_cost_krw END as overstock_value
                    FROM mart.mart_overstock o
                    LEFT JOIN cost_latest c ON o.item_id = c.item_id AND c.rn = 1
                    WHERE o.overstock_flag = true
                    ORDER BY overstock_value DESC NULLS LAST
                """
                ov_val = query_df(con, ov_value_sql)
                if not ov_val.empty:
                    total_ov_val = ov_val["overstock_value"].sum()
                    no_cost_n = ov_val["unit_cost_krw"].isna().sum()
                    st.metric("과재고 금액 합계", format_krw(total_ov_val))
                    if no_cost_n > 0:
                        st.warning(f"원가 미확인 {no_cost_n}건 — 금액 미산출")

                    st.markdown("**과재고 금액 TOP 10**")
                    st.dataframe(ov_val.head(10), use_container_width=True, hide_index=True)

            with st.expander("과재고 상세", expanded=False):
                st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 6: 유통기한 관리 (Expiry — enhanced with value + stats)
    # ═══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.header("유통기한 관리")
        df = query_df(con, "SELECT * FROM mart.mart_expiry_risk")
        if df.empty:
            st.info("유통기한 위험 데이터가 없습니다.")
        else:
            # --- 2-4: Expiry risk value + stats ---
            st.subheader("유통기한 위험 현황")

            # Risk value KPIs
            has_value = "risk_value_krw" in df.columns
            has_days = "days_to_expiry" in df.columns

            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("총 위험 로트", f"{len(df)}건")
            kpi2.metric("총 위험 수량", f"{df['onhand_qty'].sum():,.0f}")

            if has_value:
                # risk_value_krw may be NULL for items without cost
                val_sum = df["risk_value_krw"].sum()
                null_val = df["risk_value_krw"].isna().sum()
                kpi3.metric("위험 금액", format_krw(val_sum))
                if null_val > 0:
                    st.warning(f"원가 미확인 {null_val}건 — 금액 미산출 (NULL)")
            else:
                kpi3.metric("위험 금액", "컬럼 없음")

            if has_days:
                avg_days = df["days_to_expiry"].mean()
                min_days = df["days_to_expiry"].min()
                kpi4.metric("평균 잔여일수", f"{avg_days:.0f}일" if pd.notna(avg_days) else "—")
                st.caption(f"최소 잔여일수: {min_days}일" if pd.notna(min_days) else "")

            # Bucket table
            if "expiry_bucket" in df.columns:
                st.subheader("위험 버킷별 분포")
                bucket_agg = df.groupby("expiry_bucket").agg({
                    "onhand_qty": "sum",
                    "item_id": "count",
                }).reset_index()
                bucket_agg.columns = ["버킷", "수량", "로트수"]

                if has_value:
                    bucket_val = df.groupby("expiry_bucket")["risk_value_krw"].sum().reset_index()
                    bucket_val.columns = ["버킷", "위험금액"]
                    bucket_agg = bucket_agg.merge(bucket_val, on="버킷", how="left")
                    bucket_agg["위험금액"] = bucket_agg["위험금액"].apply(format_krw)

                st.dataframe(bucket_agg, use_container_width=True, hide_index=True)
                st.bar_chart(df.groupby("expiry_bucket")["onhand_qty"].sum())

            with st.expander("유통기한 위험 상세", expanded=False):
                st.dataframe(df, use_container_width=True)

        st.subheader("FEFO 피킹 리스트")
        fefo = query_df(con, "SELECT * FROM mart.mart_fefo_pick_list ORDER BY fefo_rank LIMIT 50")
        if fefo.empty:
            st.info("FEFO 데이터가 없습니다.")
        else:
            st.dataframe(fefo, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 7: 서비스 레벨
    # ═══════════════════════════════════════════════════════════════
    with tabs[7]:
        st.header("서비스 레벨 (주간)")
        df = query_df(con, "SELECT * FROM mart.mart_service_level ORDER BY week_start")
        if df.empty:
            st.info("서비스 레벨 데이터가 없습니다.")
        else:
            avg_sl = df["service_level_pct"].mean() * 100
            st.metric("평균 서비스 레벨", f"{avg_sl:.1f}%")

            if "week_start" in df.columns:
                chart_df = df.set_index("week_start")["service_level_pct"]
                st.line_chart(chart_df)

            st.dataframe(df, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 8: 제약/병목 (Constraints — enhanced with root cause + effectiveness)
    # ═══════════════════════════════════════════════════════════════
    with tabs[8]:
        st.header("제약/병목 감지")
        df = query_df(con, "SELECT * FROM mart.mart_constraint_signals ORDER BY severity, detected_at DESC")
        if df.empty:
            st.info("제약 신호가 없습니다.")
        else:
            critical = len(df[df["severity"] == "CRITICAL"])
            high = len(df[df["severity"] == "HIGH"])
            col1, col2, col3 = st.columns(3)
            col1.metric("CRITICAL 신호", f"{critical}")
            col2.metric("HIGH 신호", f"{high}")
            col3.metric("총 신호", f"{len(df)}")

            st.subheader("제약 신호 목록")
            st.dataframe(df, use_container_width=True)

        # --- 4-2: Constraint root cause + effectiveness ---
        with st.expander("🔍 근본 원인 분석", expanded=False):
            root = query_df(con, "SELECT * FROM mart.mart_constraint_root_cause")
            if root.empty:
                st.info("근본 원인 분석 데이터가 없습니다.")
            else:
                st.dataframe(root, use_container_width=True)

        with st.expander("📈 제약 해소 효과", expanded=False):
            eff = query_df(con, "SELECT * FROM mart.mart_constraint_effectiveness")
            if eff.empty:
                st.info("효과 측정 데이터가 없습니다.")
            else:
                resolved = len(eff[eff.get("resolved", pd.Series([False]*len(eff))) == True])
                st.metric("해소 건수", f"{resolved}/{len(eff)}")
                st.dataframe(eff, use_container_width=True)

        st.subheader("조치 계획")
        actions = query_df(con, "SELECT * FROM mart.mart_constraint_action_plan")
        if actions.empty:
            st.info("조치 계획이 없습니다.")
        else:
            st.dataframe(actions, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 9: 대사/검증 (Unified Reco Center)
    # ═══════════════════════════════════════════════════════════════
    with tabs[9]:
        st.header("📋 대사/검증 센터")
        st.caption("데이터 정합성 확인을 위한 5개 대사 영역")

        # Expander 1: Inventory movement reco
        with st.expander("1️⃣ 재고 이동 대사 (수불 일치)", expanded=False):
            inv_reco = query_df(con, "SELECT * FROM mart.mart_reco_inventory_movement")
            if inv_reco.empty:
                st.info("재고 이동 대사 데이터가 없습니다.")
            else:
                issues = inv_reco[inv_reco["severity"].isin(["WARN", "HIGH"])] if "severity" in inv_reco.columns else pd.DataFrame()
                c1, c2 = st.columns(2)
                c1.metric("대사 건수", f"{len(inv_reco):,}")
                c2.metric("이슈 건수", f"{len(issues):,}")
                st.dataframe(inv_reco, use_container_width=True)

        # Expander 2: OMS vs WMS
        with st.expander("2️⃣ OMS vs WMS 출고 대사", expanded=False):
            oms_wms = query_df(con, "SELECT * FROM mart.mart_reco_oms_vs_wms")
            if oms_wms.empty:
                st.info("OMS vs WMS 대사 데이터가 없습니다.")
            else:
                st.warning("월말 경계 주의: 주문월과 출고월이 다를 수 있습니다.")
                if "delta" in oms_wms.columns:
                    total_delta = oms_wms["delta"].sum()
                    abs_delta = oms_wms["delta"].abs().sum()
                    c1, c2 = st.columns(2)
                    c1.metric("순차이 합", f"{total_delta:,.0f}")
                    c2.metric("절대차이 합", f"{abs_delta:,.0f}")
                if "fulfillment_rate" in oms_wms.columns:
                    avg_ful = oms_wms["fulfillment_rate"].mean()
                    st.metric("평균 이행율", f"{avg_ful*100:.1f}%" if pd.notna(avg_ful) else "—")
                st.dataframe(oms_wms, use_container_width=True)

        # Expander 3: ERP vs WMS receipt
        with st.expander("3️⃣ ERP 입고 vs WMS 입고 대사", expanded=False):
            erp_wms = query_df(con, "SELECT * FROM mart.mart_reco_erp_gr_vs_wms_receipt")
            if erp_wms.empty:
                st.info("ERP vs WMS 입고 대사 데이터가 없습니다.")
            else:
                if "delta" in erp_wms.columns:
                    c1, c2 = st.columns(2)
                    c1.metric("대사 건수", f"{len(erp_wms):,}")
                    c2.metric("차이 건수", f"{len(erp_wms[erp_wms['delta'] != 0]):,}")
                st.dataframe(erp_wms, use_container_width=True)

        # Expander 4: Settlement vs estimated
        with st.expander("4️⃣ 정산 vs 추정 매출", expanded=False):
            settle = query_df(con, "SELECT * FROM mart.mart_reco_settlement_vs_estimated")
            if settle.empty:
                st.info("정산 검증 데이터가 없습니다.")
            else:
                if "delta_krw" in settle.columns:
                    total_delta = settle["delta_krw"].sum()
                    abs_delta = settle["delta_krw"].abs().sum()
                    settle_sum = settle["settlement_revenue_krw"].sum() if "settlement_revenue_krw" in settle.columns else 0
                    ratio = abs_delta / abs(settle_sum) if settle_sum != 0 else 0

                    c1, c2, c3 = st.columns(3)
                    c1.metric("Delta 합", format_krw(total_delta))
                    c2.metric("|Delta| 합", format_krw(abs_delta))
                    c3.metric("차이율", f"{ratio*100:.2f}%")
                st.dataframe(settle, use_container_width=True)

        # Expander 5: Invoice vs allocated
        with st.expander("5️⃣ 청구 vs 배분 대사", expanded=False):
            inv_alloc = query_df(con, "SELECT * FROM mart.mart_reco_charges_invoice_vs_allocated")
            if inv_alloc.empty:
                st.info("청구 대사 데이터가 없습니다.")
            else:
                if "delta" in inv_alloc.columns:
                    inv_delta = inv_alloc["delta"].sum()
                    inv_abs = inv_alloc["delta"].abs().sum()
                    inv_total = inv_alloc["invoice_total"].sum() if "invoice_total" in inv_alloc.columns else 0
                    inv_ratio = inv_abs / abs(inv_total) if inv_total != 0 else 0

                    c1, c2, c3 = st.columns(3)
                    c1.metric("Delta 합", format_krw(inv_delta))
                    c2.metric("|Delta| 합", format_krw(inv_abs))
                    c3.metric("차이율", f"{inv_ratio*100:.2f}%")

                untied = inv_alloc[inv_alloc.get("tied", pd.Series([True]*len(inv_alloc))) == False]
                c1, c2 = st.columns(2)
                c1.metric("대사 건수", f"{len(inv_alloc)}")
                c2.metric("불일치 건수", f"{len(untied)}")
                st.dataframe(inv_alloc, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # Tab 10: 비용 시뮬레이터
    # ═══════════════════════════════════════════════════════════════
    with tabs[10]:
        render_cost_simulator()


if __name__ == "__main__":
    main()
