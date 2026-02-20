"""Physical schema SSOT for SCM analytics database.

All CREATE TABLE / CREATE SCHEMA statements live here.
No other module may define DDL.
"""
import duckdb
from pathlib import Path

DB_PATH = Path("data/scm.duckdb")

SCHEMAS = ["raw", "core", "mart", "ops"]

# ---------- SYSTEM COLUMNS appended to every CORE fact ----------
SYSTEM_COLS_DDL = """
    source_system VARCHAR NOT NULL,
    load_batch_id BIGINT NOT NULL,
    source_file_hash VARCHAR NOT NULL,
    source_pk VARCHAR,
    loaded_at TIMESTAMP DEFAULT current_timestamp
"""

# ================================================================
# RAW LAYER
# ================================================================
RAW_TABLES = {
    "raw.system_batch_log": """
        CREATE TABLE IF NOT EXISTS raw.system_batch_log (
            batch_id BIGINT PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR NOT NULL DEFAULT 'running',
            file_count INTEGER DEFAULT 0,
            rows_ingested BIGINT DEFAULT 0,
            error_msg VARCHAR
        )
    """,
    "raw.system_file_log": """
        CREATE TABLE IF NOT EXISTS raw.system_file_log (
            batch_id BIGINT NOT NULL,
            file_name VARCHAR NOT NULL,
            file_hash VARCHAR NOT NULL,
            table_name VARCHAR,
            row_count BIGINT DEFAULT 0,
            status VARCHAR NOT NULL DEFAULT 'pending',
            error_msg VARCHAR,
            processed_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "raw.system_dq_report": """
        CREATE TABLE IF NOT EXISTS raw.system_dq_report (
            batch_id BIGINT NOT NULL,
            file_name VARCHAR,
            table_name VARCHAR,
            check_name VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            passed BOOLEAN NOT NULL,
            detail VARCHAR,
            checked_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "raw.system_batch_lock": """
        CREATE TABLE IF NOT EXISTS raw.system_batch_lock (
            lock_id INTEGER PRIMARY KEY DEFAULT 1,
            locked BOOLEAN NOT NULL DEFAULT false,
            pid INTEGER,
            started_at TIMESTAMP,
            CHECK (lock_id = 1)
        )
    """,
}

# ================================================================
# CORE LAYER - DIMENSIONS
# ================================================================
CORE_DIM_TABLES = {
    "core.dim_item": """
        CREATE TABLE IF NOT EXISTS core.dim_item (
            item_id VARCHAR PRIMARY KEY,
            item_type VARCHAR,
            uom_id VARCHAR,
            pack_size DOUBLE,
            weight DOUBLE,
            volume_cbm DOUBLE,
            category VARCHAR,
            active_flag BOOLEAN DEFAULT true,
            expiry_control_flag BOOLEAN DEFAULT false,
            expiry_basis VARCHAR,
            shelf_life_days INTEGER,
            min_sellable_days INTEGER,
            pao_days INTEGER,
            qc_required_flag BOOLEAN DEFAULT false,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "core.dim_partner": """
        CREATE TABLE IF NOT EXISTS core.dim_partner (
            partner_id VARCHAR PRIMARY KEY,
            partner_type VARCHAR,
            country VARCHAR,
            default_currency VARCHAR,
            tax_profile VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "core.dim_channel_store": """
        CREATE TABLE IF NOT EXISTS core.dim_channel_store (
            channel_store_id VARCHAR PRIMARY KEY,
            channel VARCHAR,
            store VARCHAR,
            settlement_currency VARCHAR,
            settlement_cycle VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "core.dim_warehouse": """
        CREATE TABLE IF NOT EXISTS core.dim_warehouse (
            warehouse_id VARCHAR PRIMARY KEY,
            warehouse_type VARCHAR,
            country VARCHAR,
            cost_center VARCHAR,
            operator_partner_id VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
    "core.dim_uom_conversion": """
        CREATE TABLE IF NOT EXISTS core.dim_uom_conversion (
            from_uom VARCHAR NOT NULL,
            to_uom VARCHAR NOT NULL,
            factor DOUBLE NOT NULL,
            effective_from DATE NOT NULL,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (from_uom, to_uom, effective_from)
        )
    """,
    "core.dim_charge_policy": """
        CREATE TABLE IF NOT EXISTS core.dim_charge_policy (
            charge_type VARCHAR PRIMARY KEY,
            charge_domain VARCHAR NOT NULL,
            cost_stage VARCHAR NOT NULL,
            capitalizable_flag BOOLEAN NOT NULL DEFAULT false,
            default_allocation_basis VARCHAR NOT NULL,
            severity_if_missing VARCHAR NOT NULL DEFAULT 'warn',
            loaded_at TIMESTAMP DEFAULT current_timestamp
        )
    """,
}

# ================================================================
# CORE LAYER - FACTS
# ================================================================
CORE_FACT_TABLES = {
    "core.fact_order": """
        CREATE TABLE IF NOT EXISTS core.fact_order (
            channel_order_id VARCHAR NOT NULL,
            line_no BIGINT NOT NULL,
            order_date DATE NOT NULL,
            channel_store_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            qty_ordered DOUBLE NOT NULL,
            partner_id VARCHAR,
            ship_from_warehouse_id VARCHAR,
            currency VARCHAR,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (channel_order_id, line_no)
        )
    """,
    "core.fact_shipment": """
        CREATE TABLE IF NOT EXISTS core.fact_shipment (
            shipment_id VARCHAR NOT NULL,
            ship_date DATE NOT NULL,
            warehouse_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            qty_shipped DOUBLE NOT NULL,
            lot_id VARCHAR NOT NULL DEFAULT '__NONE__',
            weight DOUBLE,
            volume_cbm DOUBLE,
            channel_order_id VARCHAR,
            channel_store_id VARCHAR,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (shipment_id, item_id, lot_id)
        )
    """,
    "core.fact_return": """
        CREATE TABLE IF NOT EXISTS core.fact_return (
            return_id VARCHAR NOT NULL,
            return_date DATE NOT NULL,
            warehouse_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            qty_returned DOUBLE NOT NULL,
            lot_id VARCHAR NOT NULL DEFAULT '__NONE__',
            channel_order_id VARCHAR,
            reason VARCHAR,
            disposition VARCHAR,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (return_id, item_id, lot_id)
        )
    """,
    "core.fact_inventory_snapshot": """
        CREATE TABLE IF NOT EXISTS core.fact_inventory_snapshot (
            snapshot_date DATE NOT NULL,
            warehouse_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            lot_id VARCHAR NOT NULL,
            onhand_qty DOUBLE NOT NULL,
            expiry_date DATE,
            qc_status VARCHAR,
            hold_flag BOOLEAN DEFAULT false,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (snapshot_date, warehouse_id, item_id, lot_id)
        )
    """,
    "core.fact_po": """
        CREATE TABLE IF NOT EXISTS core.fact_po (
            po_id VARCHAR NOT NULL,
            po_date DATE NOT NULL,
            supplier_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            qty_ordered DOUBLE NOT NULL,
            eta_date DATE,
            incoterms VARCHAR,
            currency VARCHAR,
            unit_price DOUBLE,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (po_id, item_id)
        )
    """,
    "core.fact_receipt": """
        CREATE TABLE IF NOT EXISTS core.fact_receipt (
            receipt_id VARCHAR NOT NULL,
            receipt_date DATE NOT NULL,
            warehouse_id VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,
            qty_received DOUBLE NOT NULL,
            po_id VARCHAR,
            lot_id VARCHAR,
            expiry_date DATE,
            mfg_date DATE,
            qc_status VARCHAR,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (receipt_id, item_id)
        )
    """,
    "core.fact_settlement": """
        CREATE TABLE IF NOT EXISTS core.fact_settlement (
            settlement_id VARCHAR NOT NULL,
            line_no BIGINT NOT NULL,
            period VARCHAR NOT NULL,
            channel_store_id VARCHAR NOT NULL,
            currency VARCHAR NOT NULL,
            item_id VARCHAR,
            gross_sales DOUBLE,
            discounts DOUBLE,
            fees DOUBLE,
            refunds DOUBLE,
            net_payout DOUBLE,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (settlement_id, line_no)
        )
    """,
    "core.fact_charge_actual": """
        CREATE TABLE IF NOT EXISTS core.fact_charge_actual (
            invoice_no VARCHAR NOT NULL,
            invoice_line_no BIGINT NOT NULL,
            charge_type VARCHAR NOT NULL,
            amount DOUBLE NOT NULL,
            currency VARCHAR NOT NULL,
            period VARCHAR NOT NULL,
            invoice_date DATE,
            vendor_partner_id VARCHAR,
            charge_basis VARCHAR,
            reference_type VARCHAR,
            reference_id VARCHAR,
            channel_store_id VARCHAR,
            warehouse_id VARCHAR,
            country VARCHAR,
            source_system VARCHAR NOT NULL,
            load_batch_id BIGINT NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (invoice_no, invoice_line_no, charge_type)
        )
    """,
    "core.fact_exchange_rate": """
        CREATE TABLE IF NOT EXISTS core.fact_exchange_rate (
            period VARCHAR NOT NULL,
            currency VARCHAR NOT NULL,
            rate_to_krw DOUBLE NOT NULL,
            source_system VARCHAR DEFAULT 'manual',
            load_batch_id BIGINT DEFAULT 0,
            source_file_hash VARCHAR DEFAULT '',
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (period, currency)
        )
    """,
    "core.fact_cost_structure": """
        CREATE TABLE IF NOT EXISTS core.fact_cost_structure (
            item_id VARCHAR NOT NULL,
            cost_component VARCHAR NOT NULL,
            effective_from DATE NOT NULL,
            cost_per_unit_krw DOUBLE NOT NULL,
            source_system VARCHAR DEFAULT 'manual',
            load_batch_id BIGINT DEFAULT 0,
            source_file_hash VARCHAR DEFAULT '',
            source_pk VARCHAR,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (item_id, cost_component, effective_from)
        )
    """,
}

# ================================================================
# MART LAYER
# ================================================================
MART_TABLES = {
    # -- SCM marts --
    "mart.mart_inventory_onhand": """
        CREATE TABLE IF NOT EXISTS mart.mart_inventory_onhand (
            snapshot_date DATE,
            warehouse_id VARCHAR,
            item_id VARCHAR,
            lot_id VARCHAR,
            onhand_qty DOUBLE,
            sellable_qty DOUBLE,
            blocked_qty DOUBLE,
            expired_qty DOUBLE,
            final_expiry_date DATE,
            expiry_bucket VARCHAR,
            fefo_rank INTEGER,
            min_sellable_days INTEGER
        )
    """,
    "mart.mart_open_po": """
        CREATE TABLE IF NOT EXISTS mart.mart_open_po (
            po_id VARCHAR,
            item_id VARCHAR,
            supplier_id VARCHAR,
            po_date DATE,
            eta_date DATE,
            first_receipt_date DATE,
            qty_ordered DOUBLE,
            qty_received DOUBLE,
            qty_open DOUBLE,
            delay_days INTEGER,
            po_lead_days INTEGER,
            eta_vs_actual_days INTEGER,
            period VARCHAR
        )
    """,
    "mart.mart_stockout_risk": """
        CREATE TABLE IF NOT EXISTS mart.mart_stockout_risk (
            item_id VARCHAR,
            warehouse_id VARCHAR,
            sellable_qty DOUBLE,
            avg_daily_demand DOUBLE,
            days_of_cover DOUBLE,
            threshold_days INTEGER,
            risk_flag BOOLEAN,
            as_of_date DATE
        )
    """,
    "mart.mart_overstock": """
        CREATE TABLE IF NOT EXISTS mart.mart_overstock (
            item_id VARCHAR,
            warehouse_id VARCHAR,
            item_type VARCHAR,
            onhand_qty DOUBLE,
            avg_daily_demand DOUBLE,
            days_on_hand DOUBLE,
            doh_threshold INTEGER,
            overstock_flag BOOLEAN,
            overstock_qty DOUBLE,
            as_of_date DATE
        )
    """,
    "mart.mart_expiry_risk": """
        CREATE TABLE IF NOT EXISTS mart.mart_expiry_risk (
            item_id VARCHAR,
            warehouse_id VARCHAR,
            lot_id VARCHAR,
            onhand_qty DOUBLE,
            final_expiry_date DATE,
            days_to_expiry INTEGER,
            expiry_bucket VARCHAR,
            risk_value_krw DOUBLE,
            as_of_date DATE
        )
    """,
    "mart.mart_fefo_pick_list": """
        CREATE TABLE IF NOT EXISTS mart.mart_fefo_pick_list (
            warehouse_id VARCHAR,
            item_id VARCHAR,
            lot_id VARCHAR,
            onhand_qty DOUBLE,
            sellable_qty DOUBLE,
            final_expiry_date DATE,
            fefo_rank INTEGER,
            snapshot_date DATE
        )
    """,
    "mart.mart_service_level": """
        CREATE TABLE IF NOT EXISTS mart.mart_service_level (
            week_start DATE,
            channel_store_id VARCHAR,
            total_orders BIGINT,
            shipped_on_time BIGINT,
            service_level_pct DOUBLE
        )
    """,
    "mart.mart_shipment_performance": """
        CREATE TABLE IF NOT EXISTS mart.mart_shipment_performance (
            period VARCHAR,
            warehouse_id VARCHAR,
            channel_store_id VARCHAR,
            total_shipments BIGINT,
            total_qty_shipped DOUBLE,
            total_weight DOUBLE,
            total_volume_cbm DOUBLE,
            avg_qty_per_shipment DOUBLE,
            avg_lead_days DOUBLE,
            on_time_count BIGINT,
            on_time_pct DOUBLE
        )
    """,
    "mart.mart_shipment_daily": """
        CREATE TABLE IF NOT EXISTS mart.mart_shipment_daily (
            ship_date DATE,
            warehouse_id VARCHAR,
            shipment_count BIGINT,
            qty_shipped DOUBLE,
            weight DOUBLE,
            volume_cbm DOUBLE,
            unique_orders BIGINT,
            unique_items BIGINT
        )
    """,
    "mart.mart_return_analysis": """
        CREATE TABLE IF NOT EXISTS mart.mart_return_analysis (
            period VARCHAR,
            item_id VARCHAR,
            warehouse_id VARCHAR,
            channel_store_id VARCHAR,
            reason VARCHAR,
            disposition VARCHAR,
            return_count BIGINT,
            qty_returned DOUBLE,
            qty_shipped DOUBLE,
            return_rate DOUBLE
        )
    """,
    "mart.mart_return_daily": """
        CREATE TABLE IF NOT EXISTS mart.mart_return_daily (
            return_date DATE,
            warehouse_id VARCHAR,
            return_count BIGINT,
            qty_returned DOUBLE,
            unique_orders BIGINT,
            unique_items BIGINT,
            top_reason VARCHAR
        )
    """,
    # -- P&L marts --
    "mart.mart_pnl_revenue": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_revenue (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            gross_sales_krw DOUBLE,
            discounts_krw DOUBLE,
            refunds_krw DOUBLE,
            net_revenue_krw DOUBLE,
            source VARCHAR,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_cogs": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_cogs (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            qty_shipped DOUBLE,
            qty_returned DOUBLE,
            qty_net DOUBLE,
            unit_cost_krw DOUBLE,
            cogs_krw DOUBLE,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_gross_margin": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_gross_margin (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            net_revenue_krw DOUBLE,
            cogs_krw DOUBLE,
            gross_margin_krw DOUBLE,
            gross_margin_pct DOUBLE,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_variable_cost": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_variable_cost (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            charge_domain VARCHAR,
            charge_type VARCHAR,
            allocated_amount_krw DOUBLE,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_contribution": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_contribution (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            gross_margin_krw DOUBLE,
            total_variable_cost_krw DOUBLE,
            contribution_krw DOUBLE,
            contribution_pct DOUBLE,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_operating_profit": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_operating_profit (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            country VARCHAR,
            contribution_krw DOUBLE,
            fixed_cost_krw DOUBLE,
            operating_profit_krw DOUBLE,
            operating_profit_pct DOUBLE,
            coverage_flag VARCHAR
        )
    """,
    "mart.mart_pnl_waterfall_summary": """
        CREATE TABLE IF NOT EXISTS mart.mart_pnl_waterfall_summary (
            period VARCHAR,
            metric_name VARCHAR,
            metric_order INTEGER,
            amount_krw DOUBLE
        )
    """,
    # -- Reconciliation marts --
    "mart.mart_reco_inventory_movement": """
        CREATE TABLE IF NOT EXISTS mart.mart_reco_inventory_movement (
            snapshot_date DATE,
            warehouse_id VARCHAR,
            item_id VARCHAR,
            prev_onhand DOUBLE,
            receipts DOUBLE,
            shipments DOUBLE,
            returns DOUBLE,
            adjustments DOUBLE,
            expected_onhand DOUBLE,
            actual_onhand DOUBLE,
            delta DOUBLE,
            delta_ratio DOUBLE,
            severity VARCHAR
        )
    """,
    "mart.mart_reco_oms_vs_wms": """
        CREATE TABLE IF NOT EXISTS mart.mart_reco_oms_vs_wms (
            period VARCHAR,
            item_id VARCHAR,
            channel_store_id VARCHAR,
            oms_qty_ordered DOUBLE,
            wms_qty_shipped DOUBLE,
            delta DOUBLE,
            fulfillment_rate DOUBLE
        )
    """,
    "mart.mart_reco_erp_gr_vs_wms_receipt": """
        CREATE TABLE IF NOT EXISTS mart.mart_reco_erp_gr_vs_wms_receipt (
            period VARCHAR,
            item_id VARCHAR,
            po_id VARCHAR,
            erp_qty DOUBLE,
            wms_qty DOUBLE,
            delta DOUBLE
        )
    """,
    "mart.mart_reco_settlement_vs_estimated": """
        CREATE TABLE IF NOT EXISTS mart.mart_reco_settlement_vs_estimated (
            period VARCHAR,
            channel_store_id VARCHAR,
            item_id VARCHAR,
            settlement_revenue_krw DOUBLE,
            estimated_revenue_krw DOUBLE,
            delta_krw DOUBLE,
            variance_pct DOUBLE
        )
    """,
    "mart.mart_reco_charges_invoice_vs_allocated": """
        CREATE TABLE IF NOT EXISTS mart.mart_reco_charges_invoice_vs_allocated (
            period VARCHAR,
            charge_type VARCHAR,
            invoice_total DOUBLE,
            allocated_total DOUBLE,
            delta DOUBLE,
            tied BOOLEAN
        )
    """,
    # -- Constraint marts --
    "mart.mart_constraint_signals": """
        CREATE TABLE IF NOT EXISTS mart.mart_constraint_signals (
            signal_id VARCHAR,
            domain VARCHAR,
            metric_name VARCHAR,
            current_value DOUBLE,
            threshold_value DOUBLE,
            severity VARCHAR,
            entity_type VARCHAR,
            entity_id VARCHAR,
            period VARCHAR,
            detected_at TIMESTAMP
        )
    """,
    "mart.mart_constraint_root_cause": """
        CREATE TABLE IF NOT EXISTS mart.mart_constraint_root_cause (
            signal_id VARCHAR,
            root_cause VARCHAR,
            contributing_factors VARCHAR,
            domain VARCHAR,
            period VARCHAR
        )
    """,
    "mart.mart_constraint_action_plan": """
        CREATE TABLE IF NOT EXISTS mart.mart_constraint_action_plan (
            signal_id VARCHAR,
            action VARCHAR,
            priority VARCHAR,
            responsible VARCHAR,
            domain VARCHAR,
            period VARCHAR
        )
    """,
    "mart.mart_constraint_effectiveness": """
        CREATE TABLE IF NOT EXISTS mart.mart_constraint_effectiveness (
            signal_id VARCHAR,
            metric_name VARCHAR,
            before_value DOUBLE,
            after_value DOUBLE,
            delta DOUBLE,
            resolved BOOLEAN,
            period VARCHAR
        )
    """,
    # -- Coverage + Allocation --
    "mart.mart_coverage_period": """
        CREATE TABLE IF NOT EXISTS mart.mart_coverage_period (
            period VARCHAR,
            domain VARCHAR,
            coverage_rate DOUBLE,
            included_rows BIGINT,
            missing_rows BIGINT,
            severity VARCHAR,
            is_closed_period BOOLEAN
        )
    """,
    "mart.mart_charge_allocated": """
        CREATE TABLE IF NOT EXISTS mart.mart_charge_allocated (
            period VARCHAR,
            charge_type VARCHAR,
            charge_domain VARCHAR,
            cost_stage VARCHAR,
            invoice_no VARCHAR,
            invoice_line_no BIGINT,
            item_id VARCHAR,
            warehouse_id VARCHAR,
            channel_store_id VARCHAR,
            lot_id VARCHAR,
            allocation_basis VARCHAR,
            basis_value DOUBLE,
            allocated_amount DOUBLE,
            allocated_amount_krw DOUBLE,
            currency VARCHAR,
            capitalizable_flag BOOLEAN
        )
    """,
}

# ================================================================
# OPS LAYER
# ================================================================
OPS_TABLES = {
    "ops.ops_issue_log": """
        CREATE TABLE IF NOT EXISTS ops.ops_issue_log (
            issue_id VARCHAR PRIMARY KEY,
            issue_type VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            domain VARCHAR,
            entity_type VARCHAR,
            entity_id VARCHAR,
            period VARCHAR,
            detail VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp,
            resolved_at TIMESTAMP,
            resolved_by VARCHAR,
            action_taken VARCHAR
        )
    """,
    "ops.ops_period_close": """
        CREATE TABLE IF NOT EXISTS ops.ops_period_close (
            period VARCHAR PRIMARY KEY,
            closed_at TIMESTAMP,
            closed_by VARCHAR,
            lock_flag BOOLEAN NOT NULL DEFAULT false,
            notes VARCHAR
        )
    """,
    "ops.ops_adjustment_log": """
        CREATE TABLE IF NOT EXISTS ops.ops_adjustment_log (
            adjustment_id BIGINT PRIMARY KEY,
            period VARCHAR NOT NULL,
            table_name VARCHAR NOT NULL,
            business_key VARCHAR NOT NULL,
            field_name VARCHAR NOT NULL,
            old_value VARCHAR,
            new_value VARCHAR,
            reason VARCHAR,
            adjusted_by VARCHAR,
            adjusted_at TIMESTAMP DEFAULT current_timestamp,
            batch_id BIGINT
        )
    """,
    "ops.ops_snapshot": """
        CREATE TABLE IF NOT EXISTS ops.ops_snapshot (
            snapshot_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT current_timestamp,
            label VARCHAR,
            batch_id BIGINT
        )
    """,
}


def get_connection(path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    """Create all schemas and tables idempotently."""
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    all_tables = {}
    all_tables.update(RAW_TABLES)
    all_tables.update(CORE_DIM_TABLES)
    all_tables.update(CORE_FACT_TABLES)
    all_tables.update(MART_TABLES)
    all_tables.update(OPS_TABLES)

    for ddl in all_tables.values():
        con.execute(ddl)

    # Migrate: add coverage_flag to existing mart tables that lack it.
    # Uses information_schema to check column existence (fail-loud if table missing).
    _COVERAGE_FLAG_TABLES = [
        "mart.mart_pnl_cogs",
        "mart.mart_pnl_gross_margin",
        "mart.mart_pnl_contribution",
        "mart.mart_pnl_operating_profit",
    ]
    for tbl in _COVERAGE_FLAG_TABLES:
        schema_name, tbl_name = tbl.split(".")
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = 'coverage_flag'",
            [schema_name, tbl_name],
        ).fetchone()[0]
        if not exists:
            con.execute(f"ALTER TABLE {tbl} ADD COLUMN coverage_flag VARCHAR")

    # Seed batch lock row if not exists
    con.execute("""
        INSERT INTO raw.system_batch_lock (lock_id, locked, pid, started_at)
        SELECT 1, false, NULL, NULL
        WHERE NOT EXISTS (SELECT 1 FROM raw.system_batch_lock WHERE lock_id = 1)
    """)


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check if a table exists in the given schema."""
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table]
    ).fetchone()
    return result[0] > 0


def get_table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    """Get column names for a table."""
    result = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, table]
    ).fetchall()
    return [r[0] for r in result]


def get_row_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Get row counts for all tables across all schemas."""
    counts = {}
    for schema in SCHEMAS:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [schema]
        ).fetchall()
        for (tbl,) in tables:
            full_name = f"{schema}.{tbl}"
            cnt = con.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
            counts[full_name] = cnt
    return counts
