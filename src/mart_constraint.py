"""Constraint/bottleneck detection and action planning.

Scans all domains, emits signals when metrics exceed thresholds.
CRITICAL signals auto-create ops_issue_log entries.
"""
import uuid
from datetime import datetime, timezone

import duckdb
import polars as pl

from src.config import AppConfig


def _gen_signal_id() -> str:
    return f"SIG-{uuid.uuid4().hex[:8].upper()}"


def compute_supply_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Detect supply-side constraints: late PO, supplier delay."""
    signals = []
    thresholds = config.thresholds.get("constraints", {}).get("supply", {})
    late_ratio_th = thresholds.get("late_po_ratio_high", 0.2)
    delay_days_th = thresholds.get("supplier_delay_days_high", 7)

    try:
        # Late PO ratio per supplier
        df = con.execute("""
            SELECT
                supplier_id,
                STRFTIME(po_date, '%Y-%m') as period,
                COUNT(*) as total_po,
                SUM(CASE WHEN eta_date IS NOT NULL AND eta_date < CURRENT_DATE
                         AND po_id NOT IN (SELECT DISTINCT po_id FROM core.fact_receipt WHERE po_id IS NOT NULL)
                         THEN 1 ELSE 0 END) as late_po
            FROM core.fact_po
            GROUP BY supplier_id, period
        """).fetchall()

        for supplier_id, period, total_po, late_po in df:
            if total_po == 0:
                continue
            ratio = late_po / total_po
            if ratio >= late_ratio_th:
                severity = "CRITICAL" if ratio >= late_ratio_th * 1.5 else "HIGH"
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "supply",
                    "metric_name": "late_po_ratio",
                    "current_value": round(ratio, 4),
                    "threshold_value": late_ratio_th,
                    "severity": severity,
                    "entity_type": "supplier",
                    "entity_id": supplier_id,
                    "period": period,
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    try:
        # Supplier delay days
        df = con.execute("""
            SELECT
                p.supplier_id,
                STRFTIME(p.po_date, '%Y-%m') as period,
                AVG(DATEDIFF('day', p.eta_date, r.receipt_date)) as avg_delay
            FROM core.fact_po p
            JOIN core.fact_receipt r ON p.po_id = r.po_id AND p.item_id = r.item_id
            WHERE p.eta_date IS NOT NULL
            GROUP BY p.supplier_id, period
        """).fetchall()

        for supplier_id, period, avg_delay in df:
            if avg_delay and avg_delay >= delay_days_th:
                severity = "CRITICAL" if avg_delay >= delay_days_th * 2 else "HIGH"
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "supply",
                    "metric_name": "supplier_delay_days",
                    "current_value": round(float(avg_delay), 1),
                    "threshold_value": float(delay_days_th),
                    "severity": severity,
                    "entity_type": "supplier",
                    "entity_id": supplier_id,
                    "period": period,
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    return signals


def compute_warehouse_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Detect warehouse/3PL constraints: backlog, throughput."""
    signals = []
    thresholds = config.thresholds.get("constraints", {}).get("warehouse_3pl", {})
    backlog_th = thresholds.get("backlog_ship_orders_high", 200)

    try:
        # Orders not yet shipped (backlog)
        df = con.execute("""
            SELECT
                COALESCE(o.ship_from_warehouse_id, 'UNKNOWN') as warehouse_id,
                STRFTIME(o.order_date, '%Y-%m') as period,
                COUNT(*) as backlog_count
            FROM core.fact_order o
            LEFT JOIN core.fact_shipment s
                ON o.channel_order_id = s.channel_order_id AND o.item_id = s.item_id
            WHERE s.shipment_id IS NULL
            GROUP BY warehouse_id, period
        """).fetchall()

        for warehouse_id, period, backlog in df:
            if backlog >= backlog_th:
                severity = "CRITICAL" if backlog >= backlog_th * 2 else "HIGH"
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "warehouse_3pl",
                    "metric_name": "backlog_ship_orders",
                    "current_value": float(backlog),
                    "threshold_value": float(backlog_th),
                    "severity": severity,
                    "entity_type": "warehouse",
                    "entity_id": warehouse_id,
                    "period": period,
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    return signals


def compute_logistics_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Detect logistics/customs constraints: dwell time."""
    signals = []
    thresholds = config.thresholds.get("constraints", {}).get("logistics_customs", {})
    dwell_th = thresholds.get("dwell_time_days_high", 5)

    try:
        # Average days between receipt and PO eta
        df = con.execute("""
            SELECT
                r.warehouse_id,
                STRFTIME(r.receipt_date, '%Y-%m') as period,
                AVG(DATEDIFF('day', p.eta_date, r.receipt_date)) as avg_dwell
            FROM core.fact_receipt r
            JOIN core.fact_po p ON r.po_id = p.po_id AND r.item_id = p.item_id
            WHERE p.eta_date IS NOT NULL
            GROUP BY r.warehouse_id, period
        """).fetchall()

        for warehouse_id, period, avg_dwell in df:
            if avg_dwell and avg_dwell >= dwell_th:
                severity = "HIGH"
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "logistics_customs",
                    "metric_name": "dwell_time_days",
                    "current_value": round(float(avg_dwell), 1),
                    "threshold_value": float(dwell_th),
                    "severity": severity,
                    "entity_type": "warehouse",
                    "entity_id": warehouse_id,
                    "period": period,
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    return signals


def compute_demand_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Detect demand/channel constraints: return spikes."""
    signals = []
    thresholds = config.thresholds.get("constraints", {}).get("demand_channel", {})
    return_spike_th = thresholds.get("return_rate_spike_ratio_high", 1.5)

    try:
        df = con.execute("""
            SELECT
                COALESCE(r.channel_order_id, 'UNKNOWN') as channel,
                STRFTIME(r.return_date, '%Y-%m') as period,
                SUM(r.qty_returned) as total_returns,
                COALESCE(s.total_shipped, 1) as total_shipped,
                SUM(r.qty_returned) / COALESCE(s.total_shipped, 1) as return_rate
            FROM core.fact_return r
            LEFT JOIN (
                SELECT STRFTIME(ship_date, '%Y-%m') as period, SUM(qty_shipped) as total_shipped
                FROM core.fact_shipment GROUP BY 1
            ) s ON STRFTIME(r.return_date, '%Y-%m') = s.period
            GROUP BY channel, period, s.total_shipped
        """).fetchall()

        for channel, period, returns, shipped, rate in df:
            if rate and rate >= return_spike_th:
                severity = "HIGH"
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "demand_channel",
                    "metric_name": "return_rate_spike",
                    "current_value": round(float(rate), 4),
                    "threshold_value": float(return_spike_th),
                    "severity": severity,
                    "entity_type": "channel",
                    "entity_id": channel,
                    "period": period,
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    return signals


def compute_finance_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Detect finance constraints: overstock/expiry value at risk."""
    signals = []

    try:
        # Expired value at risk
        df = con.execute("""
            SELECT
                item_id, warehouse_id,
                SUM(expired_qty) as total_expired,
                snapshot_date as period
            FROM mart.mart_inventory_onhand
            WHERE expired_qty > 0
            GROUP BY item_id, warehouse_id, snapshot_date
        """).fetchall()

        for item_id, warehouse_id, expired, period in df:
            if expired > 0:
                signals.append({
                    "signal_id": _gen_signal_id(),
                    "domain": "finance",
                    "metric_name": "expired_value_at_risk",
                    "current_value": float(expired),
                    "threshold_value": 0.0,
                    "severity": "CRITICAL",
                    "entity_type": "item",
                    "entity_id": f"{item_id}|{warehouse_id}",
                    "period": str(period),
                    "detected_at": datetime.now(timezone.utc),
                })
    except Exception:
        pass

    return signals


def build_mart_constraint_signals(con: duckdb.DuckDBPyConnection, config: AppConfig) -> list[dict]:
    """Build constraint signals mart and return all signals."""
    con.execute("DELETE FROM mart.mart_constraint_signals")

    all_signals = []
    all_signals.extend(compute_supply_signals(con, config))
    all_signals.extend(compute_warehouse_signals(con, config))
    all_signals.extend(compute_logistics_signals(con, config))
    all_signals.extend(compute_demand_signals(con, config))
    all_signals.extend(compute_finance_signals(con, config))

    if all_signals:
        df = pl.DataFrame(all_signals)
        arrow = df.to_arrow()
        con.register("_sig_staging", arrow)
        con.execute("INSERT INTO mart.mart_constraint_signals SELECT * FROM _sig_staging")
        con.unregister("_sig_staging")

        # Auto-create ops_issue_log for CRITICAL signals
        for sig in all_signals:
            if sig["severity"] == "CRITICAL":
                try:
                    con.execute(
                        "INSERT INTO ops.ops_issue_log "
                        "(issue_id, issue_type, severity, domain, entity_type, entity_id, period, detail) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [sig["signal_id"], f"CONSTRAINT_{sig['metric_name']}",
                         sig["severity"], sig["domain"], sig["entity_type"],
                         sig["entity_id"], sig["period"],
                         f"{sig['metric_name']}={sig['current_value']} exceeds threshold {sig['threshold_value']}"]
                    )
                except Exception:
                    pass

    return all_signals


def build_mart_constraint_root_cause(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build root cause mart from signals."""
    con.execute("DELETE FROM mart.mart_constraint_root_cause")

    # Rule-based root cause mapping
    cause_map = {
        "late_po_ratio": ("Supplier delivery performance below target", "Late shipments, capacity constraints"),
        "supplier_delay_days": ("Supplier production delays", "Lead time variability, raw material shortage"),
        "backlog_ship_orders": ("Warehouse processing capacity exceeded", "Staffing shortage, space constraints"),
        "dwell_time_days": ("Port/customs clearance delays", "Documentation issues, inspection holds"),
        "return_rate_spike": ("Product quality or fulfillment errors", "Damaged goods, wrong items shipped"),
        "expired_value_at_risk": ("Inventory aging beyond shelf life", "Over-ordering, demand forecast error"),
    }

    try:
        signals = con.execute(
            "SELECT signal_id, domain, metric_name, period FROM mart.mart_constraint_signals "
            "WHERE severity IN ('CRITICAL', 'HIGH')"
        ).fetchall()

        rows = []
        for signal_id, domain, metric, period in signals:
            cause_info = cause_map.get(metric, ("Unknown root cause", "Requires investigation"))
            rows.append({
                "signal_id": signal_id,
                "root_cause": cause_info[0],
                "contributing_factors": cause_info[1],
                "domain": domain,
                "period": period,
            })

        if rows:
            df = pl.DataFrame(rows)
            arrow = df.to_arrow()
            con.register("_rc_staging", arrow)
            con.execute("INSERT INTO mart.mart_constraint_root_cause SELECT * FROM _rc_staging")
            con.unregister("_rc_staging")
    except Exception:
        pass


def build_mart_constraint_action_plan(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build action plan mart from root causes."""
    con.execute("DELETE FROM mart.mart_constraint_action_plan")

    action_map = {
        "late_po_ratio": ("Escalate to procurement; evaluate alternate suppliers", "HIGH", "Procurement"),
        "supplier_delay_days": ("Negotiate expedited shipping; increase safety stock", "HIGH", "Supply Chain"),
        "backlog_ship_orders": ("Add temporary labor; prioritize by SLA", "CRITICAL", "Warehouse Ops"),
        "dwell_time_days": ("Pre-clear documentation; engage customs broker", "MEDIUM", "Logistics"),
        "return_rate_spike": ("Root cause analysis on returns; quality audit", "HIGH", "Quality/CS"),
        "expired_value_at_risk": ("Markdown/donation plan; adjust reorder parameters", "CRITICAL", "Planning"),
    }

    try:
        signals = con.execute(
            "SELECT signal_id, domain, metric_name, period FROM mart.mart_constraint_signals"
        ).fetchall()

        rows = []
        for signal_id, domain, metric, period in signals:
            action_info = action_map.get(metric, ("Investigate and develop mitigation plan", "MEDIUM", "Operations"))
            rows.append({
                "signal_id": signal_id,
                "action": action_info[0],
                "priority": action_info[1],
                "responsible": action_info[2],
                "domain": domain,
                "period": period,
            })

        if rows:
            df = pl.DataFrame(rows)
            arrow = df.to_arrow()
            con.register("_ap_staging", arrow)
            con.execute("INSERT INTO mart.mart_constraint_action_plan SELECT * FROM _ap_staging")
            con.unregister("_ap_staging")
    except Exception:
        pass


def build_mart_constraint_effectiveness(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Build effectiveness mart: before/after for resolved issues."""
    con.execute("DELETE FROM mart.mart_constraint_effectiveness")

    try:
        # Check if any issues are resolved
        resolved = con.execute("""
            SELECT i.issue_id, s.metric_name, s.current_value, i.period
            FROM ops.ops_issue_log i
            JOIN mart.mart_constraint_signals s ON i.issue_id = s.signal_id
            WHERE i.resolved_at IS NOT NULL
        """).fetchall()

        rows = []
        for issue_id, metric, before_val, period in resolved:
            rows.append({
                "signal_id": issue_id,
                "metric_name": metric,
                "before_value": float(before_val),
                "after_value": 0.0,
                "delta": -float(before_val),
                "resolved": True,
                "period": period,
            })

        if rows:
            df = pl.DataFrame(rows)
            arrow = df.to_arrow()
            con.register("_eff_staging", arrow)
            con.execute("INSERT INTO mart.mart_constraint_effectiveness SELECT * FROM _eff_staging")
            con.unregister("_eff_staging")
    except Exception:
        pass


def build_all_constraint_marts(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Orchestrate all constraint marts in dependency order."""
    build_mart_constraint_signals(con, config)
    build_mart_constraint_root_cause(con, config)
    build_mart_constraint_action_plan(con, config)
    build_mart_constraint_effectiveness(con, config)
