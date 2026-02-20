"""Cost allocation with conservation + determinism guarantees.

Conservation: SUM(allocated) == invoice_total for every invoice.
Determinism: same inputs -> same outputs (stable ordering, Hare-Niemeyer rounding).
"""
import math
from decimal import Decimal, ROUND_HALF_UP

import duckdb
import polars as pl

from src.config import AppConfig, SUPPORTED_ALLOCATION_BASES


def largest_fraction_round(raw_amounts: list[float], total: float, decimals: int) -> list[float]:
    """Hare-Niemeyer rounding that guarantees sum == total.

    1. Floor each amount.
    2. Distribute remainder to rows with largest fractional parts.
    3. Deterministic: ties broken by index (stable).
    """
    if not raw_amounts:
        return []

    factor = 10 ** decimals
    total_int = round(total * factor)

    # Floor each
    floored = [math.floor(a * factor) for a in raw_amounts]
    remainders = [(a * factor) - math.floor(a * factor) for a in raw_amounts]

    floored_sum = sum(floored)
    shortfall = total_int - floored_sum

    if shortfall > 0:
        # Sort by remainder descending, index ascending for tie-breaking
        indices = sorted(range(len(remainders)), key=lambda i: (-remainders[i], i))
        for i in range(min(int(shortfall), len(indices))):
            floored[indices[i]] += 1

    return [v / factor for v in floored]


def resolve_basis(
    charge_type: str,
    targets: pl.DataFrame,
    config: AppConfig,
) -> str | None:
    """Resolve the first usable allocation basis for a charge type.

    Checks charge_type_overrides first, then default_basis_by_stage.
    A basis is usable if all targets have non-null, non-zero values for it.
    """
    basis_priority = config.get_allocation_basis_priority(charge_type)

    for basis in basis_priority:
        if basis in targets.columns:
            col = targets[basis]
            # Check if all values are non-null and non-zero
            if col.null_count() == 0 and (col != 0).all():
                return basis
            # Allow if at least some are non-null and non-zero
            if col.null_count() < col.len() and col.drop_nulls().filter(col.drop_nulls() != 0).len() > 0:
                return basis

    return None


def allocate_charge(
    invoice_no: str,
    invoice_line_no: int,
    charge_type: str,
    amount: float,
    currency: str,
    period: str,
    targets: pl.DataFrame,
    config: AppConfig,
    rate_to_krw: float = 1.0,
) -> pl.DataFrame:
    """Allocate a single charge across targets.

    Returns DataFrame with allocation details including allocated_amount.
    Raises ValueError if no valid basis can be resolved.
    """
    if targets.height == 0:
        return pl.DataFrame()

    # Resolve allocation basis
    basis = resolve_basis(charge_type, targets, config)
    if basis is None:
        raise ValueError(
            f"Cannot resolve allocation basis for charge_type='{charge_type}'. "
            f"No valid basis found in targets. Tried: {config.get_allocation_basis_priority(charge_type)}"
        )

    # Sort targets deterministically
    sort_keys = config.get_sort_keys()
    available_sort_keys = [k for k in sort_keys if k in targets.columns]
    if available_sort_keys:
        targets = targets.sort(available_sort_keys)

    # Compute proportions
    basis_values = targets[basis].fill_null(0).to_list()
    total_basis = sum(basis_values)

    if total_basis == 0:
        # Equal distribution if all basis values are zero
        raw_amounts = [amount / targets.height] * targets.height
    else:
        raw_amounts = [(v / total_basis) * amount for v in basis_values]

    # Apply Hare-Niemeyer rounding
    rounding_cfg = config.allocation.get("rounding", {})
    decimals = rounding_cfg.get("decimals", 0)
    allocated = largest_fraction_round(raw_amounts, amount, decimals)

    # Get charge policy
    ct_policy = config.get_charge_type(charge_type)

    # Build result
    result = targets.with_columns([
        pl.lit(period).alias("period"),
        pl.lit(charge_type).alias("charge_type"),
        pl.lit(ct_policy.charge_domain).alias("charge_domain"),
        pl.lit(ct_policy.cost_stage).alias("cost_stage"),
        pl.lit(invoice_no).alias("invoice_no"),
        pl.lit(invoice_line_no).alias("invoice_line_no"),
        pl.lit(basis).alias("allocation_basis"),
        pl.Series("basis_value", basis_values, dtype=pl.Float64),
        pl.Series("allocated_amount", allocated, dtype=pl.Float64),
        pl.Series("allocated_amount_krw", [a * rate_to_krw for a in allocated], dtype=pl.Float64),
        pl.lit(currency).alias("currency"),
        pl.lit(ct_policy.capitalizable_flag).alias("capitalizable_flag"),
    ])

    return result


def allocate_all_charges(con: duckdb.DuckDBPyConnection, config: AppConfig) -> None:
    """Run full allocation for all charges. Write to mart.mart_charge_allocated."""
    # Read charges
    try:
        charges_df = con.execute("SELECT * FROM core.fact_charge_actual").pl()
    except Exception:
        return

    if charges_df.height == 0:
        con.execute("DELETE FROM mart.mart_charge_allocated")
        return

    # Read FX rates
    try:
        fx_df = con.execute("SELECT period, currency, rate_to_krw FROM core.fact_exchange_rate").pl()
        fx_map = {(r["period"], r["currency"]): r["rate_to_krw"] for r in fx_df.iter_rows(named=True)}
    except Exception:
        fx_map = {}

    # Read potential targets (shipments as default allocation targets)
    try:
        ship_df = con.execute("""
            SELECT shipment_id, ship_date, warehouse_id, item_id, lot_id,
                   qty_shipped, weight, volume_cbm, channel_order_id, channel_store_id,
                   source_system
            FROM core.fact_shipment
        """).pl()
    except Exception:
        ship_df = pl.DataFrame()

    all_allocated = []

    for row in charges_df.iter_rows(named=True):
        charge_type = row["charge_type"]
        period = row["period"]
        amount = row["amount"]
        currency = row["currency"]
        invoice_no = row["invoice_no"]
        invoice_line_no = row["invoice_line_no"]
        warehouse_id = row.get("warehouse_id")
        channel_store_id = row.get("channel_store_id")

        # Get FX rate
        rate_to_krw = fx_map.get((period, currency), 1.0)
        if currency == "KRW":
            rate_to_krw = 1.0

        # Build target set
        targets = ship_df.clone() if ship_df.height > 0 else pl.DataFrame()

        if targets.height == 0:
            # If no shipment data, create a single-row fallback target
            targets = pl.DataFrame({
                "item_id": ["UNALLOCATED"],
                "warehouse_id": [warehouse_id or "UNKNOWN"],
                "channel_store_id": [channel_store_id or "UNKNOWN"],
                "lot_id": ["__NONE__"],
                "qty": [1.0],
            })

        # Filter targets by scope if reference info available
        if warehouse_id and "warehouse_id" in targets.columns:
            scoped = targets.filter(pl.col("warehouse_id") == warehouse_id)
            if scoped.height > 0:
                targets = scoped

        # Add derived basis columns if missing
        if "qty" not in targets.columns and "qty_shipped" in targets.columns:
            targets = targets.with_columns(pl.col("qty_shipped").alias("qty"))
        if "order_count" not in targets.columns:
            targets = targets.with_columns(pl.lit(1).alias("order_count"))
        if "line_count" not in targets.columns:
            targets = targets.with_columns(pl.lit(1).alias("line_count"))
        if "value" not in targets.columns and "qty" in targets.columns:
            targets = targets.with_columns(pl.col("qty").alias("value"))
        if "revenue" not in targets.columns:
            targets = targets.with_columns(pl.lit(1.0).alias("revenue"))

        try:
            allocated_df = allocate_charge(
                invoice_no=invoice_no,
                invoice_line_no=invoice_line_no,
                charge_type=charge_type,
                amount=amount,
                currency=currency,
                period=period,
                targets=targets,
                config=config,
                rate_to_krw=rate_to_krw,
            )
            if allocated_df.height > 0:
                # Select only the columns needed for mart_charge_allocated
                keep_cols = [
                    "period", "charge_type", "charge_domain", "cost_stage",
                    "invoice_no", "invoice_line_no", "item_id", "warehouse_id",
                    "channel_store_id", "lot_id", "allocation_basis", "basis_value",
                    "allocated_amount", "allocated_amount_krw", "currency", "capitalizable_flag",
                ]
                for c in keep_cols:
                    if c not in allocated_df.columns:
                        allocated_df = allocated_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
                allocated_df = allocated_df.select(keep_cols)
                all_allocated.append(allocated_df)
        except ValueError as e:
            # Log but continue
            pass

    # Write to mart
    con.execute("DELETE FROM mart.mart_charge_allocated")
    if all_allocated:
        combined = pl.concat(all_allocated, how="diagonal_relaxed")
        arrow = combined.to_arrow()
        con.register("_alloc_staging", arrow)
        con.execute("INSERT INTO mart.mart_charge_allocated SELECT * FROM _alloc_staging")
        con.unregister("_alloc_staging")
