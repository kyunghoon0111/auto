"""Main entry point for SCM analytics pipeline.

Supports: --init, --once, --dry-run, --status, --unlock, --rollback N
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent))

from src.db import get_connection, init_db, get_row_counts, DB_PATH
from src.config import AppConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def seed_dim_charge_policy(con, config: AppConfig) -> None:
    """Populate dim_charge_policy from charge_policy.yaml."""
    con.execute("DELETE FROM core.dim_charge_policy")
    for ct_name, ct_policy in config.charge_policy.items():
        con.execute(
            "INSERT INTO core.dim_charge_policy "
            "(charge_type, charge_domain, cost_stage, capitalizable_flag, default_allocation_basis, severity_if_missing) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [ct_name, ct_policy.charge_domain, ct_policy.cost_stage,
             ct_policy.capitalizable_flag, ct_policy.default_allocation_basis,
             ct_policy.severity_if_missing]
        )


def acquire_lock(con) -> int:
    """Acquire batch lock. Returns batch_id."""
    lock = con.execute("SELECT locked, pid FROM raw.system_batch_lock WHERE lock_id = 1").fetchone()
    if lock and lock[0]:
        raise RuntimeError(
            f"Pipeline is locked by PID {lock[1]}. "
            f"If the previous run crashed, use: python run.py --unlock"
        )

    now = datetime.now(timezone.utc)
    pid = os.getpid()
    con.execute(
        "UPDATE raw.system_batch_lock SET locked = true, pid = ?, started_at = ? WHERE lock_id = 1",
        [pid, now]
    )

    # Get next batch_id
    result = con.execute("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM raw.system_batch_log").fetchone()
    batch_id = result[0]

    con.execute(
        "INSERT INTO raw.system_batch_log (batch_id, started_at, status) VALUES (?, ?, 'running')",
        [batch_id, now]
    )

    return batch_id


def release_lock(con, batch_id: int, status: str = "success", error: str | None = None) -> None:
    """Release batch lock and finalize batch log."""
    now = datetime.now(timezone.utc)
    con.execute(
        "UPDATE raw.system_batch_lock SET locked = false, pid = NULL, started_at = NULL WHERE lock_id = 1"
    )
    con.execute(
        "UPDATE raw.system_batch_log SET finished_at = ?, status = ?, error_msg = ? WHERE batch_id = ?",
        [now, status, error, batch_id]
    )


def unlock_batch(con) -> None:
    """Force-unlock the batch lock (crash recovery)."""
    con.execute(
        "UPDATE raw.system_batch_lock SET locked = false, pid = NULL, started_at = NULL WHERE lock_id = 1"
    )
    logger.info("Batch lock released.")


def run_pipeline(con, config: AppConfig, dry_run: bool = False) -> None:
    """Full ETL + mart build cycle."""
    from src.ingest import ingest_all
    from src.mart_scm import build_all_scm_marts
    from src.mart_pnl import build_all_pnl_marts
    from src.mart_reco import build_all_reco_marts
    from src.mart_constraint import build_all_constraint_marts
    from src.allocation import allocate_all_charges
    from src.coverage import compute_coverage

    batch_id = acquire_lock(con)
    logger.info(f"Pipeline started. Batch ID: {batch_id}, dry_run={dry_run}")

    try:
        # 1. Ingest files from inbox/
        logger.info("=== PHASE 1: Ingestion ===")
        results = ingest_all(con, config, batch_id=batch_id, dry_run=dry_run)
        success_count = sum(1 for r in results if r["status"] == "success")
        total_rows = sum(r["rows"] for r in results)
        logger.info(f"Ingestion complete: {success_count}/{len(results)} files, {total_rows} rows")

        for r in results:
            if r["status"] not in ("success", "skipped"):
                logger.warning(f"  FAILED: {r['file']} - {r['error']}")

        # Update batch log
        con.execute(
            "UPDATE raw.system_batch_log SET file_count = ?, rows_ingested = ? WHERE batch_id = ?",
            [len(results), total_rows, batch_id]
        )

        # 2. Build SCM marts
        logger.info("=== PHASE 2: SCM Marts ===")
        build_all_scm_marts(con, config)

        # 3. Run allocation
        logger.info("=== PHASE 3: Cost Allocation ===")
        allocate_all_charges(con, config)

        # 4. Build P&L marts
        logger.info("=== PHASE 4: P&L Marts ===")
        build_all_pnl_marts(con, config)

        # 5. Build reconciliation marts
        logger.info("=== PHASE 5: Reconciliation Marts ===")
        build_all_reco_marts(con, config)

        # 6. Build constraint marts
        logger.info("=== PHASE 6: Constraint Detection ===")
        build_all_constraint_marts(con, config)

        # 7. Compute coverage
        logger.info("=== PHASE 7: Coverage Reporting ===")
        compute_coverage(con, config)

        if dry_run:
            logger.info("Dry run complete. Results NOT persisted (no rollback needed for read-based marts).")

        release_lock(con, batch_id, status="success")
        logger.info(f"Pipeline completed successfully. Batch ID: {batch_id}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        release_lock(con, batch_id, status="failed", error=str(e))
        raise


def print_status(con, config: AppConfig) -> None:
    """Print pipeline status: last batch, table counts, DQ summary."""
    print("\n" + "=" * 60)
    print("SCM Analytics Pipeline Status")
    print("=" * 60)

    # Last batch
    try:
        batch = con.execute(
            "SELECT batch_id, started_at, finished_at, status, file_count, rows_ingested, error_msg "
            "FROM raw.system_batch_log ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()
        if batch:
            print(f"\nLast Batch:")
            print(f"  ID: {batch[0]}")
            print(f"  Started: {batch[1]}")
            print(f"  Finished: {batch[2]}")
            print(f"  Status: {batch[3]}")
            print(f"  Files: {batch[4]}")
            print(f"  Rows: {batch[5]}")
            if batch[6]:
                print(f"  Error: {batch[6]}")
        else:
            print("\nNo batches found. Run: python run.py --init && python run.py --once")
    except Exception:
        print("\nDatabase not initialized. Run: python run.py --init")
        return

    # Lock status
    try:
        lock = con.execute("SELECT locked, pid FROM raw.system_batch_lock WHERE lock_id = 1").fetchone()
        if lock:
            print(f"\nLock: {'LOCKED (PID: ' + str(lock[1]) + ')' if lock[0] else 'unlocked'}")
    except Exception:
        pass

    # Table row counts
    print("\nTable Row Counts:")
    try:
        counts = get_row_counts(con)
        for tbl, cnt in sorted(counts.items()):
            if cnt > 0:
                print(f"  {tbl}: {cnt:,}")
    except Exception as e:
        print(f"  Error: {e}")

    # DQ summary
    print("\nRecent DQ Issues:")
    try:
        dq = con.execute(
            "SELECT check_name, severity, detail FROM raw.system_dq_report "
            "WHERE passed = false ORDER BY checked_at DESC LIMIT 5"
        ).fetchall()
        if dq:
            for row in dq:
                print(f"  [{row[1]}] {row[0]}: {row[2]}")
        else:
            print("  No DQ issues found.")
    except Exception:
        pass

    # Period close status
    print("\nClosed Periods:")
    try:
        from src.period_close import get_closed_periods
        closed = get_closed_periods(con)
        if closed:
            for p in closed:
                print(f"  {p}")
        else:
            print("  No closed periods.")
    except Exception:
        pass

    # Coverage summary
    print("\nCoverage Summary:")
    try:
        cov = con.execute(
            "SELECT domain, period, coverage_rate, severity FROM mart.mart_coverage_period "
            "WHERE severity != 'OK' ORDER BY severity, domain LIMIT 10"
        ).fetchall()
        if cov:
            for row in cov:
                print(f"  [{row[3]}] {row[0]} ({row[1]}): {row[2]:.0%}")
        else:
            print("  All domains covered or no data yet.")
    except Exception:
        pass

    print("\n" + "=" * 60)


def rollback_batches(con, config: AppConfig, n: int) -> None:
    """Rollback the last N batches."""
    from src.mart_scm import build_all_scm_marts
    from src.mart_pnl import build_all_pnl_marts
    from src.mart_reco import build_all_reco_marts
    from src.mart_constraint import build_all_constraint_marts
    from src.allocation import allocate_all_charges
    from src.coverage import compute_coverage

    # Get batch IDs to rollback
    batches = con.execute(
        f"SELECT batch_id FROM raw.system_batch_log ORDER BY batch_id DESC LIMIT {n}"
    ).fetchall()

    if not batches:
        logger.info("No batches to rollback.")
        return

    batch_ids = [b[0] for b in batches]
    logger.info(f"Rolling back batches: {batch_ids}")

    # Delete rows from all CORE fact tables with these batch_ids
    core_tables = [
        "fact_order", "fact_shipment", "fact_return", "fact_inventory_snapshot",
        "fact_po", "fact_receipt", "fact_settlement", "fact_charge_actual",
        "fact_exchange_rate", "fact_cost_structure",
    ]

    for tbl in core_tables:
        placeholders = ",".join(["?"] * len(batch_ids))
        try:
            deleted = con.execute(
                f"DELETE FROM core.{tbl} WHERE load_batch_id IN ({placeholders})", batch_ids
            ).fetchone()
            logger.info(f"  Rolled back core.{tbl}")
        except Exception as e:
            logger.warning(f"  Could not rollback core.{tbl}: {e}")

    # Update batch log
    for bid in batch_ids:
        con.execute(
            "UPDATE raw.system_batch_log SET status = 'rolled_back' WHERE batch_id = ?", [bid]
        )

    # Rebuild all marts
    logger.info("Rebuilding all marts...")
    build_all_scm_marts(con, config)
    allocate_all_charges(con, config)
    build_all_pnl_marts(con, config)
    build_all_reco_marts(con, config)
    build_all_constraint_marts(con, config)
    compute_coverage(con, config)

    logger.info(f"Rollback of {len(batch_ids)} batch(es) complete.")


def main():
    parser = argparse.ArgumentParser(description="SCM Analytics Pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--init", action="store_true", help="Initialize database and schemas")
    group.add_argument("--once", action="store_true", help="Run one full ETL + mart cycle")
    group.add_argument("--dry-run", action="store_true", help="Run pipeline without persisting changes")
    group.add_argument("--status", action="store_true", help="Show pipeline status")
    group.add_argument("--unlock", action="store_true", help="Force-unlock batch lock (crash recovery)")
    group.add_argument("--rollback", type=int, metavar="N", help="Rollback last N batches")

    args = parser.parse_args()

    config = AppConfig()
    con = get_connection()

    try:
        if args.init:
            init_db(con)
            seed_dim_charge_policy(con, config)
            logger.info("Database initialized successfully.")
            logger.info(f"Database path: {DB_PATH.resolve()}")

        elif args.once:
            run_pipeline(con, config, dry_run=False)

        elif args.dry_run:
            run_pipeline(con, config, dry_run=True)

        elif args.status:
            print_status(con, config)

        elif args.unlock:
            unlock_batch(con)

        elif args.rollback is not None:
            rollback_batches(con, config, args.rollback)

    finally:
        con.close()


if __name__ == "__main__":
    main()
