"""ETL ingestion: detect table type, apply aliases, validate, DQ check, upsert.

Ingestion from inbox/ (CSV/XLSX). Idempotent, transactional, fail-loud.
"""
import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from src.aliases import apply_aliases, build_alias_map
from src.config import AppConfig
from src.dq import DQResult, has_failures, run_all_checks

logger = logging.getLogger(__name__)

INBOX_DIR = Path("inbox")

# Type mapping from schema.yaml type strings to Polars casts
TYPE_CAST_MAP = {
    "VARCHAR": pl.Utf8,
    "BIGINT": pl.Int64,
    "DOUBLE": pl.Float64,
    "DATE": pl.Date,
    "BOOLEAN": pl.Boolean,
}

# Columns that are optional in the business key (use sentinel for NULL)
SENTINEL_VALUE = "__NONE__"


def compute_file_hash(path: Path) -> str:
    """Compute SHA256 hash of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_file(path: Path) -> pl.DataFrame:
    """Read CSV or XLSX file into a Polars DataFrame (all columns as strings)."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path, infer_schema_length=0, encoding="utf8-lossy")
    elif suffix in (".xlsx", ".xls"):
        return pl.read_excel(path, infer_schema_length=0)
    raise ValueError(f"Unsupported file type: {suffix}")


def detect_table_type(df: pl.DataFrame, config: AppConfig) -> str:
    """Score each table in schema.yaml by required_columns overlap. Ties -> FAIL."""
    scores: dict[str, float] = {}

    for table_name, table_schema in config.schema.items():
        # Build alias map for candidate table to check mapped names
        alias_map = build_alias_map(config, table_name)
        mapped_cols: set[str] = set()
        for col in df.columns:
            canonical = alias_map.get(col.lower().strip(), col.lower().strip())
            mapped_cols.add(canonical)

        required = {c.name for c in table_schema.required_columns}
        if len(required) == 0:
            continue
        scores[table_name] = len(required & mapped_cols) / len(required)

    if not scores:
        raise ValueError("No tables defined in schema.yaml")

    ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))

    # Check for ties at the top
    if len(ranked) >= 2 and ranked[0][1] == ranked[1][1]:
        raise ValueError(
            f"Tie in table detection: {ranked[0][0]}={ranked[0][1]:.2f} "
            f"vs {ranked[1][0]}={ranked[1][1]:.2f}. Cannot determine table type."
        )

    if ranked[0][1] < 0.5:
        raise ValueError(
            f"No table matched with score > 0.5. Best: {ranked[0][0]}={ranked[0][1]:.2f}"
        )

    return ranked[0][0]


def cast_columns(df: pl.DataFrame, table_name: str, config: AppConfig) -> pl.DataFrame:
    """Cast DataFrame columns to their declared types from schema.yaml."""
    schema = config.get_schema(table_name)
    all_cols = list(schema.required_columns) + list(schema.optional_columns)

    exprs = []
    for col_def in all_cols:
        if col_def.name not in df.columns:
            continue
        target = TYPE_CAST_MAP.get(col_def.type)
        if target is None:
            continue
        if df[col_def.name].dtype == target:
            continue

        if target == pl.Date:
            exprs.append(pl.col(col_def.name).str.to_date(strict=False).alias(col_def.name))
        elif target == pl.Boolean:
            exprs.append(
                pl.when(pl.col(col_def.name).str.to_lowercase().is_in(["true", "1", "yes"]))
                .then(True)
                .when(pl.col(col_def.name).str.to_lowercase().is_in(["false", "0", "no"]))
                .then(False)
                .otherwise(None)
                .alias(col_def.name)
            )
        else:
            exprs.append(pl.col(col_def.name).cast(target, strict=False).alias(col_def.name))

    if exprs:
        df = df.with_columns(exprs)

    return df


def filter_columns(df: pl.DataFrame, table_name: str, config: AppConfig) -> pl.DataFrame:
    """Keep only columns that are in the schema (required + optional) + system columns."""
    schema = config.get_schema(table_name)
    known_cols = {c.name for c in schema.required_columns} | {c.name for c in schema.optional_columns}
    # Also keep system columns that might already be present
    known_cols.update({"source_system", "source_pk", "load_batch_id", "source_file_hash", "loaded_at"})

    keep = [c for c in df.columns if c in known_cols]
    return df.select(keep)


def add_system_columns(
    df: pl.DataFrame, batch_id: int, file_hash: str, table_name: str, config: AppConfig
) -> pl.DataFrame:
    """Add system columns required for CORE tables."""
    schema = config.get_schema(table_name)

    # Ensure source_system exists
    if "source_system" not in df.columns:
        df = df.with_columns(pl.lit("unknown").alias("source_system"))

    # Ensure source_pk exists
    if "source_pk" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("source_pk"))

    # Add batch tracking columns
    df = df.with_columns([
        pl.lit(batch_id).alias("load_batch_id"),
        pl.lit(file_hash).alias("source_file_hash"),
        pl.lit(datetime.now(timezone.utc)).alias("loaded_at"),
    ])

    # Fill sentinel for optional PK components
    bk_cols = list(schema.business_key)
    for col in bk_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).fill_null(SENTINEL_VALUE).alias(col)
            )

    return df


def get_target_column_order(table_name: str, con: duckdb.DuckDBPyConnection) -> list[str]:
    """Get the column order from the target CORE table."""
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'core' AND table_name = ? ORDER BY ordinal_position",
        [table_name]
    ).fetchall()
    return [c[0] for c in cols]


def upsert_core(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    table_name: str,
    config: AppConfig,
    batch_id: int,
    file_hash: str,
) -> int:
    """Transactional upsert: DELETE matching BK rows, then INSERT."""
    schema = config.get_schema(table_name)
    bk_cols = list(schema.business_key)

    # Get target table column order
    target_cols = get_target_column_order(table_name, con)

    # Ensure df has all target columns (add missing as NULL)
    for col in target_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    # Reorder columns to match target table
    df = df.select(target_cols)

    arrow_table = df.to_arrow()
    con.register("_staging", arrow_table)

    try:
        # Delete existing rows with matching business keys
        bk_where = " AND ".join(f"t.{c} = s.{c}" for c in bk_cols)
        con.execute(f"""
            DELETE FROM core.{table_name} t
            WHERE EXISTS (
                SELECT 1 FROM _staging s WHERE {bk_where}
            )
        """)

        # Insert all rows
        con.execute(f"INSERT INTO core.{table_name} SELECT * FROM _staging")
        row_count = len(df)
    finally:
        con.unregister("_staging")

    return row_count


def is_file_already_loaded(con: duckdb.DuckDBPyConnection, file_hash: str) -> bool:
    """Check if a file with this hash was already loaded successfully."""
    result = con.execute(
        "SELECT COUNT(*) FROM raw.system_file_log WHERE file_hash = ? AND status = 'success'",
        [file_hash]
    ).fetchone()
    return result[0] > 0


def log_file(
    con: duckdb.DuckDBPyConnection,
    batch_id: int,
    file_name: str,
    file_hash: str,
    table_name: str | None,
    row_count: int,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Log file processing result."""
    con.execute(
        "INSERT INTO raw.system_file_log (batch_id, file_name, file_hash, table_name, row_count, status, error_msg) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [batch_id, file_name, file_hash, table_name, row_count, status, error_msg]
    )


def log_dq_results(
    con: duckdb.DuckDBPyConnection,
    batch_id: int,
    file_name: str,
    table_name: str,
    results: list[DQResult],
) -> None:
    """Log DQ check results."""
    for r in results:
        con.execute(
            "INSERT INTO raw.system_dq_report (batch_id, file_name, table_name, check_name, severity, passed, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [batch_id, file_name, table_name, r.check_name, r.severity, r.passed, r.detail]
        )


def process_file(
    con: duckdb.DuckDBPyConnection,
    file_path: Path,
    config: AppConfig,
    batch_id: int,
    dry_run: bool = False,
) -> dict:
    """Process a single file through the ETL pipeline.

    Returns a dict with processing result info.
    """
    file_name = file_path.name
    result = {"file": file_name, "status": "pending", "table": None, "rows": 0, "error": None}

    try:
        # 1. Compute file hash
        file_hash = compute_file_hash(file_path)

        # 2. Check if already loaded
        if is_file_already_loaded(con, file_hash):
            result["status"] = "skipped"
            result["error"] = "File already loaded (same hash)"
            log_file(con, batch_id, file_name, file_hash, None, 0, "skipped", result["error"])
            return result

        # 3. Read file
        df = read_file(file_path)
        if df.height == 0:
            result["status"] = "skipped"
            result["error"] = "Empty file"
            log_file(con, batch_id, file_name, file_hash, None, 0, "skipped", result["error"])
            return result

        # 4. Detect table type
        table_name = detect_table_type(df, config)
        result["table"] = table_name

        # 5. Apply column aliases
        df = apply_aliases(df, table_name, config)

        # 6. Run DQ checks
        dq_results = run_all_checks(df, table_name, config)
        log_dq_results(con, batch_id, file_name, table_name, dq_results)

        if has_failures(dq_results):
            failed = [r for r in dq_results if not r.passed and r.severity in ("CRITICAL", "HIGH")]
            error_detail = "; ".join(r.detail for r in failed)
            result["status"] = "dq_failed"
            result["error"] = error_detail
            log_file(con, batch_id, file_name, file_hash, table_name, 0, "dq_failed", error_detail)
            return result

        # 7. Filter to known columns and cast types
        df = filter_columns(df, table_name, config)
        df = cast_columns(df, table_name, config)

        # 8. Add system columns
        df = add_system_columns(df, batch_id, file_hash, table_name, config)

        # 9. Upsert into CORE
        if not dry_run:
            row_count = upsert_core(con, df, table_name, config, batch_id, file_hash)
        else:
            row_count = df.height

        result["status"] = "success"
        result["rows"] = row_count
        log_file(con, batch_id, file_name, file_hash, table_name, row_count, "success")

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.error(f"Error processing {file_name}: {e}")
        try:
            log_file(con, batch_id, file_name, "", None, 0, "error", str(e))
        except Exception:
            pass

    return result


def ingest_all(
    con: duckdb.DuckDBPyConnection,
    config: AppConfig,
    inbox_dir: Path | None = None,
    dry_run: bool = False,
    batch_id: int = 0,
) -> list[dict]:
    """Ingest all files from inbox/ directory.

    Files are processed in sorted order for determinism.
    """
    inbox = inbox_dir or INBOX_DIR
    if not inbox.exists():
        logger.info(f"Inbox directory does not exist: {inbox}")
        return []

    files = sorted(
        [f for f in inbox.iterdir() if f.is_file() and f.suffix.lower() in (".csv", ".xlsx", ".xls")],
        key=lambda f: f.name
    )

    if not files:
        logger.info("No files found in inbox/")
        return []

    results = []
    for file_path in files:
        r = process_file(con, file_path, config, batch_id, dry_run=dry_run)
        results.append(r)
        logger.info(f"  {r['file']}: {r['status']} ({r['rows']} rows) -> {r['table']}")

    return results
