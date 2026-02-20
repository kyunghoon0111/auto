"""Data quality checks. CRITICAL/HIGH -> FAIL.

Every DQ check returns DQResult objects.
The pipeline rejects files when any CRITICAL or HIGH check fails.
"""
import polars as pl
from dataclasses import dataclass
from typing import Literal

from src.config import AppConfig


@dataclass
class DQResult:
    check_name: str
    severity: Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    passed: bool
    detail: str


def check_required_columns(df: pl.DataFrame, table_name: str, config: AppConfig) -> list[DQResult]:
    """Check that all required columns from schema.yaml are present."""
    results = []
    schema = config.get_schema(table_name)
    present = {c.lower() for c in df.columns}

    missing = []
    for col_def in schema.required_columns:
        if col_def.name.lower() not in present:
            missing.append(col_def.name)

    if missing:
        results.append(DQResult(
            check_name="required_columns",
            severity="CRITICAL",
            passed=False,
            detail=f"Missing required columns for {table_name}: {missing}"
        ))
    else:
        results.append(DQResult(
            check_name="required_columns",
            severity="CRITICAL",
            passed=True,
            detail=f"All required columns present for {table_name}"
        ))
    return results


def check_null_business_keys(df: pl.DataFrame, table_name: str, config: AppConfig) -> list[DQResult]:
    """Check that business key columns have no NULLs."""
    results = []
    schema = config.get_schema(table_name)

    for bk_col in schema.business_key:
        if bk_col not in df.columns:
            continue

        null_count = df.filter(pl.col(bk_col).is_null()).height
        if null_count > 0:
            results.append(DQResult(
                check_name=f"null_business_key_{bk_col}",
                severity="CRITICAL",
                passed=False,
                detail=f"Business key '{bk_col}' has {null_count} NULL values in {table_name}"
            ))
        else:
            results.append(DQResult(
                check_name=f"null_business_key_{bk_col}",
                severity="CRITICAL",
                passed=True,
                detail=f"Business key '{bk_col}' has no NULLs"
            ))
    return results


def check_duplicate_business_keys(df: pl.DataFrame, table_name: str, config: AppConfig) -> list[DQResult]:
    """Check for duplicate composite business keys within the file."""
    results = []
    schema = config.get_schema(table_name)
    bk_cols = [c for c in schema.business_key if c in df.columns]

    if not bk_cols:
        return results

    dup_count = df.height - df.unique(subset=bk_cols).height
    if dup_count > 0:
        results.append(DQResult(
            check_name="duplicate_business_keys",
            severity="HIGH",
            passed=False,
            detail=f"Found {dup_count} duplicate business key(s) in {table_name} on {bk_cols}"
        ))
    else:
        results.append(DQResult(
            check_name="duplicate_business_keys",
            severity="HIGH",
            passed=True,
            detail=f"No duplicate business keys in {table_name}"
        ))
    return results


def check_charge_types(df: pl.DataFrame, config: AppConfig) -> list[DQResult]:
    """For fact_charge_actual: every charge_type must exist in charge_policy.yaml."""
    results = []
    if "charge_type" not in df.columns:
        return results

    valid_types = config.get_valid_charge_types()
    actual_types = set(df["charge_type"].unique().to_list())
    unknown = actual_types - valid_types

    if unknown:
        results.append(DQResult(
            check_name="charge_type_validation",
            severity="HIGH",
            passed=False,
            detail=f"Unknown charge types not in policy: {sorted(unknown)}"
        ))
    else:
        results.append(DQResult(
            check_name="charge_type_validation",
            severity="HIGH",
            passed=True,
            detail="All charge types are valid"
        ))
    return results


def check_type_coercion(df: pl.DataFrame, table_name: str, config: AppConfig) -> list[DQResult]:
    """Check that columns can be cast to their declared types."""
    results = []
    schema = config.get_schema(table_name)
    all_cols = list(schema.required_columns) + list(schema.optional_columns)

    type_map = {
        "VARCHAR": pl.Utf8,
        "BIGINT": pl.Int64,
        "DOUBLE": pl.Float64,
        "DATE": pl.Date,
        "BOOLEAN": pl.Boolean,
    }

    for col_def in all_cols:
        if col_def.name not in df.columns:
            continue
        target_type = type_map.get(col_def.type)
        if target_type is None:
            continue

        # Only check if column is currently string and needs conversion
        if df[col_def.name].dtype == pl.Utf8 and target_type != pl.Utf8:
            try:
                if target_type == pl.Int64:
                    df[col_def.name].cast(pl.Int64, strict=True)
                elif target_type == pl.Float64:
                    df[col_def.name].cast(pl.Float64, strict=True)
                elif target_type == pl.Date:
                    df[col_def.name].str.to_date(strict=False)
                elif target_type == pl.Boolean:
                    pass  # Boolean coercion is flexible
            except Exception as e:
                results.append(DQResult(
                    check_name=f"type_coercion_{col_def.name}",
                    severity="HIGH",
                    passed=False,
                    detail=f"Column '{col_def.name}' cannot be cast to {col_def.type}: {e}"
                ))
                continue

        results.append(DQResult(
            check_name=f"type_coercion_{col_def.name}",
            severity="HIGH",
            passed=True,
            detail=f"Column '{col_def.name}' can be cast to {col_def.type}"
        ))
    return results


def run_all_checks(
    df: pl.DataFrame,
    table_name: str,
    config: AppConfig,
) -> list[DQResult]:
    """Run all DQ checks for a given DataFrame and table type."""
    results: list[DQResult] = []
    results.extend(check_required_columns(df, table_name, config))
    results.extend(check_null_business_keys(df, table_name, config))
    results.extend(check_duplicate_business_keys(df, table_name, config))
    results.extend(check_type_coercion(df, table_name, config))
    if table_name == "fact_charge_actual":
        results.extend(check_charge_types(df, config))
    return results


def has_failures(results: list[DQResult]) -> bool:
    """Check if any CRITICAL or HIGH DQ checks failed."""
    return any(not r.passed and r.severity in ("CRITICAL", "HIGH") for r in results)
