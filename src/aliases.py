"""Column alias mapping -- ONLY from config, no hardcoded variants.

All column name variants are defined in config/column_aliases.yaml.
This module builds reverse lookups and applies them to DataFrames.
"""
import polars as pl
from src.config import AppConfig


def build_alias_map(config: AppConfig, table_name: str) -> dict[str, str]:
    """Build {lowercased_alias -> canonical_name} for a given table.

    Applies common aliases first, then table-specific aliases (override on conflict).
    """
    result: dict[str, str] = {}

    # Apply common aliases
    common = config.aliases.get("common", {})
    for canonical, variants in common.items():
        for v in variants:
            result[v.lower().strip()] = canonical

    # Apply table-specific aliases (override common if conflict)
    table_aliases = config.aliases.get(table_name, {})
    for canonical, variants in table_aliases.items():
        for v in variants:
            result[v.lower().strip()] = canonical

    return result


def apply_aliases(df: pl.DataFrame, table_name: str, config: AppConfig) -> pl.DataFrame:
    """Rename DataFrame columns using alias mapping from config.

    Returns a new DataFrame with canonical column names.
    Unknown columns are kept as-is (they will be filtered later by schema validation).
    """
    alias_map = build_alias_map(config, table_name)
    rename_map: dict[str, str] = {}

    for col in df.columns:
        canonical = alias_map.get(col.lower().strip())
        if canonical and canonical != col:
            rename_map[col] = canonical

    if rename_map:
        df = df.rename(rename_map)

    return df
