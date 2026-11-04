"""
DuckDB Execution Engine (Pass 2).

This module executes global table-level and cross-column SQL rules.
It mounts the target dataset directly into DuckDB functioning as a transient OLAP compute kernel.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
import pyarrow.dataset as ds

from netra_validator.compiler.duckdb_builder import CompiledSQLRule
from netra_validator.config import SpillConfig
from netra_validator.exceptions import ExecutionEngineError

# Clauses that disqualify a COUNT(*) query from single-pass filter fusion
_NON_FUSIBLE_SQL_CLAUSES = re.compile(
    r"\b(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|SELECT|JOIN|WINDOW|QUALIFY)\b",
    re.IGNORECASE,
)


@dataclass
class SqlValidationResult:
    """The outcome of a single DuckDB SQL rule execution."""

    rule_name: str
    severity: Literal["CRITICAL", "WARNING"]
    passed: bool
    query: str
    actual_value: float | None = None
    error_message: str | None = None


def _mount_virtual_view(
    connection: duckdb.DuckDBPyConnection, file_path: str, table_name: str
) -> None:
    """
    Mounts a physical file as a DuckDB virtual view.
    This avoids loading the entire file into RAM.
    """

    path_object = Path(file_path)

    # Handle Directory Globs (Defaulting to Parquet for directories)
    if path_object.is_dir():
        file_path = f"{file_path}/*.parquet"
        extension = ".parquet"
    elif "*" in str(file_path):
        # Handle raw glob strings like "data/*.csv"
        extension = f".{str(file_path).split('.')[-1].lower()}"
    else:
        extension = path_object.suffix.lower()

    try:
        if extension == ".parquet":
            connection.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}');"
            )
        elif extension == ".csv":
            connection.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{file_path}');"
            )
        elif extension in {".ipc", ".arrow"}:
            # Zero-copy out-of-core scan via PyArrow Dataset and the Arrow C Data Interface
            ipc_dataset = ds.dataset(file_path, format="ipc")
            connection.register(table_name, ipc_dataset)
        elif extension == ".json":
            connection.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_json_auto('{file_path}');"
            )
        else:
            raise ExecutionEngineError(f"DuckDB engine does not support file type: {extension}")
    except Exception as e:
        raise ExecutionEngineError(f"Failed to mount view for {file_path}: {e}") from e


def _evaluate_bounds(actual_value: float, rule: CompiledSQLRule) -> bool:
    """Evaluates the mathematical result against the contract boundaries."""

    must_be_equal = getattr(rule, "must_be_equal", None)
    if must_be_equal is not None and actual_value != must_be_equal:
        return False

    must_be = getattr(rule, "must_be", None)
    if must_be is not None and actual_value != must_be:
        return False

    must_be_less_than = getattr(rule, "must_be_less_than", None)
    if must_be_less_than is not None and actual_value >= must_be_less_than:
        return False

    must_be_greater_than = getattr(rule, "must_be_greater_than", None)
    return must_be_greater_than is None or actual_value > must_be_greater_than


def _resolve_severity(rule: CompiledSQLRule) -> Literal["CRITICAL", "WARNING"]:
    """Normalizes the rule severity to a strict Literal."""
    raw_severity = str(getattr(rule, "severity", "CRITICAL")).upper()
    return "WARNING" if raw_severity == "WARNING" else "CRITICAL"


def _extract_fusible_select_expr(query: str, table_name: str) -> str | None:
    """
    Inspects a SQL query to determine if it is a pure single-table row count
    (`SELECT COUNT(*) FROM <table> [WHERE <predicate>]`) that can be fused into
    a shared table scan. Returns the `COUNT(*) [FILTER ...]` projection or None.
    """

    pattern = re.compile(
        rf"^\s*SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+{re.escape(table_name)}"
        rf"(?:\s+WHERE\s+(?P<predicate>.+?))?\s*;?\s*$",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.match(query)
    if not match:
        return None

    predicate = match.group("predicate")
    if predicate is None:
        return "COUNT(*)"

    if _NON_FUSIBLE_SQL_CLAUSES.search(predicate):
        return None

    return f"COUNT(*) FILTER (WHERE ({predicate}))"


def _execute_single_rule(
    connection: duckdb.DuckDBPyConnection, rule: CompiledSQLRule
) -> SqlValidationResult:
    """Executes an individual SQL rule with fault isolation."""

    severity = _resolve_severity(rule)
    try:
        result = connection.execute(rule.query).fetchone()
        actual_value = float(result[0]) if result and result[0] is not None else 0.0
        passed = _evaluate_bounds(actual_value, rule)
        return SqlValidationResult(
            rule_name=rule.name,
            severity=severity,
            passed=passed,
            query=rule.query,
            actual_value=actual_value,
        )
    except Exception as e:
        return SqlValidationResult(
            rule_name=rule.name,
            severity=severity,
            passed=False,
            query=rule.query,
            error_message=str(e),
        )


def _execute_fused_batch(
    connection: duckdb.DuckDBPyConnection,
    fusible_batch: list[tuple[int, CompiledSQLRule, str]],
    table_name: str,
) -> list[tuple[int, SqlValidationResult]]:
    """
    Executes multiple `COUNT(*)` filter rules in a single DuckDB table scan.
    Automatically falls back to isolated sequential execution if any predicate
    contains a SQL binding or syntax error.
    """

    projections = [
        f"{select_expr} AS _netra_fuse_{i}" for i, (_, _, select_expr) in enumerate(fusible_batch)
    ]
    fused_query = f"SELECT {', '.join(projections)} FROM {table_name};"

    try:
        row = connection.execute(fused_query).fetchone()
        outcomes: list[tuple[int, SqlValidationResult]] = []
        for i, (orig_idx, rule, _) in enumerate(fusible_batch):
            raw_val = row[i] if row is not None else None
            actual_value = float(raw_val) if raw_val is not None else 0.0
            passed = _evaluate_bounds(actual_value, rule)
            outcomes.append(
                (
                    orig_idx,
                    SqlValidationResult(
                        rule_name=rule.name,
                        severity=_resolve_severity(rule),
                        passed=passed,
                        query=rule.query,
                        actual_value=actual_value,
                    ),
                )
            )
        return outcomes
    except Exception:
        # Fallback: isolate the offending rule by running each rule sequentially
        return [
            (orig_idx, _execute_single_rule(connection, rule))
            for orig_idx, rule, _ in fusible_batch
        ]


def validate_sql_rules_preflight(
    dataset_path: str | Path,
    rules: list[CompiledSQLRule],
    table_name: str = "target_dataset",
) -> list[tuple[str, str]]:
    """
    Performs a zero-scan pre-flight syntax and schema-binding check on compiled
    SQL rules using DuckDB's EXPLAIN planner before Pass 1 begins.
    """
    if not rules:
        return []

    errors: list[tuple[str, str]] = []
    connection = duckdb.connect(database=":memory:")
    try:
        _mount_virtual_view(connection, str(dataset_path), table_name)
        for rule in rules:
            try:
                connection.execute(f"EXPLAIN {rule.query}")
            except Exception as e:
                clean_msg = " ".join(str(e).splitlines())
                errors.append((rule.name, clean_msg))
    except Exception as e:
        clean_msg = " ".join(str(e).splitlines())
        errors.append(("DuckDB View Error", clean_msg))
    finally:
        connection.close()

    return errors


def execute_duckdb_rules(
    dataset_path: str | Path,
    rules: list[CompiledSQLRule],
    thread_count: int,
    table_name: str,
    spill_config: SpillConfig,
) -> list[SqlValidationResult]:
    """
    Executes the compiled SQL rules against the target dataset.

    Args:
        dataset_path: Path to the dataset (Parquet, CSV, etc.).
        rules: The compiled SQL rules generated by duckdb_builder.
        thread_count: The explicit thread limit assigned by the Orchestrator.
        table_name: The internal view name (must match duckdb_builder replacement).
        spill_config: Configuration dictating out-of-core memory settings.

    Returns:
        A list of SqlValidationResult objects indicating pass/fail state.
    """

    if not rules:
        return []

    path_str = str(dataset_path)
    ordered_results: list[SqlValidationResult | None] = [None] * len(rules)

    # Initialize the Engine
    connection = duckdb.connect(database=":memory:")

    # Apply Spill-to-Disk & Thread Governance Settings
    connection.execute(f"PRAGMA threads={thread_count};")

    if spill_config.enabled:
        connection.execute(f"PRAGMA memory_limit='{spill_config.memory_limit_mb}MB';")
        connection.execute(f"PRAGMA temp_directory='{spill_config.temp_directory}';")

    # Disable Insertion Order Preservation
    # Pass 2 strictly evaluates scalar/aggregate global SQL rules. Disabling order
    # preservation prevents DuckDB from hoarding RAM to re-sort multi-threaded Parquet reads.
    connection.execute("PRAGMA preserve_insertion_order=false;")

    try:
        # Mount the Zero-Copy View
        _mount_virtual_view(connection, path_str, table_name)

        # Partition rules into fusible single-scan predicates vs. complex queries
        fusible_batch: list[tuple[int, CompiledSQLRule, str]] = []
        standalone_rules: list[tuple[int, CompiledSQLRule]] = []

        for idx, rule in enumerate(rules):
            fusible_expr = _extract_fusible_select_expr(rule.query, table_name)
            if fusible_expr is not None:
                fusible_batch.append((idx, rule, fusible_expr))
            else:
                standalone_rules.append((idx, rule))

        # 1. Execute fusible COUNT(*) rules in a single shared table scan (if >= 2)
        if len(fusible_batch) >= 2:  # noqa: PLR2004
            for orig_idx, res in _execute_fused_batch(connection, fusible_batch, table_name):
                ordered_results[orig_idx] = res
        elif len(fusible_batch) == 1:
            orig_idx, single_rule, _ = fusible_batch[0]
            ordered_results[orig_idx] = _execute_single_rule(connection, single_rule)

        # 2. Execute complex/grouping SQL rules (e.g., PK uniqueness) individually
        for orig_idx, rule in standalone_rules:
            ordered_results[orig_idx] = _execute_single_rule(connection, rule)
    finally:
        # Guarantee memory release even if the OS tries to interrupt
        connection.close()

    return [result for result in ordered_results if result is not None]
