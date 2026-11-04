"""
Integration Tests for the DuckDB (Pass 2) Kernel (`netra_validator.engine.duckdb_runner`).

Verifies zero-copy virtual view mounting across file formats and directory globs,
mathematical boundary evaluation, NULL-to-float coercion, out-of-core PRAGMA
governance, and per-rule SQL fault isolation.
"""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from netra_validator.compiler.duckdb_builder import (
    CompiledSQLRule,
    build_global_sql_rules,
)
from netra_validator.config import SpillConfig
from netra_validator.engine.duckdb_runner import execute_duckdb_rules
from netra_validator.exceptions import ExecutionEngineError
from netra_validator.parser.models import DataContract


@pytest.mark.parametrize("fmt", ["parquet", "csv", "ipc", "arrow", "json", "dir"])
def test_duckdb_runner_multi_format_view_mounting_and_rule_evaluation(
    fmt: str,
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
) -> None:
    """
    Verifies that DuckDB mounts virtual views across all supported formats
    and accurately evaluates Primary Key uniqueness and table-level SQL rules.
    """

    dataset_path = dataset_factory(fmt, None)
    rules = build_global_sql_rules(sample_contract, table_name="target_dataset")

    results = execute_duckdb_rules(
        dataset_path=dataset_path,
        rules=rules,
        thread_count=2,
        table_name="target_dataset",
        spill_config=SpillConfig(enabled=False),
    )

    assert len(results) == 3

    # 1. packet_id_is_unique: 2 duplicate PK values (10 and 11) -> fails must_be_equal=0.0
    pk_res = results[0]
    assert pk_res.rule_name == "packet_id_is_unique"
    assert pk_res.passed is False
    assert pk_res.actual_value == 2.0
    assert pk_res.severity == "CRITICAL"
    assert pk_res.error_message is None

    # 2. max_temp_spike_check: 1 row has ambient_temp_c == 60.0 (> 45.0) -> fails
    temp_res = results[1]
    assert temp_res.rule_name == "max_temp_spike_check"
    assert temp_res.passed is False
    assert temp_res.actual_value == 1.0
    assert temp_res.severity == "CRITICAL"

    # 3. lba_station_warning: 0 rows have antenna_type == 'LBA' (< 50.0) -> passes
    lba_res = results[2]
    assert lba_res.rule_name == "lba_station_warning"
    assert lba_res.passed is True
    assert lba_res.actual_value == 0.0
    assert lba_res.severity == "WARNING"


def test_duckdb_runner_glob_pattern_and_spill_pragmas(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
    tmp_path: Path,
) -> None:
    """
    Verifies wildcard glob string mounting (e.g., `dir/*.parquet`) and execution
    under active `SpillConfig` memory and temporary directory PRAGMAs.
    """

    part_dir = dataset_factory("dir", None)
    glob_path = f"{part_dir}/*.parquet"
    rules = build_global_sql_rules(sample_contract, table_name="target_dataset")

    spill_dir = tmp_path / "duckdb_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    spill_cfg = SpillConfig(
        enabled=True,
        memory_limit_mb=512,
        temp_directory=spill_dir,
    )

    results = execute_duckdb_rules(
        dataset_path=glob_path,
        rules=rules,
        thread_count=2,
        table_name="target_dataset",
        spill_config=spill_cfg,
    )

    assert len(results) == 3
    assert results[0].actual_value == 2.0


def test_duckdb_runner_boundary_operators_and_null_coercion(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
) -> None:
    """
    Verifies all mathematical boundary comparators (`must_be_equal`, `must_be`,
    `must_be_less_than`, `must_be_greater_than`) and NULL-to-0.0 result coercion.
    """

    dataset_path = dataset_factory("parquet", None)

    custom_rules = [
        CompiledSQLRule(
            name="exact_row_count_must_be",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be=100.0,
        ),
        CompiledSQLRule(
            name="failed_must_be",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be=99.0,
        ),
        CompiledSQLRule(
            name="passed_greater_than",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be_greater_than=50.0,
        ),
        CompiledSQLRule(
            name="failed_greater_than",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be_greater_than=100.0,
        ),
        CompiledSQLRule(
            name="failed_less_than",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be_less_than=100.0,
        ),
        # Query over empty filter returns [(None,)], which must coerce to 0.0
        CompiledSQLRule(
            name="null_coercion_check",
            query="SELECT SUM(ambient_temp_c) FROM target_dataset WHERE 1 = 0",
            must_be_equal=0.0,
        ),
    ]

    results = execute_duckdb_rules(
        dataset_path=dataset_path,
        rules=custom_rules,
        thread_count=2,
        table_name="target_dataset",
        spill_config=SpillConfig(enabled=False),
    )

    outcomes = {r.rule_name: (r.passed, r.actual_value) for r in results}
    assert outcomes == {
        "exact_row_count_must_be": (True, 100.0),
        "failed_must_be": (False, 100.0),
        "passed_greater_than": (True, 100.0),
        "failed_greater_than": (False, 100.0),
        "failed_less_than": (False, 100.0),
        "null_coercion_check": (True, 0.0),
    }


def test_duckdb_runner_sql_fault_isolation_and_empty_rules(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
) -> None:
    """
    Verifies that a malformed SQL query fails gracefully and records its error message
    without aborting subsequent valid SQL rules in the execution queue.
    """

    dataset_path = dataset_factory("parquet", None)

    # Empty rules short-circuit check
    assert (
        execute_duckdb_rules(
            dataset_path=dataset_path,
            rules=[],
            thread_count=2,
            table_name="target_dataset",
            spill_config=SpillConfig(enabled=False),
        )
        == []
    )

    mixed_rules = [
        CompiledSQLRule(
            name="broken_column_reference",
            query="SELECT non_existent_col FROM target_dataset",
            must_be_equal=0.0,
            severity="WARNING",
        ),
        CompiledSQLRule(
            name="healthy_followup_rule",
            query="SELECT COUNT(*) FROM target_dataset",
            must_be_equal=100.0,
            severity="CRITICAL",
        ),
    ]

    results = execute_duckdb_rules(
        dataset_path=dataset_path,
        rules=mixed_rules,
        thread_count=2,
        table_name="target_dataset",
        spill_config=SpillConfig(enabled=False),
    )

    assert len(results) == 2

    broken_res = results[0]
    assert broken_res.rule_name == "broken_column_reference"
    assert broken_res.passed is False
    assert broken_res.actual_value is None
    assert broken_res.severity == "WARNING"
    assert broken_res.error_message is not None
    assert "non_existent_col" in broken_res.error_message

    healthy_res = results[1]
    assert healthy_res.rule_name == "healthy_followup_rule"
    assert healthy_res.passed is True
    assert healthy_res.actual_value == 100.0
    assert healthy_res.error_message is None


def test_duckdb_runner_unsupported_extension_raises_engine_error(
    sample_contract: DataContract,
    tmp_path: Path,
) -> None:
    """Verifies that mounting an unsupported file format raises `ExecutionEngineError`."""

    unsupported_file = tmp_path / "telemetry.xml"
    unsupported_file.write_text("<root></root>", encoding="utf-8")
    rules = build_global_sql_rules(sample_contract)

    with pytest.raises(ExecutionEngineError, match="does not support file type: .xml"):
        execute_duckdb_rules(
            dataset_path=unsupported_file,
            rules=rules,
            thread_count=2,
            table_name="target_dataset",
            spill_config=SpillConfig(enabled=False),
        )
