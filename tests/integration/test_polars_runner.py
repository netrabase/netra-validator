"""
Integration Tests for the Polars (Pass 1) Kernel (`netra_validator.engine.polars_runner`).

Verifies micro-batch streaming, zero-copy valid/quarantine file bifurcation,
internal flag column stripping, multi-format ingestion, row-group alignment,
and deadlock-free background writer teardown under I/O failure conditions.
"""

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from netra_validator.compiler.polars_builder import build_row_level_expressions
from netra_validator.config import SpillConfig
from netra_validator.engine.polars_runner import execute_polars_rules
from netra_validator.exceptions import ExecutionEngineError
from netra_validator.parser.models import DataContract

EXPECTED_TOTAL_ROWS = 100
EXPECTED_VALID_ROWS = 94
EXPECTED_QUARANTINED_ROWS = 6


@pytest.mark.parametrize("fmt", ["parquet", "csv", "ipc", "dir"])
def test_polars_runner_multi_format_bifurcation_and_schema_hygiene(
    fmt: str,
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
    sample_telemetry_df: pl.DataFrame,
    tmp_path: Path,
) -> None:
    """
    Verifies that the Polars Pass accurately bifurcates valid and quarantined rows
    across all supported storage formats and strips internal `_valid_*` boolean masks
    before writing to disk.
    """

    dataset_path = dataset_factory(fmt, None)
    expressions = build_row_level_expressions(sample_contract)

    valid_out = tmp_path / f"valid_{fmt}.parquet"
    quarantine_out = tmp_path / f"quarantine_{fmt}.parquet"

    result = execute_polars_rules(
        dataset_path=dataset_path,
        expressions=expressions,
        output_path=valid_out,
        quarantine_path=quarantine_out,
        spill_config=SpillConfig(enabled=False),
    )

    assert result.total_rows == EXPECTED_TOTAL_ROWS
    assert result.valid_rows == EXPECTED_VALID_ROWS
    assert result.quarantined_rows == EXPECTED_QUARANTINED_ROWS
    assert result.unvalidated_columns == ["untracked_metric"]

    # Verify per-column failure tallies
    column_failures = {
        column.column_name: column.failed_row_count for column in result.column_results
    }
    assert column_failures == {
        "packet_id": 0,
        "station_id": 3,
        "ambient_temp_c": 2,
        "antenna_type": 1,
        "rfi_flag": 0,
    }

    # Verify physical output partitions and ensure internal flag columns were dropped
    assert valid_out.is_file()
    assert quarantine_out.is_file()

    valid_df = pl.read_parquet(valid_out)
    quarantine_df = pl.read_parquet(quarantine_out)

    assert valid_df.height == EXPECTED_VALID_ROWS
    assert quarantine_df.height == EXPECTED_QUARANTINED_ROWS
    assert valid_df.columns == sample_telemetry_df.columns
    assert quarantine_df.columns == sample_telemetry_df.columns


def test_polars_runner_out_of_core_micro_batch_clamping(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
    tmp_path: Path,
) -> None:
    """
    Verifies that enabling `spill_to_disk` clamps the PyArrow streaming batch size
    and produces identical validation tallies across multiple micro-batches.
    """

    dataset_path = dataset_factory("parquet", None)
    expressions = build_row_level_expressions(sample_contract)

    valid_out = tmp_path / "spilled_valid.parquet"
    quarantine_out = tmp_path / "spilled_quarantine.parquet"

    # Force 10 micro-batches of 10 rows each
    spill_config = SpillConfig(
        enabled=True,
        memory_limit_mb=1024,
        chunk_size=10,
        temp_directory=tmp_path / "spill",
    )

    result = execute_polars_rules(
        dataset_path=dataset_path,
        expressions=expressions,
        output_path=valid_out,
        quarantine_path=quarantine_out,
        spill_config=spill_config,
    )

    assert result.total_rows == EXPECTED_TOTAL_ROWS
    assert result.valid_rows == EXPECTED_VALID_ROWS
    assert result.quarantined_rows == EXPECTED_QUARANTINED_ROWS
    assert pl.read_parquet(valid_out).height == EXPECTED_VALID_ROWS
    assert pl.read_parquet(quarantine_out).height == EXPECTED_QUARANTINED_ROWS


def test_polars_runner_zero_rules_passthrough(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    tmp_path: Path,
) -> None:
    """
    Verifies the fast-path bypass when a contract contains zero row-level rules:
    all rows must be counted as valid and streamed directly to `output_path`.
    """

    dataset_path = dataset_factory("parquet", None)
    valid_out = tmp_path / "passthrough_valid.parquet"
    quarantine_out = tmp_path / "passthrough_quarantine.parquet"

    result = execute_polars_rules(
        dataset_path=dataset_path,
        expressions={},
        output_path=valid_out,
        quarantine_path=quarantine_out,
        spill_config=SpillConfig(enabled=False),
    )

    assert result.total_rows == EXPECTED_TOTAL_ROWS
    assert result.valid_rows == EXPECTED_TOTAL_ROWS
    assert result.quarantined_rows == 0
    assert result.column_results == []
    assert valid_out.is_file()
    assert not quarantine_out.exists()


def test_polars_runner_all_valid_and_all_quarantined_edge_cases(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
    sample_telemetry_df: pl.DataFrame,
    tmp_path: Path,
) -> None:
    """
    Verifies that no empty/orphan Parquet files are created when 100% of rows pass
    or 100% of rows are quarantined.
    """

    expressions = build_row_level_expressions(sample_contract)

    # Case 1: 100% Clean Dataset (Slice rows 6..100 from sample_telemetry_df)
    clean_df = sample_telemetry_df.slice(6, EXPECTED_VALID_ROWS)
    clean_path = dataset_factory("parquet", clean_df)
    clean_valid_out = tmp_path / "clean_valid.parquet"
    clean_quarantine_out = tmp_path / "clean_quarantine.parquet"

    clean_res = execute_polars_rules(
        dataset_path=clean_path,
        expressions=expressions,
        output_path=clean_valid_out,
        quarantine_path=clean_quarantine_out,
        spill_config=SpillConfig(enabled=False),
    )

    assert clean_res.valid_rows == EXPECTED_VALID_ROWS
    assert clean_res.quarantined_rows == 0
    assert clean_valid_out.is_file()
    assert not clean_quarantine_out.exists()

    # Case 2: 100% Quarantined Dataset (Slice rows 0..6 from sample_telemetry_df)
    dirty_df = sample_telemetry_df.slice(0, EXPECTED_QUARANTINED_ROWS)
    dirty_path = dataset_factory("ipc", dirty_df)
    dirty_valid_out = tmp_path / "dirty_valid.parquet"
    dirty_quarantine_out = tmp_path / "dirty_quarantine.parquet"

    dirty_res = execute_polars_rules(
        dataset_path=dirty_path,
        expressions=expressions,
        output_path=dirty_valid_out,
        quarantine_path=dirty_quarantine_out,
        spill_config=SpillConfig(enabled=False),
    )

    assert dirty_res.valid_rows == 0
    assert dirty_res.quarantined_rows == EXPECTED_QUARANTINED_ROWS
    assert not dirty_valid_out.exists()
    assert dirty_quarantine_out.is_file()


def test_polars_runner_background_io_error_propagation_without_deadlock(
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_contract: DataContract,
    tmp_path: Path,
) -> None:
    """
    Simulates an asynchronous writer failure by passing an existing directory as
    `output_path` across 20 micro-batches (exceeding `IO_QUEUE_MAXSIZE = 3`).
    Verifies that `_shutdown_writer_thread` does not deadlock and raises `ExecutionEngineError`.
    """

    dataset_path = dataset_factory("parquet", None)
    expressions = build_row_level_expressions(sample_contract)

    # Passing an existing directory as a target file path forces ParquetWriter to fail
    invalid_output_dir = tmp_path / "is_a_directory"
    invalid_output_dir.mkdir(parents=True, exist_ok=True)

    spill_cfg = SpillConfig(enabled=True, chunk_size=5, temp_directory=tmp_path)

    with pytest.raises(ExecutionEngineError, match="Background I/O Error"):
        execute_polars_rules(
            dataset_path=dataset_path,
            expressions=expressions,
            output_path=invalid_output_dir,
            quarantine_path=None,
            spill_config=spill_cfg,
        )


def test_polars_runner_invalid_dataset_mount_raises_engine_error(
    sample_contract: DataContract,
    tmp_path: Path,
) -> None:
    """Verifies that mounting a non-existent dataset raises `ExecutionEngineError`."""

    expressions = build_row_level_expressions(sample_contract)
    missing_path = tmp_path / "non_existent.parquet"

    with pytest.raises(ExecutionEngineError, match="Failed to mount dataset via PyArrow"):
        execute_polars_rules(
            dataset_path=missing_path,
            expressions=expressions,
            output_path=None,
            quarantine_path=None,
            spill_config=SpillConfig(enabled=False),
        )
