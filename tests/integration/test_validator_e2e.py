"""
End-to-End Integration Tests for the Core Orchestrator (`netra_validator.engine.validator`).

Verifies full Two-Pass pipeline orchestration, global_target data routing,
thread budget resolution cascades, dynamic out-of-core thread clamping,
pre-flight disk preconditions, and fault-tolerance safeguards.
"""

from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from netra_validator.config import ValidatorConfig
from netra_validator.engine.validator import EngineTelemetry, Validator
from netra_validator.exceptions import ExecutionEngineError, ResourcePreconditionError

EXPECTED_TOTAL_ROWS = 100
EXPECTED_VALID_ROWS = 94
EXPECTED_QUARANTINED_ROWS = 6


def test_validator_e2e_raw_vs_validated_sql_routing(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    tmp_path: Path,
) -> None:
    """
    Proves the behavioral difference between `global_target=raw` and `validated`:
    - Row 4 has `ambient_temp_c = 60.0`, which violates Pass 1 (`maximum: 50.0`)
      and Pass 2 (`max_temp_spike_check: ambient_temp_c > 45.0`).
    - When Pass 2 runs on RAW data, `max_temp_spike_check` FAILS (actual_value = 1.0).
    - When Pass 2 runs on VALIDATED data, Row 4 is quarantined in Pass 1, so
      `max_temp_spike_check` PASSES (actual_value = 0.0) while `packet_id_is_unique`
      still FAILS on the surviving duplicate PKs at rows 98 and 99.
    """

    contract_path = contract_factory(None, "contract.yaml")
    dataset_path = dataset_factory("parquet", None)
    valid_out = tmp_path / "valid_rows.parquet"
    quarantine_out = tmp_path / "quarantine_rows.parquet"

    # 1. Default Mode: global_target = raw (Evaluates Raw Dataset)
    raw_validator = Validator(
        contract_path=contract_path,
        dataset_path=dataset_path,
        output_path=valid_out,
        quarantine_path=quarantine_out,
        config={
            "pipeline": {"fail_on_critical": True},
            "validator": {"global_target": "raw"},
        },
    )
    raw_report = raw_validator.run()

    assert raw_report["dataset"]["total_rows_evaluated"] == EXPECTED_TOTAL_ROWS
    assert raw_report["dataset"]["valid_rows"] == EXPECTED_VALID_ROWS
    assert raw_report["dataset"]["quarantined_rows"] == EXPECTED_QUARANTINED_ROWS
    assert raw_report["pipeline_status"] == "FAILED"

    raw_global = {r["target"]: r for r in raw_report["global_level_report"]}
    assert raw_global["max_temp_spike_check"]["status"] == "FAILED"
    assert raw_global["packet_id_is_unique"]["status"] == "FAILED"
    assert raw_global["lba_station_warning"]["status"] == "PASSED"

    # 2. Validated Routing Mode: global_target = validated (Evaluates Clean Output)
    clean_out = tmp_path / "clean_routed.parquet"
    validated_validator = Validator(
        contract_path=contract_path,
        dataset_path=dataset_path,
        output_path=clean_out,
        quarantine_path=quarantine_out,
        config={
            "pipeline": {"fail_on_critical": True},
            "validator": {"global_target": "validated"},
        },
    )
    validated_report = validated_validator.run()

    val_global = {r["target"]: r for r in validated_report["global_level_report"]}
    # Row 4 (60.0C) was quarantined in Pass 1, so Pass 2 sees 0 spikes > 45.0C
    assert val_global["max_temp_spike_check"]["status"] == "PASSED"
    # Duplicate PKs (rows 98 & 99) passed row-level rules, so PK check still fails
    assert val_global["packet_id_is_unique"]["status"] == "FAILED"


def test_validator_global_target_requires_output_path(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
) -> None:
    """Verifies that `global_target="validated"` without `output_path` raises ValueError."""
    contract_path = contract_factory(None, "contract.yaml")
    dataset_path = dataset_factory("parquet", None)

    with pytest.raises(ValueError, match="without providing an `output_path`"):
        Validator(
            contract_path=contract_path,
            dataset_path=dataset_path,
            output_path=None,
            config={"validator": {"global_target": "validated"}},
        )


def test_validator_thread_governance_cascade_and_telemetry_sync(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    Verifies environment variable thread overrides, dynamic DuckDB out-of-core
    thread clamping (`memory_limit_mb // 4096`), and live `EngineTelemetry` sync.
    """

    contract_path = contract_factory(None, "contract.yaml")
    dataset_path = dataset_factory("parquet", None)

    monkeypatch.setenv("NETRA_POLARS_THREADS", "24")
    monkeypatch.setenv("NETRA_DUCKDB_THREADS", "32")

    # 12,288 MB // 4096 MB = 3 threads (clamped down from 32)
    cfg = ValidatorConfig(
        {
            "spill_to_disk": {
                "enabled": True,
                "memory_limit_mb": 12288,
                "chunk_size": 25,
                "min_spill_space_gb": 0,
                "temp_directory": str(tmp_path / "spill_zone"),
            }
        }
    )

    validator = Validator(
        contract_path=contract_path,
        dataset_path=dataset_path,
        config=cfg,
        config_source="EnvTest",
    )

    assert validator.polars_threads == 24
    assert validator.duckdb_threads == 3

    telemetry = EngineTelemetry()
    validator.telemetry = telemetry

    report = validator.run()

    assert telemetry.current_phase == "ASSEMBLY"
    assert telemetry.polars_threads == 24
    assert telemetry.duckdb_threads == 3
    assert report["_meta"]["threads"] == {"polars": 24, "duckdb": 3}
    assert report["_meta"]["config_source"] == "EnvTest"

    # Malformed environment variable should fall back gracefully without crashing
    monkeypatch.setenv("NETRA_POLARS_THREADS", "not_an_integer")
    fallback_validator = Validator(
        contract_path=contract_path,
        dataset_path=dataset_path,
    )
    assert fallback_validator.polars_threads >= 1


def test_validator_preflight_disk_space_guard(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    tmp_path: Path,
) -> None:
    """
    Verifies that `_execute_pre_flight_checks` raises `ResourcePreconditionError`
    when available disk space is below `min_spill_space_gb`.
    """

    contract_path = contract_factory(None, "contract.yaml")
    dataset_path = dataset_factory("parquet", None)

    impossible_spill_cfg = {
        "spill_to_disk": {
            "enabled": True,
            "min_spill_space_gb": 999_999,
            "temp_directory": str(tmp_path / "spill_check"),
        }
    }

    with pytest.raises(ResourcePreconditionError, match="Spill-to-Disk Pre-Flight Failed"):
        Validator(
            contract_path=contract_path,
            dataset_path=dataset_path,
            config=impossible_spill_cfg,
        )


def test_validator_safeguard_zero_valid_rows_skips_duckdb_pass(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_telemetry_df: pl.DataFrame,
    tmp_path: Path,
) -> None:
    """
    Verifies Safeguard 1: When `global_target="validated"` and Pass 1 quarantines 100%
    of rows (0 valid rows remaining), Pass 2 is skipped with a warning.
    """

    contract_path = contract_factory(None, "contract.yaml")
    # Rows 0..6 all violate row-level constraints
    all_invalid_df = sample_telemetry_df.slice(0, EXPECTED_QUARANTINED_ROWS)
    dataset_path = dataset_factory("parquet", all_invalid_df)

    validator = Validator(
        contract_path=contract_path,
        dataset_path=dataset_path,
        output_path=tmp_path / "empty_valid.parquet",
        quarantine_path=tmp_path / "all_quarantined.parquet",
        config={"validator": {"global_target": "validated"}},
    )

    report = validator.run()

    assert report["dataset"]["valid_rows"] == 0
    assert report["dataset"]["quarantined_rows"] == EXPECTED_QUARANTINED_ROWS
    assert any("0 valid rows remained" in warning for warning in report["_meta"]["engine_warnings"])

    for global_rule in report["global_level_report"]:
        assert global_rule["status"] == "FAILED"
        assert global_rule["severity"] == "WARNING"


def test_validator_preflight_sql_halt_and_fatal_duckdb_degradation(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    base_contract_dict: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verifies that:
    1. Invalid SQL rules or missing columns halt immediately in Pass 0 Pre-Flight
       with `ExecutionEngineError` before Pass 1 runs.
    2. Unexpected runtime Pass 2 engine exceptions degrade gracefully into
       `engine_warnings` (Safeguard 2).
    """

    payload = deepcopy(base_contract_dict)
    payload["schema"][0]["quality"].append(
        {
            "name": "bad_sql_rule",
            "type": "sql",
            "query": "SELECT missing_column FROM {object}",
            "mustBeEqual": 0.0,
        }
    )
    bad_contract_path = contract_factory(payload, "bad_sql_contract.yaml")
    dataset_path = dataset_factory("parquet", None)

    # 1. Pre-flight SQL validation halts before Pass 1
    bad_validator = Validator(contract_path=bad_contract_path, dataset_path=dataset_path)
    with pytest.raises(
        ExecutionEngineError,
        match="Pre-flight SQL validation failed for rule 'bad_sql_rule'",
    ):
        bad_validator.run()

    # 2. Fatal Pass 2 engine crash on a valid contract (Safeguard 2)
    valid_contract_path = contract_factory(None, "valid_contract.yaml")
    valid_validator = Validator(contract_path=valid_contract_path, dataset_path=dataset_path)

    def _mock_fatal_duckdb(*_args: Any, **_kwargs: Any) -> list[Any]:
        raise RuntimeError("Simulated DuckDB C++ Allocator Crash")

    monkeypatch.setattr(
        "netra_validator.engine.validator.execute_duckdb_rules",
        _mock_fatal_duckdb,
    )

    fatal_report = valid_validator.run()
    assert any(
        "DuckDB Pass encountered a fatal engine error" in w
        for w in fatal_report["_meta"]["engine_warnings"]
    )
    for rule_diagnostic in fatal_report["global_level_report"]:
        assert rule_diagnostic["status"] == "FAILED"
        assert rule_diagnostic["severity"] == "CRITICAL"


def test_validator_polars_type_mismatch_raises_execution_engine_error(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    dataset_factory: Callable[[str, pl.DataFrame | None], Path],
    sample_telemetry_df: pl.DataFrame,
    base_contract_dict: dict[str, Any],
) -> None:
    """
    Verifies that a physical schema type mismatch triggering a Polars Rust
    `ComputeError` in Pass 1 is caught and translated into an `ExecutionEngineError`.
    """

    payload = deepcopy(base_contract_dict)
    payload["schema"][0]["quality"] = []  # Bypass Pass 0 SQL binder check on ambient_temp_c
    contract_path = contract_factory(payload, "polars_only_contract.yaml")

    # Replace numeric `ambient_temp_c` with incompatible strings to trigger ComputeError
    corrupt_df = sample_telemetry_df.with_columns(
        pl.lit("corrupt_string_temp").alias("ambient_temp_c")
    )
    dataset_path = dataset_factory("parquet", corrupt_df)

    validator = Validator(contract_path=contract_path, dataset_path=dataset_path)

    with pytest.raises(ExecutionEngineError, match="Polars Engine Error"):
        validator.run()
