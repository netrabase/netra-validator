"""
Unit Tests for the Report Generation Layer (`netra_validator.report.generator`).

Verifies the CI/CD Kill Switch state machine, critical-over-warning precedence,
descending failure-count rule sorting, and metadata serialization.
"""

import time
from typing import Literal

import pytest

from netra_validator.config import ValidatorConfig
from netra_validator.engine.duckdb_runner import SqlValidationResult
from netra_validator.engine.polars_runner import (
    ColumnValidationResult,
    PolarsValidationResult,
    RuleValidationResult,
)
from netra_validator.report.generator import generate_validation_report


@pytest.mark.parametrize(
    (
        "pipeline_cfg",
        "quarantined_rows",
        "sql_severities_failed",
        "expected_status",
        "expected_exit_code",
        "expected_reason",
    ),
    [
        # 1. Passive Mode: Even with critical failures, status is PASSIVE & exit_code 0
        (
            {"quality_gate_active": False, "fail_on_critical": False},
            25,
            ["CRITICAL"],
            "PASSIVE",
            0,
            None,
        ),
        # 2. Active Mode, Clean Dataset: Status is PASSED & exit_code 0
        (
            {"fail_on_critical": True, "fail_on_warnings": True},
            0,
            [],
            "PASSED",
            0,
            None,
        ),
        # 3. Active Mode, Row-Level Quarantine triggers CRITICAL failure
        (
            {"fail_on_critical": True, "fail_on_warnings": False},
            10,
            [],
            "FAILED",
            1,
            "CRITICAL_ANOMALIES_DETECTED",
        ),
        # 4. Active Mode, Global SQL Warning triggers WARNING failure when armed
        (
            {"fail_on_critical": True, "fail_on_warnings": True},
            0,
            ["WARNING"],
            "FAILED",
            1,
            "WARNING_ANOMALIES_DETECTED",
        ),
        # 5. Precedence Check: Both CRITICAL and WARNING fail -> CRITICAL reason wins
        (
            {"fail_on_critical": True, "fail_on_warnings": True},
            15,
            ["WARNING", "CRITICAL"],
            "FAILED",
            1,
            "CRITICAL_ANOMALIES_DETECTED",
        ),
        # 6. Sensitivity Cascade: fail_on_warnings=True alone halts on CRITICAL anomalies
        (
            {"fail_on_critical": False, "fail_on_warnings": True},
            10,
            [],
            "FAILED",
            1,
            "CRITICAL_ANOMALIES_DETECTED",
        ),
    ],
)
def test_cicd_kill_switch_state_machine(  # noqa: PLR0917
    pipeline_cfg: dict[str, bool],
    quarantined_rows: int,
    sql_severities_failed: list[Literal["CRITICAL", "WARNING"]],
    expected_status: Literal["PASSIVE", "PASSED", "FAILED"],
    expected_exit_code: int,
    expected_reason: str | None,
) -> None:
    """Verifies all CI/CD quality gate state transitions and failure precedence."""
    polars_res = PolarsValidationResult(
        total_rows=100,
        valid_rows=100 - quarantined_rows,
        quarantined_rows=quarantined_rows,
        column_results=[],
        unvalidated_columns=[],
    )

    duckdb_res = [
        SqlValidationResult(
            rule_name=f"rule_{idx}",
            severity=sev,
            passed=False,
            query="SELECT 1",
            actual_value=1.0,
        )
        for idx, sev in enumerate(sql_severities_failed)
    ]

    report = generate_validation_report(
        polars_results=polars_res,
        duckdb_results=duckdb_res,
        engine_warnings=[],
        execution_start_time=time.time() - 1.5,
        dataset_path="data/telemetry.parquet",
        contract_path="contracts/odcs.yaml",
        output_path="out/valid.parquet",
        quarantine_path="out/quarantine.parquet",
        config=ValidatorConfig({"pipeline": pipeline_cfg}),
        config_source="UnitTest",
        polars_threads=12,
        duckdb_threads=8,
    )

    assert report["pipeline_status"] == expected_status
    ctx = report["_meta"]["pipeline_context"]
    assert ctx is not None
    assert ctx["status"] == expected_status
    assert ctx["exit_code"] == expected_exit_code
    assert ctx["reason"] == expected_reason


def test_report_column_sorting_unvalidated_mapping_and_io_flags() -> None:
    """
    Verifies that column rules are sorted descending by failure count,
    unvalidated columns are flagged, and I/O write booleans reflect actual row counts.
    """
    polars_res = PolarsValidationResult(
        total_rows=100,
        valid_rows=100,
        quarantined_rows=0,
        column_results=[
            ColumnValidationResult(
                column_name="station_id",
                failed_row_count=15,
                rule_results=[
                    RuleValidationResult(rule_name="required", failed_row_count=0),
                    RuleValidationResult(rule_name="pattern", failed_row_count=12),
                    RuleValidationResult(rule_name="minLength: 5", failed_row_count=3),
                ],
            )
        ],
        unvalidated_columns=["untracked_col"],
    )

    report = generate_validation_report(
        polars_results=polars_res,
        duckdb_results=[],
        engine_warnings=["Sample non-fatal warning"],
        execution_start_time=time.time() - 0.5,
        dataset_path="data/telemetry.parquet",
        contract_path="contracts/odcs.yaml",
        output_path="out/valid.parquet",
        quarantine_path="out/quarantine.parquet",
        config=ValidatorConfig(),
        config_source="Default",
        polars_threads=16,
        duckdb_threads=8,
    )

    # 1. Verify descending failure sort order on station_id
    col_report = report["column_level_report"]
    assert len(col_report) == 2

    station_diag = col_report[0]
    assert station_diag["target"] == "station_id"
    assert station_diag["status"] == "FAILED"
    assert station_diag["error_context"] == [
        {"rule_name": "pattern", "failed_rows": 12},
        {"rule_name": "minLength: 5", "failed_rows": 3},
        {"rule_name": "required", "failed_rows": 0},
    ]

    # 2. Verify unvalidated column diagnostic
    unvalidated_diag = col_report[1]
    assert unvalidated_diag["target"] == "untracked_col"
    assert unvalidated_diag["rule_name"] == "IGNORED"
    assert unvalidated_diag["status"] == "UNVALIDATED"
    assert unvalidated_diag["severity"] == "WARNING"
    assert unvalidated_diag["error_context"] is None

    # 3. Verify I/O & thread telemetry metadata
    meta = report["_meta"]
    assert meta["threads"] == {"polars": 16, "duckdb": 8}
    assert meta["io"]["output_path_written"] is True
    # quarantined_rows == 0, so quarantine_path_written must be False even with path set
    assert meta["io"]["quarantine_path_written"] is False
    assert meta["engine_warnings"] == ["Sample non-fatal warning"]
