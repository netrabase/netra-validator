"""
Report Generation Layer.

This module compiles the raw execution results from the Polars and DuckDB engines
into a strictly typed, JSON-serializable Enterprise Data Contract audit payload.
It isolates the presentation and schema logic from the core execution engine.
"""

import datetime
import time
from pathlib import Path
from typing import Literal, cast

from netra_validator import __version__
from netra_validator.config import ValidatorConfig
from netra_validator.engine.duckdb_runner import SqlValidationResult
from netra_validator.engine.polars_runner import PolarsValidationResult
from netra_validator.types import RuleDiagnostic, ValidationReport


def generate_validation_report(  # noqa: PLR0913, PLR0917
    polars_results: PolarsValidationResult,
    duckdb_results: list[SqlValidationResult],
    engine_warnings: list[str],
    execution_start_time: float,
    dataset_path: str | Path,
    contract_path: str | Path,
    output_path: str | Path | None,
    quarantine_path: str | Path | None,
    config: ValidatorConfig,
    config_source: str,
    polars_threads: int,
    duckdb_threads: int,
) -> ValidationReport:
    """
    Compiles engine results into the CI/CD JSON payload and evaluates the Kill Switch.

    Args:
        polars_results: The dataclass containing Pass 1 row-level metrics.
        duckdb_results: The list of dataclasses containing Pass 2 global SQL results.
        engine_warnings: A list of non-fatal warnings encountered during execution.
        execution_start_time: Epoch timestamp of when the Validator run() began.
        dataset_path: Path to the original dataset.
        contract_path: Path to the ODCS contract used.
        output_path: Path where clean data was routed (if any).
        quarantine_path: Path where corrupted data was routed (if any).
        config: The active ValidatorConfig instance governing quality gates and spill limits.
        config_source: Identifier for the configuration origin (e.g., CLI, Default).
        polars_threads: The CPU budget used for the streaming engine.
        duckdb_threads: The CPU budget used for the out-of-core SQL engine.

    Returns:
        A strictly typed dictionary representing the final audit report.
    """
    execution_end_time = time.time()

    # 1. CI/CD Kill Switch Logic using Severities
    has_critical_fails = False
    has_warning_fails = False

    # Row-level rules natively result in quarantines (Treated as CRITICAL)
    if polars_results.quarantined_rows > 0:
        has_critical_fails = True

    # SQL Rules evaluation based on parsed AST severity
    for result in duckdb_results:
        if not result.passed:
            if result.severity == "CRITICAL":
                has_critical_fails = True
            elif result.severity == "WARNING":
                has_warning_fails = True

    # Evaluate the final pipeline status against config thresholds
    pipeline_status = "PASSIVE"
    exit_code = 0
    reason = None

    if config.pipeline.quality_gate_active:
        pipeline_status = "PASSED"
        if has_critical_fails and (
            config.pipeline.fail_on_critical or config.pipeline.fail_on_warnings
        ):
            pipeline_status = "FAILED"
            exit_code = 1
            reason = "CRITICAL_ANOMALIES_DETECTED"
        elif has_warning_fails and config.pipeline.fail_on_warnings:
            pipeline_status = "FAILED"
            exit_code = 1
            reason = "WARNING_ANOMALIES_DETECTED"

    pipeline_context = {
        "quality_gate_active": config.pipeline.quality_gate_active,
        "fail_on_critical": config.pipeline.fail_on_critical,
        "fail_on_warnings": config.pipeline.fail_on_warnings,
        "status": pipeline_status,
        "exit_code": exit_code,
        "reason": reason,
    }

    # 2. Map Column metrics (Row-Level)
    column_level_report: list[RuleDiagnostic] = []

    # A. Map Validated Columns
    for column_result in polars_results.column_results:
        column_rule_status: Literal["PASSED", "FAILED"] = (
            "FAILED" if column_result.failed_row_count > 0 else "PASSED"
        )

        # Sort to bubble failing rules to the top, and un-collapse to show all PASSING rules
        sorted_rules = sorted(
            column_result.rule_results, key=lambda r: r.failed_row_count, reverse=True
        )

        column_error_context = [
            {"rule_name": rule.rule_name, "failed_rows": rule.failed_row_count}
            for rule in sorted_rules
        ]

        column_level_report.append(
            {
                "target": column_result.column_name,
                "rule_name": "CONTRACT_ENFORCED",
                "status": column_rule_status,
                "severity": "CRITICAL",
                "error_context": column_error_context,
            }
        )

    # B. Map Unvalidated Columns
    for unvalidated_column in polars_results.unvalidated_columns:
        column_level_report.append(
            {
                "target": unvalidated_column,
                "rule_name": "IGNORED",
                "status": "UNVALIDATED",
                "severity": "WARNING",
                "error_context": None,
            }
        )

    # 3. Map Global SQL metrics
    global_level_report: list[RuleDiagnostic] = []

    for result in duckdb_results:
        sql_rule_status: Literal["PASSED", "FAILED"] = "PASSED" if result.passed else "FAILED"

        sql_error_context = {
            "query": result.query,
            "actual_value": result.actual_value,
            "error_message": result.error_message,
        }

        global_level_report.append(
            {
                "target": result.rule_name,
                "rule_name": "SQL_RULE",
                "status": sql_rule_status,
                "severity": result.severity,
                "error_context": sql_error_context,
            }
        )

    # 4. Construct Final Validation Report Object
    report = {
        "dataset": {
            "path": str(dataset_path),
            "total_rows_evaluated": polars_results.total_rows,
            "valid_rows": polars_results.valid_rows,
            "quarantined_rows": polars_results.quarantined_rows,
        },
        "pipeline_status": pipeline_status,
        "column_level_report": column_level_report,
        "global_level_report": global_level_report,
        "_meta": {
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "execution_start_epoch": execution_start_time,
            "execution_end_epoch": execution_end_time,
            "engine_time_seconds": round(execution_end_time - execution_start_time, 4),
            "validator_version": __version__,
            "config_source": config_source,
            "contract_path": str(contract_path),
            "global_target": config.global_target,
            "spill_to_disk": {
                "enabled": config.spill.enabled,
                "memory_limit_mb": config.spill.memory_limit_mb,
                "temp_directory": str(config.spill.temp_directory),
                "chunk_size": config.spill.chunk_size,
                "min_spill_space_gb": config.spill.min_spill_space_gb,
            },
            "threads": {
                "polars": polars_threads,
                "duckdb": duckdb_threads,
            },
            "io": {
                "output_path_written": bool(output_path and polars_results.valid_rows > 0),
                "quarantine_path_written": bool(
                    quarantine_path and polars_results.quarantined_rows > 0
                ),
            },
            "engine_warnings": engine_warnings,
            "pipeline_context": pipeline_context,
        },
    }

    return cast(ValidationReport, report)
