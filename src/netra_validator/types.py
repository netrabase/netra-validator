"""
Type definitions and schemas for Netra Validator.

This module serves as the structural foundation for Netra Validator. It defines:
1. The strict hierarchical output structure for `ValidationReport`.
2. Telemetry metadata schemas (`ValidatorExecutionMeta`).
3. Out-of-core spill configurations (`SpillConfigDict`).
4. Pipeline execution contexts for CI/CD integrations.
"""

from typing import Literal, TypedDict

from typing_extensions import Any


class PipelineContext(TypedDict):
    """
    Context injected by the CLI orchestrator (`cli/main.py`) when running from the command line.
    This does not exist in the Python API context.
    """

    quality_gate_active: bool
    fail_on_critical: bool
    fail_on_warnings: bool
    status: Literal["PASSED", "FAILED", "PASSIVE"]
    exit_code: int
    reason: str | None


class ThreadMeta(TypedDict):
    """CPU core allocation telemetry."""

    polars: int
    duckdb: int


class IOMeta(TypedDict):
    """Disk read/write state telemetry."""

    output_path_written: bool
    quarantine_path_written: bool


class SpillToDiskMeta(TypedDict):
    """
    JSON Serialization Schema for out-of-core execution settings.
    Records the exact config used during the run for CI/CD observability.
    """

    enabled: bool
    memory_limit_mb: int
    temp_directory: str
    chunk_size: int
    min_spill_space_gb: int


class ExecutionMeta(TypedDict):
    """Telemetry specific to the Netra Validator engine execution."""

    created_at: str
    execution_start_epoch: float
    execution_end_epoch: float
    engine_time_seconds: float
    validator_version: str
    config_source: str
    contract_path: str
    global_target: Literal["raw", "validated"]
    spill_to_disk: SpillToDiskMeta
    threads: ThreadMeta
    io: IOMeta
    engine_warnings: list[str]
    pipeline_context: PipelineContext | None


class DatasetValidationSummary(TypedDict):
    """High-level volume metrics for the dataset evaluation."""

    path: str
    total_rows_evaluated: int
    valid_rows: int
    quarantined_rows: int


class RuleDiagnostic(TypedDict):
    """Universal diagnostic schema for both row-level and global validation rules."""

    target: str
    rule_name: str
    status: Literal["PASSED", "FAILED", "UNVALIDATED"]
    severity: Literal["CRITICAL", "WARNING"]
    error_context: Any  # Accepts a string (Global Rules) or list of dicts (Row-Level Rules)


class ValidationReport(TypedDict):
    """The root schema for the Netra Validator Audit Payload."""

    dataset: DatasetValidationSummary
    pipeline_status: Literal["PASSIVE", "PASSED", "FAILED"]
    column_level_report: list[RuleDiagnostic]
    global_level_report: list[RuleDiagnostic]
    _meta: ExecutionMeta
