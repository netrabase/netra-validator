"""
Netra Validator Core Orchestrator.

Manages the end-to-end lifecycle of a data contract validation session,
transforming a target dataset and an ODCS v3.1.0 contract into a deterministic
audit report. Connects the ODCS parser, AST compilers, and execution kernels
(Polars Pass 1 / DuckDB Pass 2) under unified memory and thread governance.
"""

import contextlib
import ctypes
import gc
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from polars.exceptions import ComputeError

from netra_validator.compiler.duckdb_builder import CompiledSQLRule, build_global_sql_rules
from netra_validator.compiler.polars_builder import build_row_level_expressions
from netra_validator.config import ValidatorConfig
from netra_validator.engine.duckdb_runner import (
    SqlValidationResult,
    execute_duckdb_rules,
    validate_sql_rules_preflight,
)
from netra_validator.engine.polars_runner import execute_polars_rules
from netra_validator.exceptions import ExecutionEngineError, ResourcePreconditionError
from netra_validator.parser.reader import read_contract
from netra_validator.report.generator import generate_validation_report
from netra_validator.types import ValidationReport

# Out-of-core DuckDB heuristic: ~4 GB (4096 MB) per active thread prevents
# concurrent thread-local string heaps from spiking past the OS OOM threshold
# before the central Buffer Manager can flush to NVMe.
DUCKDB_MIN_MB_PER_THREAD = 4096
DEFAULT_FALLBACK_THREADS = 4


@dataclass
class EngineTelemetry:
    """Engine macro-state flag for the UI."""

    current_phase: str = "BOOT"
    start_time: float = 0.0
    polars_threads: int = 16
    duckdb_threads: int = 16
    engine_time: float | None = None
    peak_ram_mb: float | None = None
    throughput_gb_s: float | None = None
    row_velocity: float | None = None
    total_ram_mb: float = 0.0


class Validator:
    """
    The core orchestrator class for Netra Validator.

    Manages the lifecycle of a data contract validation session, transforming
    a target dataset and an ODCS v3.1.0 contract into a strict pass/fail audit report.
    It connects the ODCS parser, the AST compilers, and the execution runners into a
    unified, fault-tolerant pipeline.

    Architecture Notes:
    1. Zero-Copy Out-of-Core I/O: Both Polars (via PyArrow) and DuckDB stream
       datasets directly from disk without whole-file RAM materialization.
    2. Two-Pass Sequential Execution: Evaluates row-level rules via streaming Polars (Pass 1)
       before evaluating global/SQL rules via DuckDB (Pass 2) to prevent I/O thrashing.
    3. Fault Tolerance: Isolated errors gracefully degrade into logged warnings rather
       than crashing the pipeline.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        contract_path: str | Path,
        dataset_path: str | Path,
        output_path: str | Path | None = None,
        quarantine_path: str | Path | None = None,
        config: ValidatorConfig | dict[str, Any] | None = None,
        config_source: str = "Default",
    ) -> None:
        """
        Initializes the validation session and calculates hardware thread budgets.

        Args:
            contract_path: Path to the Open Data Contract Standard (ODCS) YAML file.
            dataset_path: Path to the target dataset (Parquet, CSV, Arrow IPC, etc.).
            output_path: Destination path for clean rows passing all row-level rules.
            quarantine_path: Destination path for anomalous rows failing row-level rules.
            config: Engine configuration instance or dictionary of overrides.
            config_source: Identifier for the configuration origin (e.g., CLI, Default).
        """

        self.contract_path = Path(contract_path)
        self.dataset_path = Path(dataset_path)
        self.output_path = Path(output_path) if output_path else None
        self.quarantine_path = Path(quarantine_path) if quarantine_path else None
        self.config_source = config_source
        self.telemetry: EngineTelemetry | None = None

        # Parse config
        if isinstance(config, ValidatorConfig):
            self.config = config
        else:
            self.config = ValidatorConfig(config)

        if self.config.global_target == "validated" and not self.output_path:
            raise ValueError(
                "Cannot set `global_target='validated'` without providing an `output_path`."
            )

        # Unified Thread Governance
        self.polars_threads = self._resolve_threads("NETRA_POLARS_THREADS")
        raw_duckdb_threads = self._resolve_threads("NETRA_DUCKDB_THREADS")

        if self.config.spill.enabled:
            # Dynamic Thread Clamping
            # DuckDB thread-local string heaps spike during high-cardinality aggregations.
            # Enforce ~4GB RAM per active thread so the Buffer Manager has sufficient
            # headroom to spill to disk before triggering an OS OOM kill.
            self.duckdb_threads = max(
                1,
                min(
                    raw_duckdb_threads,
                    self.config.spill.memory_limit_mb // DUCKDB_MIN_MB_PER_THREAD,
                ),
            )
        else:
            self.duckdb_threads = raw_duckdb_threads

        # Pre-Flight: Out-of-Core Resilience Checks
        self._execute_pre_flight_checks()

    def _execute_pre_flight_checks(self) -> None:
        """Validates hardware, environment variables, and disk preconditions."""

        os.environ["POLARS_MAX_THREADS"] = str(self.polars_threads)

        if self.config.spill.enabled:
            temp_directory = self.config.spill.temp_directory
            temp_directory.mkdir(parents=True, exist_ok=True)

            # Check free space
            _, _, free = shutil.disk_usage(temp_directory)
            free_space_gb = free / (1024**3)

            required_spill_space_gb = self.config.spill.min_spill_space_gb
            if free_space_gb < required_spill_space_gb:
                raise ResourcePreconditionError(
                    f"Spill-to-Disk Pre-Flight Failed: Require {required_spill_space_gb} GB free "
                    f"on {temp_directory}, but only {free_space_gb:.2f} GB is available. "
                    "Aborting to prevent engine crash."
                )

    def _resolve_threads(self, env_variable_name: str) -> int:
        """
        Resolves the CPU thread budget using a 3-tier cascade respecting container limits.
        1. Explicit Environment Variable.
        2. Linux cgroup affinity (Respects Kubernetes/Docker vCPU quotas).
        3. OS logical CPU count fallback.
        """

        # 1. Environment Variable Override
        if env_variable_value := os.environ.get(env_variable_name):
            try:
                return max(1, int(env_variable_value))
            except ValueError:
                pass  # Fall through if environment variable is malformed

        # 2. Container/cgroup limits (Linux primarily)
        try:
            return len(os.sched_getaffinity(0))
        except AttributeError:
            pass  # Non-Linux platforms (macOS/Windows)

        # 3. Hardware Fallback
        return os.cpu_count() or DEFAULT_FALLBACK_THREADS

    def _execute_duckdb_pass(
        self,
        duckdb_ast: list[CompiledSQLRule],
        duckdb_target_view: str,
        valid_rows: int,
        engine_warnings: list[str],
    ) -> list[SqlValidationResult]:
        """Executes Pass 2 global SQL validation with fault-tolerant safeguards."""

        duckdb_results: list[SqlValidationResult] = []

        validate_clean_output = self.config.global_target == "validated"
        target_duckdb_file: Path = (
            self.output_path
            if (validate_clean_output and self.output_path is not None)
            else self.dataset_path
        )

        # Safeguard 1: Short-circuit if validating clean output but 0 valid rows survived Pass 1
        if validate_clean_output and valid_rows == 0:
            engine_warnings.append(
                "DuckDB Pass skipped: 'global_target' is 'validated', but 0 valid rows remained."
            )
            # Mark all expected DuckDB rules as failed/skipped
            for rule in duckdb_ast:
                duckdb_results.append(
                    SqlValidationResult(
                        rule_name=rule.name,
                        passed=False,
                        query=rule.query,
                        error_message="Skipped: 0 valid rows available for evaluation.",
                        severity="WARNING",
                    )
                )
        elif duckdb_ast:
            try:
                # Execute normal DuckDB Pass logic
                duckdb_results = execute_duckdb_rules(
                    dataset_path=target_duckdb_file,
                    rules=duckdb_ast,
                    thread_count=self.duckdb_threads,
                    table_name=duckdb_target_view,
                    spill_config=self.config.spill,
                )
            except Exception as e:
                # Safeguard 2: Graceful degradation on engine-level DuckDB failure
                engine_warnings.append(f"DuckDB Pass encountered a fatal engine error: {e}")
                for rule in duckdb_ast:
                    duckdb_results.append(
                        SqlValidationResult(
                            rule_name=rule.name,
                            passed=False,
                            query=rule.query,
                            error_message="Engine Failure.",
                            severity="CRITICAL",
                        )
                    )

        # Extract SQL rule syntax errors into our warnings payload
        for result in duckdb_results:
            if result.error_message and result.error_message != "Engine Failure.":
                engine_warnings.append(
                    f"SQL Rule '{result.rule_name}' failed execution: {result.error_message}"
                )

        return duckdb_results

    @staticmethod
    def _release_process_memory() -> None:
        """
        Forces Python GC, PyArrow's C++ pool, and the Linux glibc allocator to
        return freed Pass 1 heap pages to the OS before Pass 2 initializes.
        """

        gc.collect()
        pa.default_memory_pool().release_unused()
        if sys.platform.startswith("linux"):
            with contextlib.suppress(Exception):
                ctypes.CDLL("libc.so.6").malloc_trim(0)

    def run(self) -> ValidationReport:
        """
        Executes the Two-Pass validation architecture and generates the Audit Report.

        Returns:
            A structured ValidationReport containing CI/CD exit state, column-level
            diagnostics, global SQL rule outcomes, and execution telemetry.
        """

        execution_start_time = time.time()
        engine_warnings: list[str] = []

        if self.telemetry:
            self.telemetry.polars_threads = self.polars_threads
            self.telemetry.duckdb_threads = self.duckdb_threads

        # ==========================================
        # PASS 0: Ingestion & Compilation
        # ==========================================

        contract = read_contract(self.contract_path)

        # Row-level rules for Polars
        polars_ast = build_row_level_expressions(contract)

        # Global rules for DuckDB
        duckdb_target_view = "target_dataset"
        duckdb_ast = build_global_sql_rules(contract, table_name=duckdb_target_view)

        if duckdb_ast:
            sql_errors = validate_sql_rules_preflight(
                self.dataset_path, duckdb_ast, table_name=duckdb_target_view
            )
            if sql_errors:
                rule_name, error_message = sql_errors[0]
                raise ExecutionEngineError(
                    f"Pre-flight SQL validation failed for rule '{rule_name}': {error_message}"
                )

        # ==========================================
        # PASS 1: Polars Pass (Row-Level)
        # ==========================================

        if self.telemetry:
            self.telemetry.current_phase = "POLARS_PASS"

        try:
            polars_results = execute_polars_rules(
                dataset_path=self.dataset_path,
                expressions=polars_ast,
                output_path=self.output_path,
                quarantine_path=self.quarantine_path,
                spill_config=self.config.spill,
            )
        except ComputeError as e:
            # Captures Rust panics (e.g., trying to run >= on a String column)
            raise ExecutionEngineError(
                f"Polars Engine Error (Type Mismatch or Compute Error): {e}"
            ) from e
        except Exception as e:
            raise ExecutionEngineError(f"Polars Pass failed unexpectedly: {e}") from e

        # Reclaim Pass 1 heap allocations before DuckDB Pass 2 initializes
        self._release_process_memory()

        # ==========================================
        # PASS 2: DuckDB Pass (Global/SQL)
        # ==========================================

        if self.telemetry:
            self.telemetry.current_phase = "DUCKDB_PASS"

        duckdb_results = self._execute_duckdb_pass(
            duckdb_ast=duckdb_ast,
            duckdb_target_view=duckdb_target_view,
            valid_rows=polars_results.valid_rows,
            engine_warnings=engine_warnings,
        )

        # ==========================================
        # PASS 3: Report Object Assembly
        # ==========================================

        if self.telemetry:
            self.telemetry.current_phase = "ASSEMBLY"

        return generate_validation_report(
            polars_results=polars_results,
            duckdb_results=duckdb_results,
            engine_warnings=engine_warnings,
            execution_start_time=execution_start_time,
            dataset_path=self.dataset_path,
            contract_path=self.contract_path,
            output_path=self.output_path,
            quarantine_path=self.quarantine_path,
            config=self.config,
            config_source=self.config_source,
            polars_threads=self.polars_threads,
            duckdb_threads=self.duckdb_threads,
        )
