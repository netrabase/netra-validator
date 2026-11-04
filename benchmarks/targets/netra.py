"""
Netra Validator Benchmark Adapter.
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Any

from benchmarks.targets.base import BenchmarkResult, BenchmarkTarget

from netra_validator.config import ValidatorConfig
from netra_validator.engine.validator import Validator


class NetraBenchmarkTarget(BenchmarkTarget):
    """Benchmark target adapter for the Netra Validator engine."""

    def __init__(self) -> None:
        super().__init__(name="netra")
        self.dataset_path: Path | None = None
        self.contract_path: Path | None = None
        self.output_path: Path | None = None
        self.quarantine_path: Path | None = None
        self.scratch_dir: Path | None = None
        self.config: ValidatorConfig | None = None

    def setup(
        self,
        dataset_path: Path,
        contract_path: Path,
        **kwargs: Any,
    ) -> None:
        self.dataset_path = Path(dataset_path).resolve()
        self.contract_path = Path(contract_path).resolve()

        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")
        if not self.contract_path.exists():
            raise FileNotFoundError(f"Contract not found: {self.contract_path}")

        # Provision an isolated temporary workspace for outputs & spill buffers
        self.scratch_dir = self.dataset_path.parent / f".netra_bench_{os.getpid()}"
        self.scratch_dir.mkdir(parents=True, exist_ok=True)

        self.output_path = self.scratch_dir / "valid_rows.parquet"
        self.quarantine_path = self.scratch_dir / "quarantine_rows.parquet"

        # Initialize base config with quality gates armed
        config_override = kwargs.get("config_override", {})

        # --- SPILL-TO-DISK CONFIG OVERRIDE ---
        config_override["spill_to_disk"] = {
            "enabled": False,
            "memory_limit_mb": 32768,
            "chunk_size": 250_000,
            "temp_directory": str(self.scratch_dir / "engine_spill"),
        }
        # -------------------------------------

        self.config = ValidatorConfig(config_override)

    def run_validation(self) -> BenchmarkResult:
        if (
            self.dataset_path is None
            or self.contract_path is None
            or self.output_path is None
            or self.quarantine_path is None
        ):
            raise RuntimeError(
                "NetraBenchmarkTarget.setup() must be called prior to run_validation()."
            )

        validator = Validator(
            contract_path=self.contract_path,
            dataset_path=self.dataset_path,
            output_path=self.output_path,
            quarantine_path=self.quarantine_path,
            config=self.config,
            config_source="BenchmarkHarness",
        )

        t0 = time.perf_counter()
        report = validator.run()
        wall_time = time.perf_counter() - t0

        global_rules = report.get("global_level_report", [])
        assert len(global_rules) == 4, f"Expected 4 global SQL rules, got {len(global_rules)}!"
        for rule in global_rules:
            error_message = rule["error_context"].get("error_message")
            assert error_message is None, f"SQL Rule '{rule['target']}' failed: {error_message}"

        engine_time = report["_meta"]["engine_time_seconds"]
        total_rows = report["dataset"]["total_rows_evaluated"]
        quarantined_rows = report["dataset"]["quarantined_rows"]

        # Calculate anomalies across row-level rules and Pass 2 global SQL rules
        failed_column_rules = sum(
            1 for col in report["column_level_report"] if col["status"] == "FAILED"
        )
        failed_global_rules = sum(
            1 for rule in report["global_level_report"] if rule["status"] == "FAILED"
        )
        total_anomalies = failed_column_rules + failed_global_rules

        # Measure throughput against the input dataset size
        file_size_bytes = self.dataset_path.stat().st_size
        effective_time = engine_time if (engine_time and engine_time > 0) else wall_time

        throughput_mb_s = (
            (file_size_bytes / (1024 * 1024)) / effective_time if effective_time > 0 else 0.0
        )
        row_velocity = (total_rows / effective_time) / 1_000_000 if effective_time > 0 else 0.0

        # Clean up output partitions between iterations (outside timed window)
        # to maximize free NVMe space for subsequent 1B-3B iterations.
        if self.output_path.exists():
            self.output_path.unlink()
        if self.quarantine_path.exists():
            self.quarantine_path.unlink()

        return BenchmarkResult(
            target_name=self.name,
            wall_time_seconds=round(wall_time, 4),
            engine_time_seconds=round(engine_time, 4) if engine_time else None,
            peak_rss_mb=0.0,  # External collector will measure and stamp true RSS
            rows_evaluated=total_rows,
            throughput_mb_s=round(throughput_mb_s, 2),
            row_velocity_m_rows_s=round(row_velocity, 2),
            passed=report["pipeline_status"] == "PASSED",
            quarantined_rows=quarantined_rows,
            anomalies_detected=total_anomalies,
            extra_meta={
                "polars_threads": report["_meta"]["threads"]["polars"],
                "duckdb_threads": report["_meta"]["threads"]["duckdb"],
                "exit_code": report["_meta"]["pipeline_context"]["exit_code"]
                if report["_meta"]["pipeline_context"]
                else 0,
            },
        )

    def teardown(self) -> None:
        """Purges output partitions and scratch workspace."""
        if self.scratch_dir and self.scratch_dir.exists():
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
