"""
Abstract Base Target and standardized result schemas for benchmark targets.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BenchmarkResult:
    """Standardized performance and correctness metrics emitted by an adapter run."""

    target_name: str
    wall_time_seconds: float
    engine_time_seconds: float | None
    peak_rss_mb: float
    rows_evaluated: int
    throughput_mb_s: float
    row_velocity_m_rows_s: float
    passed: bool
    quarantined_rows: int
    anomalies_detected: int
    extra_meta: dict[str, Any] = field(default_factory=dict)


class BenchmarkTarget(ABC):
    """Abstract interface governing the lifecycle of a benchmark validation adapter."""

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def setup(
        self,
        dataset_path: Path,
        contract_path: Path,
        **kwargs: Any,
    ) -> None:
        """
        Prepares the adapter for execution.

        Handles pre-compilation, schema parsing, and scratchpad/output directory
        provisioning. Any initialization work performed here is excluded from the
        benchmark's timed execution phase.
        """
        ...

    @abstractmethod
    def run_validation(self) -> BenchmarkResult:
        """
        Executes strictly the validation pass over the target dataset.

        Returns:
            A BenchmarkResult instance containing execution timings and audit metrics.
        """
        ...

    @abstractmethod
    def teardown(self) -> None:
        """
        Performs post-run resource reclamation.

        Purges temporary quarantined files, output streams, or spill buffers
        between iterations to ensure clean disk and page-cache boundaries.
        """
        ...
