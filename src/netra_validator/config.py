"""
Universal Configuration Manager for Netra Validator.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

GlobalTarget = Literal["raw", "validated"]


@dataclass
class SpillConfig:
    """Strongly typed dataclass managing out-of-core disk spilling behavior."""

    enabled: bool = False
    memory_limit_mb: int = 16384  # 16 GB default RAM ceiling
    temp_directory: Path = Path("/tmp/netra_spill")
    chunk_size: int = 50_000  # Micro-batch size for Polars streaming
    min_spill_space_gb: int = 10  # Safety buffer for Pre-flight disk checks


class PipelineConfig:
    """Manages the global execution state and CI/CD quality gates."""

    def __init__(self, pipeline_block: dict[str, Any] | None = None) -> None:
        block = pipeline_block or {}

        self.fail_on_critical: bool = bool(block.get("fail_on_critical", False))
        self.fail_on_warnings: bool = bool(block.get("fail_on_warnings", False))

        # If the user didn't explicitly set 'quality_gate_active', it defaults to True
        # ONLY IF one of the severity fail flags is True.
        quality_gate_default = self.fail_on_critical or self.fail_on_warnings
        self.quality_gate_active: bool = bool(
            block.get("quality_gate_active", quality_gate_default)
        )


class ValidatorConfig:
    """
    Root Configuration Manager for Netra Validator.

    Supports both the unified multi-tool `netra_config.yaml` schema (root `pipeline:`
    defaults + `validator:` namespace) and flat configuration dictionaries.
    """

    def __init__(self, config_dict: dict[str, Any] | None = None) -> None:
        self.raw_config = config_dict or {}
        raw_validator = self.raw_config.get("validator")
        self._validator_block: dict[str, Any] = (
            raw_validator if isinstance(raw_validator, dict) else {}
        )

        self.pipeline = self._parse_pipeline_config()
        self.global_target: GlobalTarget = self._parse_global_target()
        self.spill = self._parse_spill_config()

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "ValidatorConfig":
        """Loads and parses a `netra_config.yaml` file from disk."""
        with open(config_path, encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        if loaded is not None and not isinstance(loaded, dict):
            raise ValueError("Configuration YAML root must be a key-value mapping.")
        return cls(loaded)

    def _parse_pipeline_config(self) -> PipelineConfig:
        root_pipeline = self.raw_config.get("pipeline")
        validator_pipeline = self._validator_block.get("pipeline")

        merged_pipeline: dict[str, Any] = {}
        if isinstance(root_pipeline, dict):
            merged_pipeline.update(root_pipeline)
        if isinstance(validator_pipeline, dict):
            merged_pipeline.update(validator_pipeline)

        return PipelineConfig(merged_pipeline)

    def _parse_global_target(self) -> GlobalTarget:
        raw_target = self._validator_block.get(
            "global_target", self.raw_config.get("global_target")
        )
        if raw_target is None:
            return "raw"

        normalized = str(raw_target).strip().lower()
        if normalized == "validated":
            return "validated"
        if normalized == "raw":
            return "raw"
        raise ValueError(f"Invalid `global_target`: '{raw_target}'. Must be 'raw' or 'validated'.")

    def _parse_spill_config(self) -> SpillConfig:
        spill_block = self._validator_block.get(
            "spill_to_disk", self.raw_config.get("spill_to_disk")
        )
        if not isinstance(spill_block, dict):
            return SpillConfig()

        temp_dir_str = spill_block.get("temp_directory", "/tmp/netra_spill")

        return SpillConfig(
            enabled=bool(spill_block.get("enabled", False)),
            memory_limit_mb=max(256, int(spill_block.get("memory_limit_mb", 16384))),
            temp_directory=Path(temp_dir_str),
            chunk_size=max(1_000, int(spill_block.get("chunk_size", 50_000))),
            min_spill_space_gb=max(0, int(spill_block.get("min_spill_space_gb", 10))),
        )
