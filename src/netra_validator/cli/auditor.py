"""
Business logic for pre-flight environment auditing and contract validation.
"""

import os
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from netra_validator.cli.models import ExecutionManifest
from netra_validator.compiler.duckdb_builder import build_global_sql_rules
from netra_validator.config import ValidatorConfig
from netra_validator.engine.duckdb_runner import validate_sql_rules_preflight
from netra_validator.exceptions import (
    ConfigNotFoundError,
    ContractNotFoundError,
    ContractSyntaxError,
    ContractValidationError,
    DatasetNotFoundError,
)
from netra_validator.parser.models import DataContract
from netra_validator.parser.reader import read_contract

ONE_GB = 1024
SAFE_RAM_UTILIZATION_RATIO = 0.80

# Estimated in-memory & hash-state expansion factor per on-disk file format
FORMAT_EXPANSION_FACTORS: dict[str, float] = {
    ".parquet": 5.2,
    ".json": 2.5,
    ".csv": 2.0,
    ".ipc": 1.5,
    ".arrow": 1.5,
}
DEFAULT_EXPANSION_FACTOR = 5.2


def _get_system_ram_mb() -> float:
    """Extracts total physical memory available on the host OS in MB."""
    if sys.platform == "win32":
        import psutil  # type: ignore # noqa: PLC0415

        return float(psutil.virtual_memory().total) / (1024 * 1024)

    return float(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")) / (1024 * 1024)


class GlobalTargetChoice(str, Enum):
    RAW = "raw"
    VALIDATED = "validated"


@dataclass
class RunContext:
    """Encapsulates the resolved CLI arguments and paths for the execution session."""

    dataset_path: Path
    contract_path: Path
    valid_path: Path
    quarantine_path: Path
    config_path: Path | None
    global_target: GlobalTargetChoice | None
    force: bool


class PreFlightAuditor:
    """
    Evaluates system I/O, parses the data contract, and calculates hardware heuristics
    prior to running the validation engine.
    """

    def __init__(self, context: RunContext) -> None:
        self.context = context
        self.errors: list[tuple[str, str]] = []
        self.warnings: list[str] = []
        self.loaded_config = ValidatorConfig()
        self._contract_payload: DataContract | None = None

    def _check_dataset(self) -> tuple[str, bool, float]:
        try:
            if not self.context.dataset_path.exists():
                raise DatasetNotFoundError(self.context.dataset_path.name)

            size_mb = os.path.getsize(self.context.dataset_path) / (1024 * 1024)
            dataset_meta = f"{size_mb / 1024:.2f} GB" if size_mb > ONE_GB else f"{size_mb:.2f} MB"
            return dataset_meta, False, size_mb

        except DatasetNotFoundError as e:
            self.errors.append((e.__class__.__name__, str(e)))
            return "NOT FOUND", True, 0.0

    def _check_contract(self) -> tuple[str, bool]:
        try:
            self._contract_payload = read_contract(self.context.contract_path)
            return f"v{self._contract_payload.version}", False

        except ContractNotFoundError as e:
            self.errors.append((e.__class__.__name__, str(e)))
            return "NOT FOUND", True

        except ContractSyntaxError as e:
            self.errors.append((e.__class__.__name__, str(e)))
            return "SYNTAX ERR", True

        except ContractValidationError as e:
            for schema_error in e.errors:
                clean_error = schema_error.removeprefix("- ").replace(": Value error, ", ": ")
                self.errors.append(("ContractValidationError", clean_error))
            return "INVALID", True

    def _check_config(self) -> tuple[str, str, bool]:
        if not self.context.config_path:
            return "Default", "FACTORY", False

        config_source = self.context.config_path.name
        try:
            if not self.context.config_path.exists():
                raise ConfigNotFoundError(config_source)
            self.loaded_config = ValidatorConfig.from_yaml(self.context.config_path)
            return config_source, "CUSTOM", False

        except ConfigNotFoundError as e:
            self.errors.append((e.__class__.__name__, str(e)))
            return config_source, "NOT FOUND", True
        except Exception as e:
            self.errors.append(("ConfigParseError", str(e)))
            return config_source, "INVALID", True

    def _evaluate_heuristics(self, size_mb: float) -> tuple[str, str, bool]:
        if self.loaded_config.spill.enabled:
            limit_gb = self.loaded_config.spill.memory_limit_mb / ONE_GB
            return "SPILL TO DISK", f"{limit_gb:.1f} GB LIMIT", False

        ext = self.context.dataset_path.suffix.lower()
        expansion_factor = FORMAT_EXPANSION_FACTORS.get(ext, DEFAULT_EXPANSION_FACTOR)
        estimated_footprint_mb = size_mb * expansion_factor
        safe_ram_threshold_mb = _get_system_ram_mb() * SAFE_RAM_UTILIZATION_RATIO

        if estimated_footprint_mb >= safe_ram_threshold_mb:
            est_gb = estimated_footprint_mb / ONE_GB
            total_gb = _get_system_ram_mb() / ONE_GB
            self.warnings.append(
                f"Estimated in-memory footprint (~{est_gb:.1f} GB) exceeds 80% of system RAM "
                f"({total_gb:.1f} GB) for IN-MEMORY STREAMING. Risk of severe latency or OOM crash."
            )
            return "IN-MEMORY STREAMING", "OOM RISK", True

        return "IN-MEMORY STREAMING", "DEFAULT", False

    def execute(self) -> ExecutionManifest:
        dataset_meta, dataset_critical, size_mb = self._check_dataset()
        contract_meta, contract_critical = self._check_contract()
        config_source, config_meta, config_critical = self._check_config()

        if not dataset_critical and not contract_critical and self._contract_payload is not None:
            sql_rules = build_global_sql_rules(self._contract_payload)
            sql_errors = validate_sql_rules_preflight(self.context.dataset_path, sql_rules)
            if sql_errors:
                contract_meta = "SQL_ERR"
                contract_critical = True
                for rule_name, err_msg in sql_errors:
                    self.errors.append((f"SQL Error ({rule_name})", err_msg))

        if self.context.global_target is not None:
            self.loaded_config.global_target = (
                "validated" if self.context.global_target == GlobalTargetChoice.VALIDATED else "raw"
            )

        active_target = self.loaded_config.global_target
        global_target_meta = "DEFAULT" if active_target == "raw" else "POST-FILTER"

        validated_protocol, validated_write_mode = "LOCAL", "APPEND"
        quarantine_protocol, quarantine_write_mode = "LOCAL", "OVERWRITE"

        engine_mode, engine_meta, engine_warning = self._evaluate_heuristics(size_mb)

        return ExecutionManifest(
            dataset_path=os.path.relpath(self.context.dataset_path),
            dataset_meta=dataset_meta,
            dataset_critical_error=dataset_critical,
            contract_path=os.path.relpath(self.context.contract_path),
            contract_meta=contract_meta,
            contract_critical=contract_critical,
            config_source=config_source,
            config_meta=config_meta,
            config_critical=config_critical,
            global_target=active_target.upper(),
            global_target_meta=global_target_meta,
            validated_path=str(self.context.valid_path),
            validated_protocol=validated_protocol,
            validated_write_mode=validated_write_mode,
            quarantine_path=str(self.context.quarantine_path),
            quarantine_protocol=quarantine_protocol,
            quarantine_write_mode=quarantine_write_mode,
            engine_mode=engine_mode,
            engine_meta=engine_meta,
            engine_warning=engine_warning,
            errors=self.errors,
            warnings=self.warnings,
        )
