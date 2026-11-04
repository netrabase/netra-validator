"""
Data models representing the application state for the Netra Validator CLI.
"""

from dataclasses import dataclass, field


@dataclass
class ExecutionManifest:
    """Holds the final configuration and diagnostic data checked before the engine starts."""

    dataset_path: str
    dataset_meta: str
    dataset_critical_error: bool
    contract_path: str
    contract_meta: str
    contract_critical: bool
    config_source: str
    config_meta: str
    config_critical: bool
    global_target: str
    global_target_meta: str
    validated_path: str
    validated_protocol: str
    validated_write_mode: str
    quarantine_path: str
    quarantine_protocol: str
    quarantine_write_mode: str
    engine_mode: str
    engine_meta: str
    engine_warning: bool
    errors: list[tuple[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
