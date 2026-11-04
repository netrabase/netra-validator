"""
Unit Tests for the Universal Configuration Manager (`netra_validator.config`).

Verifies default initialization, cascading `validator:` namespace resolution,
CI/CD quality gate state inference, resilience against null YAML sub-blocks,
and lower-bound hardware safety clamps.
"""

from pathlib import Path
from typing import Any

import pytest
import yaml

from netra_validator.config import SpillConfig, ValidatorConfig


def test_default_config_initialization() -> None:
    """Verifies factory defaults when no configuration dictionary is provided."""
    config = ValidatorConfig()

    assert config.pipeline.fail_on_critical is False
    assert config.pipeline.fail_on_warnings is False
    assert config.pipeline.quality_gate_active is False
    assert config.global_target == "raw"

    assert config.spill == SpillConfig(
        enabled=False,
        memory_limit_mb=16384,
        temp_directory=Path("/tmp/netra_spill"),
        chunk_size=50_000,
        min_spill_space_gb=10,
    )


@pytest.mark.parametrize(
    ("pipeline_subblock", "expected_gate"),
    [
        ({"fail_on_critical": False, "fail_on_warnings": False}, False),
        ({"fail_on_critical": True, "fail_on_warnings": False}, True),
        ({"fail_on_critical": False, "fail_on_warnings": True}, True),
        ({"fail_on_critical": True, "fail_on_warnings": True}, True),
        (
            {
                "fail_on_critical": True,
                "fail_on_warnings": True,
                "quality_gate_active": False,
            },
            False,
        ),
        (
            {
                "fail_on_critical": False,
                "fail_on_warnings": False,
                "quality_gate_active": True,
            },
            True,
        ),
    ],
)
def test_pipeline_quality_gate_inference(
    pipeline_subblock: dict[str, bool], expected_gate: bool
) -> None:
    """Verifies that `quality_gate_active` auto-arms when severity flags are enabled."""
    config = ValidatorConfig({"pipeline": pipeline_subblock})
    assert config.pipeline.quality_gate_active is expected_gate


def test_cascading_validator_namespace_and_yaml_loading(tmp_path: Path) -> None:
    """
    Verifies that the `validator:` namespace in `netra_config.yaml` configures
    `global_target`, `spill_to_disk`, and cascades over root `pipeline:` defaults.
    """
    yaml_payload: dict[str, Any] = {
        "pipeline": {
            "fail_on_critical": False,
            "fail_on_warnings": False,
        },
        "validator": {
            "global_target": "validated",
            "pipeline": {
                "fail_on_critical": True,
            },
            "spill_to_disk": {
                "enabled": True,
                "memory_limit_mb": 8192,
                "chunk_size": 25_000,
            },
        },
    }
    cfg_file = tmp_path / "netra_config.yaml"
    cfg_file.write_text(yaml.safe_dump(yaml_payload), encoding="utf-8")

    config = ValidatorConfig.from_yaml(cfg_file)

    assert config.global_target == "validated"
    assert config.pipeline.fail_on_critical is True
    assert config.pipeline.fail_on_warnings is False
    assert config.pipeline.quality_gate_active is True
    assert config.spill.enabled is True
    assert config.spill.memory_limit_mb == 8192
    assert config.spill.chunk_size == 25_000


def test_invalid_global_target_raises_value_error() -> None:
    """Verifies that an unrecognized `global_target` raises ValueError."""
    with pytest.raises(ValueError, match="Invalid `global_target`"):
        ValidatorConfig({"validator": {"global_target": "invalid_mode"}})


def test_null_yaml_subblocks_resilience() -> None:
    """
    Verifies that empty YAML keys (which PyYAML parses as `None`) degrade
    gracefully to default configurations without raising AttributeError.
    """
    raw_yaml_dict: dict[str, Any] = {
        "pipeline": None,
        "validator": None,
        "spill_to_disk": None,
    }
    config = ValidatorConfig(raw_yaml_dict)

    assert config.pipeline.quality_gate_active is False
    assert config.global_target == "raw"
    assert config.spill.enabled is False
    assert config.spill.memory_limit_mb == 16384


def test_spill_config_custom_values_and_floor_clamping() -> None:
    """
    Verifies custom spill settings and ensures sub-minimum values
    are clamped to safe operational floors.
    """
    underflow_config = ValidatorConfig(
        {
            "validator": {
                "spill_to_disk": {
                    "enabled": True,
                    "memory_limit_mb": 16,
                    "chunk_size": 50,
                    "min_spill_space_gb": -10,
                    "temp_directory": "/mnt/nvme_scratch",
                }
            }
        }
    )

    assert underflow_config.spill.enabled is True
    assert underflow_config.spill.memory_limit_mb == 256
    assert underflow_config.spill.chunk_size == 1_000
    assert underflow_config.spill.min_spill_space_gb == 0
    assert underflow_config.spill.temp_directory == Path("/mnt/nvme_scratch")
