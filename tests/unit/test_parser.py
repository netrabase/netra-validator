"""
Unit Tests for the ODCS v3.1.0 Parser & Pydantic Schema Layer (`netra_validator.parser`).

Verifies strict execution-block validation, governance-block leniency,
model immutability, boolean vs. integer type preservation, and custom error routing.
"""

from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from netra_validator.exceptions import (
    ContractNotFoundError,
    ContractSyntaxError,
    ContractValidationError,
)
from netra_validator.parser.models import (
    DataContract,
    MetricQualityRule,
    SqlQualityRule,
)
from netra_validator.parser.reader import read_contract


def test_read_valid_contract(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
) -> None:
    """Verifies end-to-end YAML ingestion and polymorphic quality rule routing."""
    contract_path = contract_factory(None, "valid_contract.yaml")
    contract = read_contract(contract_path)

    assert contract.api_version == "v3.1.0"
    assert contract.kind == "DataContract"
    assert contract.dataset.name == "lofar_station_telemetry"
    assert len(contract.schema_block) == 1

    schema = contract.schema_block[0]
    assert len(schema.properties) == 5
    assert schema.quality is not None
    assert len(schema.quality) == 2
    assert isinstance(schema.quality[0], SqlQualityRule)
    assert schema.quality[0].severity == "CRITICAL"
    assert isinstance(schema.quality[1], SqlQualityRule)
    assert schema.quality[1].severity == "WARNING"


def test_governance_leniency_ignores_extra_root_and_dataset_fields(
    base_contract_dict: dict[str, Any],
) -> None:
    """
    Verifies that enterprise governance blocks (e.g., `servers`, `roles`, `sla`)
    at the root and dataset levels are ignored without failing validation.
    """
    payload = deepcopy(base_contract_dict)
    payload["servers"] = [{"environment": "prod", "type": "s3"}]
    payload["roles"] = [{"role": "astron_admin"}]
    payload["dataset"]["customGovernanceTag"] = "PII_FREE"

    contract = DataContract.model_validate(payload)
    assert contract.dataset.name == "lofar_station_telemetry"


def test_execution_strictness_forbids_typos_in_schema_and_rules(
    contract_factory: Callable[[dict[str, Any] | None, str], Path],
    base_contract_dict: dict[str, Any],
) -> None:
    """
    Verifies that `extra='forbid'` catches misspelled keys inside execution blocks
    and formats the Pydantic error trace into a `ContractValidationError`.
    """
    payload = deepcopy(base_contract_dict)
    # Inject a typo into logicalTypeOptions ('minimus' instead of 'minimum')
    payload["schema"][0]["properties"][2]["logicalTypeOptions"]["minimus"] = -10.0

    bad_path = contract_factory(payload, "typo_contract.yaml")

    with pytest.raises(ContractValidationError) as exc_info:
        read_contract(bad_path)

    err = exc_info.value
    assert err.path == str(bad_path)
    assert any("minimus" in msg for msg in err.errors)
    assert len(err.raw_pydantic_errors) > 0


def test_contract_models_are_immutable(sample_contract: DataContract) -> None:
    """Verifies that parsed DataContract instances are frozen and cannot be mutated."""
    with pytest.raises(ValidationError):
        sample_contract.version = "9.9.9"


def test_quality_arguments_boolean_and_numeric_preservation(
    base_contract_dict: dict[str, Any],
) -> None:
    """Verifies that `validValues` preserves distinct bool, int, float, and str types."""
    payload = deepcopy(base_contract_dict)
    payload["schema"][0]["properties"][4]["quality"] = [
        {
            "metric": "invalidValues",
            "arguments": {"validValues": [True, False, 1, 0.5, "FLAG"]},
        }
    ]

    contract = DataContract.model_validate(payload)
    rfi_prop = contract.schema_block[0].properties[4]
    assert rfi_prop.quality is not None
    rule = rfi_prop.quality[0]
    assert isinstance(rule, MetricQualityRule)
    assert rule.arguments is not None
    assert rule.arguments.valid_values is not None
    assert rule.arguments.valid_values == [True, False, 1, 0.5, "FLAG"]
    assert isinstance(rule.arguments.valid_values[0], bool)


def test_read_contract_missing_file_and_directory_guard(tmp_path: Path) -> None:
    """
    Verifies that both non-existent paths and directory paths raise
    `ContractNotFoundError` rather than generic IO/syntax errors.
    """
    missing_file = tmp_path / "does_not_exist.yaml"
    with pytest.raises(ContractNotFoundError):
        read_contract(missing_file)

    # Passing a directory instead of a file must also trigger ContractNotFoundError
    with pytest.raises(ContractNotFoundError):
        read_contract(tmp_path)


def test_read_contract_malformed_yaml_and_non_dict_root(tmp_path: Path) -> None:
    """Verifies that broken YAML syntax or list-root YAML files raise `ContractSyntaxError`."""
    corrupt_yaml = tmp_path / "corrupt.yaml"
    corrupt_yaml.write_text("apiVersion: v3.1.0\nschema: [unclosed_list", encoding="utf-8")

    with pytest.raises(ContractSyntaxError, match="Failed to parse YAML"):
        read_contract(corrupt_yaml)

    list_root_yaml = tmp_path / "list_root.yaml"
    list_root_yaml.write_text("- item_1\n- item_2\n", encoding="utf-8")

    with pytest.raises(ContractSyntaxError, match="must be a key-value mapping"):
        read_contract(list_root_yaml)
