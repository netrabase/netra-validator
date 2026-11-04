"""
Open Data Contract Standard (ODCS) Reader & Ingestion Engine.

This module handles loading contract YAML files from source and validating them
against the `DataContract` Pydantic models. It intercepts raw syntax and schema
errors and converts them into structured `netra_validator.exceptions` errors.
"""

from pathlib import Path

import yaml
from pydantic import ValidationError
from pydantic_core import ErrorDetails

from netra_validator.exceptions import (
    ContractNotFoundError,
    ContractSyntaxError,
    ContractValidationError,
)
from netra_validator.parser.models import DataContract


def _format_pydantic_error(error: ErrorDetails) -> str:
    """
    Translates a raw Pydantic error dictionary into a clean, human-readable string.
    E.g., "schema -> properties -> item_price -> minimum: Input should be a valid number"
    """

    loc = " -> ".join(str(item) for item in error.get("loc", []))
    message = error.get("msg", "Validation error")
    return f"Field '{loc}': {message}"


def read_contract(contract_path: str | Path) -> DataContract:
    """
    Ingests an ODCS YAML file and validates it against the ODCS v3.1.0 schema.

    Args:
        contract_path: Path to the target contract YAML file.

    Returns:
        A strictly typed `DataContract` Pydantic object ready for query compilation.

    Raises:
        ContractNotFoundError: If the file does not exist at source.
        ContractSyntaxError: If the YAML structure is invalid or unparseable.
        ContractValidationError: If the contents violate the ODCS v3.1.0 schema.
    """

    path = Path(contract_path)

    # 1. File Existence Check
    if not path.is_file():
        raise ContractNotFoundError(str(path))

    # 2. YAML Syntax Loading
    try:
        with open(path, encoding="utf-8") as f:
            raw_dict = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ContractSyntaxError(str(path), details=str(e)) from e
    except Exception as e:
        raise ContractSyntaxError(str(path), details=f"IO Error: {e}") from e

    if not isinstance(raw_dict, dict):
        raise ContractSyntaxError(
            str(path), details="YAML document root must be a key-value mapping (dict)."
        )

    # 3. Pydantic Schema Validation
    try:
        return DataContract.model_validate(raw_dict)
    except ValidationError as e:
        raw_errors = e.errors()
        formatted_errors = [_format_pydantic_error(err) for err in raw_errors]

        raise ContractValidationError(
            path=str(path),
            errors=formatted_errors,
            raw_pydantic_errors=raw_errors,
        ) from e
