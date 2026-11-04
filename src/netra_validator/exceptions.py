"""
Custom Exception Hierarchy for Netra Validator.

This module defines the error space. By wrapping raw File I/O, PyYAML,
and Pydantic exceptions into custom Netra exceptions, we decouple our
API from third-party libraries and provide structured error payloads for
orchestrators (e.g., Airflow, Dagster, Prefect) and the CLI UX layer.
"""

from pydantic_core import ErrorDetails


class NetraValidatorError(Exception):
    """Base exception for all Netra Validator runtime and configuration errors."""


class ResourceNotFoundError(NetraValidatorError, FileNotFoundError):
    """Base exception for any file or resource that cannot be found on disk."""


class DatasetNotFoundError(ResourceNotFoundError):
    """Raised when the specified target dataset file does not exist."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Target dataset '{path}' not found.")


class ConfigNotFoundError(ResourceNotFoundError):
    """Raised when the specified custom configuration file does not exist."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"System configuration file '{path}' not found.")


class ContractError(NetraValidatorError):
    """Base exception for all ODCS contract ingestion and parsing failures."""


class ContractNotFoundError(ContractError, ResourceNotFoundError):
    """Raised when the specified contract YAML file does not exist at source."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Data contract file '{path}' not found.")


class ContractSyntaxError(ContractError):
    """Raised when the YAML file is malformed and cannot be parsed into a dictionary."""

    def __init__(self, path: str, details: str) -> None:
        self.path = path
        self.details = details
        super().__init__(f"Failed to parse YAML contract at '{path}': {details}")


class ContractValidationError(ContractError):
    """
    Raised when the contract YAML violates the ODCS v3.1.0 schema specification.

    Attributes:
        path: Path to the failing contract file.
        errors: List of human-readable, formatted field validation errors.
        raw_pydantic_errors: The raw Pydantic error dictionaries for programmatic inspection.
    """

    def __init__(
        self, path: str, errors: list[str], raw_pydantic_errors: list[ErrorDetails]
    ) -> None:
        self.path = path
        self.errors = errors
        self.raw_pydantic_errors = raw_pydantic_errors

        summary = "\n".join(f" - {error}" for error in errors)
        super().__init__(
            f"Contract at '{path}' failed ODCS v3.1.0 specification checks:\n{summary}"
        )


class ExecutionEngineError(NetraValidatorError):
    """Base exception for errors occurring during engine execution (Pass 1 or Pass 2)."""


class ResourcePreconditionError(NetraValidatorError):
    """
    Raised when hardware or OS constraints fail pre-flight checks before execution.
    For example: insufficient disk space for out-of-core memory spilling.
    """
