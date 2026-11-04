"""
Global Pytest Fixtures and Ephemeral Data Factories.

Synthesizes deterministic telemetry datasets and ODCS v3.1.0 contracts
entirely in memory and materializes them into pytest's temporary directories
on demand. Zero binary test artifacts are stored in version control.
"""

from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from netra_validator.parser.models import DataContract

# Constants for the 100-row synthetic telemetry dataset
TOTAL_SAMPLE_ROWS = 100
EXPECTED_VALID_ROWS = 94
EXPECTED_QUARANTINED_ROWS = 6
EXPECTED_DUPLICATE_PK_GROUPS = 2


@pytest.fixture
def base_contract_dict() -> dict[str, Any]:
    """
    Returns a valid, mutable ODCS v3.1.0 dictionary blueprint.
    Individual tests can mutate specific keys before validation or YAML serialization.
    """
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "status": "active",
        "dataset": {
            "name": "lofar_station_telemetry",
            "domain": "radio_astronomy",
            "owner": "netrabase",
        },
        "schema": [
            {
                "name": "telemetry_stream",
                "logicalType": "object",
                "properties": [
                    {
                        "id": "prop_packet_id",
                        "name": "packet_id",
                        "logicalType": "integer",
                        "physicalType": "int64",
                        "required": True,
                        "primaryKey": True,
                    },
                    {
                        "id": "prop_station_id",
                        "name": "station_id",
                        "logicalType": "string",
                        "physicalType": "utf8",
                        "required": True,
                        "logicalTypeOptions": {
                            "minLength": 5,
                            "maxLength": 5,
                            "pattern": r"^CS\d{3}$",
                        },
                    },
                    {
                        "id": "prop_ambient_temp_c",
                        "name": "ambient_temp_c",
                        "logicalType": "number",
                        "physicalType": "float64",
                        "required": True,
                        "logicalTypeOptions": {
                            "minimum": -25.0,
                            "maximum": 50.0,
                        },
                    },
                    {
                        "id": "prop_antenna_type",
                        "name": "antenna_type",
                        "logicalType": "string",
                        "physicalType": "utf8",
                        "required": True,
                        "quality": [
                            {
                                "metric": "invalidValues",
                                "arguments": {"validValues": ["LBA", "HBA"]},
                                "mustBeEqual": 0.0,
                            }
                        ],
                    },
                    {
                        "id": "prop_rfi_flag",
                        "name": "rfi_flag",
                        "logicalType": "boolean",
                        "physicalType": "bool",
                        "required": True,
                    },
                ],
                "quality": [
                    {
                        "name": "max_temp_spike_check",
                        "type": "sql",
                        "query": "SELECT COUNT(*) FROM {object} WHERE ambient_temp_c > 45.0",
                        "mustBeEqual": 0.0,
                        "severity": "CRITICAL",
                    },
                    {
                        "name": "lba_station_warning",
                        "type": "sql",
                        "query": "SELECT COUNT(*) FROM {object} WHERE antenna_type = 'LBA'",
                        "mustBeLessThan": 50.0,
                        "severity": "WARNING",
                    },
                ],
            }
        ],
    }


@pytest.fixture
def sample_contract(base_contract_dict: dict[str, Any]) -> DataContract:
    """Returns a validated, immutable DataContract instance for zero-I/O unit tests."""

    return DataContract.model_validate(base_contract_dict)


@pytest.fixture
def contract_factory(
    tmp_path: Path, base_contract_dict: dict[str, Any]
) -> Callable[[dict[str, Any] | None, str], Path]:
    """
    Factory fixture that writes a contract dictionary (or the default base contract)
    to an ephemeral YAML file inside `tmp_path`.
    """

    def _create_contract(
        custom_dict: dict[str, Any] | None = None, filename: str = "contract.yaml"
    ) -> Path:
        payload = deepcopy(base_contract_dict) if custom_dict is None else custom_dict
        contract_path = tmp_path / filename
        with open(contract_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(payload, f, sort_keys=False)
        return contract_path

    return _create_contract


@pytest.fixture
def sample_telemetry_df() -> pl.DataFrame:
    """
    Generates a deterministic 100-row Polars DataFrame with exact injected anomalies:
    - Row 0: station_id is None (Fails required, minLength, maxLength, pattern)
    - Row 1: station_id is "BAD" (Fails minLength: 5, pattern)
    - Row 2: station_id is "CS001_LONG" (Fails maxLength: 5, pattern)
    - Row 3: ambient_temp_c is -30.0 (Fails minimum: -25.0)
    - Row 4: ambient_temp_c is 60.0 (Fails maximum: 50.0; triggers SQL max_temp_spike_check)
    - Row 5: antenna_type is "INVALID" (Fails invalidValues)
    - Rows 98 & 99: Duplicate packet_id values (10 and 11) inside valid rows.
    - Column `untracked_metric`: Present in physical schema but absent from contract.
    """

    packet_ids = list(range(TOTAL_SAMPLE_ROWS))
    packet_ids[98] = 10  # Duplicate PK group 1 (within valid rows)
    packet_ids[99] = 11  # Duplicate PK group 2 (within valid rows)

    station_ids: list[str | None] = ["CS001"] * TOTAL_SAMPLE_ROWS
    station_ids[0] = None
    station_ids[1] = "BAD"
    station_ids[2] = "CS001_LONG"

    temps = [20.0] * TOTAL_SAMPLE_ROWS
    temps[3] = -30.0
    temps[4] = 60.0

    antennas = ["HBA"] * TOTAL_SAMPLE_ROWS
    antennas[5] = "INVALID"

    rfi_flags = [False] * TOTAL_SAMPLE_ROWS
    untracked = [42] * TOTAL_SAMPLE_ROWS

    return pl.DataFrame(
        {
            "packet_id": pl.Series(packet_ids, dtype=pl.Int64),
            "station_id": pl.Series(station_ids, dtype=pl.String),
            "ambient_temp_c": pl.Series(temps, dtype=pl.Float64),
            "antenna_type": pl.Series(antennas, dtype=pl.String),
            "rfi_flag": pl.Series(rfi_flags, dtype=pl.Boolean),
            "untracked_metric": pl.Series(untracked, dtype=pl.Int64),
        }
    )


@pytest.fixture
def dataset_factory(
    tmp_path: Path, sample_telemetry_df: pl.DataFrame
) -> Callable[[str, pl.DataFrame | None], Path]:
    """
    Factory fixture that serializes a Polars DataFrame into an ephemeral file
    or partitioned directory in the requested format ('parquet', 'csv', 'ipc', 'json', 'dir').
    """

    def _create_dataset(fmt: str = "parquet", df: pl.DataFrame | None = None) -> Path:
        target_df = sample_telemetry_df if df is None else df
        normalized_fmt = fmt.lower().lstrip(".")

        if normalized_fmt == "parquet":
            out_path = tmp_path / "telemetry.parquet"
            target_df.write_parquet(out_path)
            return out_path

        if normalized_fmt == "csv":
            out_path = tmp_path / "telemetry.csv"
            target_df.write_csv(out_path)
            return out_path

        if normalized_fmt in {"ipc", "arrow"}:
            out_path = tmp_path / f"telemetry.{normalized_fmt}"
            target_df.write_ipc(out_path)
            return out_path

        if normalized_fmt == "json":
            out_path = tmp_path / "telemetry.json"
            target_df.write_ndjson(out_path)
            return out_path

        if normalized_fmt == "dir":
            dir_path = tmp_path / "partitioned_telemetry"
            dir_path.mkdir(parents=True, exist_ok=True)
            midpoint = max(1, target_df.height // 2)
            target_df.slice(0, midpoint).write_parquet(dir_path / "part_0.parquet")
            target_df.slice(midpoint).write_parquet(dir_path / "part_1.parquet")
            return dir_path

        raise ValueError(f"Unsupported test dataset format: {fmt}")

    return _create_dataset
