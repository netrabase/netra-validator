"""
Unit Tests for the Polars (Pass 1) and DuckDB (Pass 2) AST Compilers.

Evaluates compiled Polars expressions against in-memory DataFrames and verifies
SQL query templating, Primary Key duplicate generation, and severity propagation.
"""

from copy import deepcopy
from typing import Any

import polars as pl

from netra_validator.compiler.duckdb_builder import build_global_sql_rules
from netra_validator.compiler.polars_builder import build_row_level_expressions
from netra_validator.parser.models import DataContract


def test_polars_builder_compiles_expected_rule_tree(
    sample_contract: DataContract,
    sample_telemetry_df: pl.DataFrame,
) -> None:
    """
    Verifies that `build_row_level_expressions` generates exact boolean masks
    when evaluated against the synthetic 100-row telemetry DataFrame.
    """
    ast = build_row_level_expressions(sample_contract)

    assert set(ast.keys()) == {
        "packet_id",
        "station_id",
        "ambient_temp_c",
        "antenna_type",
        "rfi_flag",
    }

    # Evaluate compiled expressions directly on sample_telemetry_df (with null->False)
    eval_exprs: list[pl.Expr] = []
    for col_name, rules in ast.items():
        for rule_name, expr in rules.items():
            eval_exprs.append(expr.fill_null(False).alias(f"{col_name}__{rule_name}"))

    evaluated = sample_telemetry_df.select(eval_exprs)

    # 1. station_id checks:
    # Row 0 is None -> fails required, minLength, maxLength, pattern
    # Row 1 is "BAD" -> fails minLength (2 total), pattern (3 total)
    # Row 2 is "CS001_LONG" -> fails maxLength (2 total), pattern (3 total)
    assert (~evaluated["station_id__required"]).sum() == 1
    assert (~evaluated["station_id__minLength: 5"]).sum() == 2
    assert (~evaluated["station_id__maxLength: 5"]).sum() == 2
    assert (~evaluated["station_id__pattern"]).sum() == 3

    # 2. ambient_temp_c checks:
    # Row 3 is -30.0 (< -25.0); Row 4 is 60.0 (> 50.0)
    assert (~evaluated["ambient_temp_c__minimum: -25.0"]).sum() == 1
    assert (~evaluated["ambient_temp_c__maximum: 50.0"]).sum() == 1

    # 3. antenna_type categorical check:
    # Row 5 is "INVALID" (not in ["LBA", "HBA"])
    assert (~evaluated["antenna_type__invalidValues"]).sum() == 1


def test_polars_builder_empty_schema_and_unconstrained_columns(
    base_contract_dict: dict[str, Any],
) -> None:
    """Verifies safe handling of empty schema blocks and unconstrained columns."""
    empty_payload = deepcopy(base_contract_dict)
    empty_payload["schema"] = []
    empty_contract = DataContract.model_validate(empty_payload)
    assert build_row_level_expressions(empty_contract) == {}

    unconstrained_payload = deepcopy(base_contract_dict)
    unconstrained_payload["schema"][0]["properties"] = [
        {
            "id": "prop_notes",
            "name": "notes",
            "logicalType": "string",
            "physicalType": "utf8",
            "required": False,
        }
    ]
    unconstrained_contract = DataContract.model_validate(unconstrained_payload)
    assert build_row_level_expressions(unconstrained_contract) == {}


def test_duckdb_builder_compiles_pk_and_custom_sql_rules(
    base_contract_dict: dict[str, Any],
) -> None:
    """
    Verifies PK duplicate query generation, `{object}` and `{property}` template
    interpolation, fallback rule naming, and severity normalization.
    """
    payload = deepcopy(base_contract_dict)
    # Inject a column-level SQL rule using {object}, {property}, and lowercase severity
    payload["schema"][0]["properties"][2]["quality"] = [
        {
            "id": "temp_null_ratio",
            "type": "sql",
            "query": "SELECT COUNT(*) FROM {object} WHERE {property} IS NULL",
            "mustBeEqual": 0.0,
            "severity": "warning",
        }
    ]

    contract = DataContract.model_validate(payload)
    compiled_rules = build_global_sql_rules(contract, table_name="custom_view")

    assert len(compiled_rules) == 4

    # Rule 0: Auto-generated Primary Key uniqueness check
    pk_rule = compiled_rules[0]
    assert pk_rule.name == "packet_id_is_unique"
    assert "SELECT packet_id FROM custom_view" in pk_rule.query
    assert "GROUP BY packet_id HAVING COUNT(*) > 1" in pk_rule.query
    assert pk_rule.must_be_equal == 0.0
    assert pk_rule.severity == "CRITICAL"

    # Rule 1: Column-level custom SQL rule
    col_sql_rule = compiled_rules[1]
    assert col_sql_rule.name == "temp_null_ratio"
    assert col_sql_rule.query == "SELECT COUNT(*) FROM custom_view WHERE ambient_temp_c IS NULL"
    assert col_sql_rule.severity == "WARNING"

    # Rule 2 & 3: Table-level custom SQL rules
    assert compiled_rules[2].name == "max_temp_spike_check"
    assert "FROM custom_view" in compiled_rules[2].query
    assert compiled_rules[2].severity == "CRITICAL"

    assert compiled_rules[3].name == "lba_station_warning"
    assert compiled_rules[3].must_be_less_than == 50.0
    assert compiled_rules[3].severity == "WARNING"


def test_duckdb_builder_empty_schema(base_contract_dict: dict[str, Any]) -> None:
    """Verifies that an empty schema block returns an empty list of SQL rules."""
    payload = deepcopy(base_contract_dict)
    payload["schema"] = []
    contract = DataContract.model_validate(payload)

    assert build_global_sql_rules(contract) == []
