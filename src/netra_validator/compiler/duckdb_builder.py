"""
DuckDB SQL Builder (Pass 2 Compiler).

This module extracts table-wide aggregations, cross-column logic, and
primary key constraints from the DataContract and compiles them into
fault-tolerant SQL strings targeting the DuckDB engine.
"""

from dataclasses import dataclass
from typing import Literal

from netra_validator.parser.models import DataContract, SqlQualityRule


@dataclass
class CompiledSQLRule:
    """
    Represents an actionable SQL query bounded by an expected mathematical result.
    """

    name: str
    query: str
    must_be_equal: float | None = None
    must_be_less_than: float | None = None
    must_be_greater_than: float | None = None
    must_be: float | None = None
    severity: Literal["CRITICAL", "WARNING"] = "CRITICAL"


def build_global_sql_rules(
    contract: DataContract, table_name: str = "target_dataset"
) -> list[CompiledSQLRule]:
    """
    Translates global ODCS constraints and custom queries into executable DuckDB SQL.

    Args:
        contract: The validated Pydantic DataContract object.
        table_name: The name of the DuckDB view or table executing the query.

    Returns:
        A list of `CompiledSQLRule` objects ready for out-of-core execution.
    """
    rules: list[CompiledSQLRule] = []
    if not contract.schema_block:
        return rules

    schema = contract.schema_block[0]

    # 1. Column-Level Global Rules (Primary Keys & Column SQL)
    for schema_property in schema.properties:
        # Auto-generate a duplicate counter query for Primary Keys
        if schema_property.primary_key:
            pk_query = (
                f"SELECT COUNT(*) FROM ("
                f"  SELECT {schema_property.name} FROM {table_name} "
                f"  GROUP BY {schema_property.name} HAVING COUNT(*) > 1"
                f") AS duplicates"
            )
            rules.append(
                CompiledSQLRule(
                    name=f"{schema_property.name}_is_unique",
                    query=pk_query,
                    must_be_equal=0.0,
                )
            )

        if schema_property.quality:
            for rule in schema_property.quality:
                # Isolate explicitly defined SQL Rules
                if isinstance(rule, SqlQualityRule):
                    formatted_query = rule.query.replace("{object}", table_name).replace(
                        "{property}", schema_property.name
                    )

                    rule_name = rule.name or rule.id or f"{schema_property.name}_custom_sql"
                    severity: Literal["CRITICAL", "WARNING"] = (
                        "WARNING" if rule.severity.upper() == "WARNING" else "CRITICAL"
                    )

                    rules.append(
                        CompiledSQLRule(
                            name=rule_name,
                            query=formatted_query,
                            must_be_equal=rule.must_be_equal,
                            must_be_less_than=rule.must_be_less_than,
                            must_be_greater_than=rule.must_be_greater_than,
                            must_be=rule.must_be,
                            severity=severity,
                        )
                    )

    # 2. Table-Level Global Rules (Cross-Column Integrity)
    if schema.quality:
        for rule in schema.quality:
            if isinstance(rule, SqlQualityRule):
                formatted_query = rule.query.replace("{object}", table_name)
                rule_name = rule.name or rule.id or "table_custom_sql"
                severity = "WARNING" if rule.severity.upper() == "WARNING" else "CRITICAL"

                rules.append(
                    CompiledSQLRule(
                        name=rule_name,
                        query=formatted_query,
                        must_be_equal=rule.must_be_equal,
                        must_be_less_than=rule.must_be_less_than,
                        must_be_greater_than=rule.must_be_greater_than,
                        must_be=rule.must_be,
                        severity=severity,
                    )
                )

    return rules
