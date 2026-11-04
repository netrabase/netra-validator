"""
Polars AST Builder (Pass 1 Compiler).

This module compiles the strictly typed DataContract into a set of row-level
Polars Expressions (pl.Expr). These expressions evaluate sequentially against
streaming data chunks to generate a boolean mask, which the Orchestrator uses
to bifurcate valid records from quarantined records without triggering OOMs.
"""

import polars as pl

from netra_validator.parser.models import DataContract, MetricQualityRule, Property


def _compile_property_rules(schema_property: Property) -> dict[str, pl.Expr]:
    """Compiles all row-level Polars expressions for a single schema column."""
    column_rules: dict[str, pl.Expr] = {}
    column = pl.col(schema_property.name)

    # 1. Completeness Check (Blocks NULLs)
    if schema_property.required:
        column_rules["required"] = column.is_not_null()

    # 2. Physical & Formatting Constraints
    if schema_property.logical_type_options:
        type_constraints = schema_property.logical_type_options

        if type_constraints.minimum is not None:
            column_rules[f"minimum: {type_constraints.minimum}"] = (
                column >= type_constraints.minimum
            )
        if type_constraints.maximum is not None:
            column_rules[f"maximum: {type_constraints.maximum}"] = (
                column <= type_constraints.maximum
            )
        if type_constraints.min_length is not None:
            column_rules[f"minLength: {type_constraints.min_length}"] = (
                column.str.len_chars() >= type_constraints.min_length
            )
        if type_constraints.max_length is not None:
            column_rules[f"maxLength: {type_constraints.max_length}"] = (
                column.str.len_chars() <= type_constraints.max_length
            )
        if type_constraints.pattern is not None:
            column_rules["pattern"] = column.str.contains(type_constraints.pattern)

    # 3. Categorical Metrics (Checks for values outside the allowed list)
    if schema_property.quality:
        for rule in schema_property.quality:
            if (
                isinstance(rule, MetricQualityRule)
                and rule.metric == "invalidValues"
                and rule.arguments
                and rule.arguments.valid_values
            ):
                column_rules["invalidValues"] = column.is_in(rule.arguments.valid_values)

    return column_rules


def build_row_level_expressions(contract: DataContract) -> dict[str, dict[str, pl.Expr]]:
    """
    Translates column-level ODCS constraints into Polars boolean expressions.

    Args:
        contract: The validated Pydantic DataContract object.

    Returns:
        A dictionary mapping column names to a dictionary of {rule_name: pl.Expr}.
        Each expression is designed to return True if the row passes the rule.
    """

    expressions: dict[str, dict[str, pl.Expr]] = {}
    if not contract.schema_block:
        return expressions

    for schema_property in contract.schema_block[0].properties:
        if column_rules := _compile_property_rules(schema_property):
            expressions[schema_property.name] = column_rules

    return expressions
