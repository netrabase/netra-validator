"""
Open Data Contract Standard (ODCS) Pydantic Models.

This module acts as the strict validation layer for the incoming
ODCS v3.1.0 YAML files. It enforces type safety, detects typos in execution blocks,
and gracefully ignores unneeded governance metadata (e.g., 'roles', 'servers').

The parsed models generated here serve as the immutable blueprint for both
the Polars (Row-Level) and DuckDB (Global) execution compilers.
"""

import re
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)

_PLACEHOLDER_PATTERN = re.compile(r"\{([^{}]+)\}")
_VALID_SQL_PLACEHOLDERS = {"object", "property"}


class LogicalTypeOptions(BaseModel):
    """
    Defines the physical bounds, limits, and patterns for a column.
    Used heavily by the Polars compiler for row-level streaming checks.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Numeric Bounds
    minimum: float | None = None
    maximum: float | None = None

    # String Length Bounds
    min_length: int | None = Field(default=None, alias="minLength")
    max_length: int | None = Field(default=None, alias="maxLength")

    # Pattern Matching
    pattern: str | None = None


class QualityArguments(BaseModel):
    """
    Optional arguments passed to specific ODCS metrics
    (e.g., 'validValues' for the 'invalidValues' metric).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    valid_values: list[str | bool | int | float] | None = Field(default=None, alias="validValues")


class BaseQualityRule(BaseModel):
    """Shared bounds for all quality rules, regardless of type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Identifiers
    id: str | None = None
    name: str | None = None
    description: str | None = None
    severity: Literal["CRITICAL", "WARNING", "critical", "warning"] = "CRITICAL"

    # Mathematical Evaluation Bounds
    must_be_less_than: float | None = Field(default=None, alias="mustBeLessThan")
    must_be_greater_than: float | None = Field(default=None, alias="mustBeGreaterThan")
    must_be_equal: float | None = Field(default=None, alias="mustBeEqual")
    must_be: float | None = Field(default=None, alias="mustBe")
    must_be_between: list[float] | None = Field(
        default=None, alias="mustBeBetween", max_length=2, min_length=2
    )


class SqlQualityRule(BaseQualityRule):
    """
    Represents an explicitly defined SQL execution rule.
    Routed directly to the DuckDB compiler (Pass 2).
    """

    type: Literal["sql"]
    query: str

    @field_validator("query")
    @classmethod
    def validate_sql_placeholders(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("SQL quality rule 'query' cannot be empty.")

        placeholders = set(_PLACEHOLDER_PATTERN.findall(value))
        invalid = sorted(placeholders - _VALID_SQL_PLACEHOLDERS)
        if invalid:
            formatted = ", ".join(f"'{{{p}}}'" for p in invalid)
            raise ValueError(
                f"Invalid placeholder(s) {formatted} in SQL query. "
                "ODCS v3.1.0 requires '{object}' (and '{property}' for column rules)."
            )
        return value


class MetricQualityRule(BaseQualityRule):
    """
    Represents a standard ODCS metric rule (e.g., 'nullValues', 'duplicateValues').
    Compiled dynamically to either Polars or DuckDB depending on the metric.
    """

    metric: str
    unit: str | None = None
    arguments: QualityArguments | None = None


# A polymorphic type that Pydantic will match against left-to-right.
# If it contains `type: sql`, it routes to SqlQualityRule. Otherwise, MetricQualityRule.
def _discriminate_quality_rule(v: Any) -> str:
    if isinstance(v, dict) and (v.get("type") == "sql" or "query" in v):
        return "sql"
    if isinstance(v, SqlQualityRule):
        return "sql"
    return "metric"


QualityRule = Annotated[
    Annotated[SqlQualityRule, Tag("sql")] | Annotated[MetricQualityRule, Tag("metric")],
    Discriminator(_discriminate_quality_rule),
]


class Property(BaseModel):
    """
    Represents a single column within the dataset schema.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    name: str
    logical_type: str = Field(alias="logicalType")
    physical_type: str = Field(alias="physicalType")

    # Structural Flags
    required: bool = False
    primary_key: bool = Field(default=False, alias="primaryKey")

    # Execution Constraints
    logical_type_options: LogicalTypeOptions | None = Field(
        default=None, alias="logicalTypeOptions"
    )
    quality: list[QualityRule] | None = None


class SchemaObject(BaseModel):
    """
    Represents the table schema and table-level constraints.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    logical_type: Literal["object"] = Field(default="object", alias="logicalType")
    properties: list[Property]
    quality: list[QualityRule] | None = None

    @model_validator(mode="after")
    def validate_table_level_sql_placeholders(self) -> "SchemaObject":
        if self.quality:
            for rule in self.quality:
                if isinstance(rule, SqlQualityRule) and "{property}" in rule.query:
                    rule_id = rule.name or rule.id or "unnamed_sql_rule"
                    raise ValueError(
                        f"Table-level SQL rule '{rule_id}' cannot use '{{property}}'; "
                        "use explicit column names or move the rule under a column property."
                    )
        return self


class DatasetMeta(BaseModel):
    """
    Metadata about the ingested dataset.
    """

    # We ignore extra fields here (like 'domain', 'owner') as they are purely governance.
    model_config = ConfigDict(extra="ignore", frozen=True)

    name: str


class DataContract(BaseModel):
    """
    The root schema for the ODCS v3.1.0 document.
    """

    # We will be lenient at the root level to allow full ODCS enterprise files without crashing.
    model_config = ConfigDict(extra="ignore", frozen=True)

    api_version: str = Field(alias="apiVersion")
    kind: Literal["DataContract"]
    version: str
    status: str | None = None

    dataset: DatasetMeta

    schema_block: list[SchemaObject] = Field(alias="schema")

    @model_validator(mode="before")
    @classmethod
    def reject_misindented_schema_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            for misplaced_key in ("quality", "properties"):
                if misplaced_key in data:
                    raise ValueError(
                        f"Found '{misplaced_key}' at the contract root. "
                        f"In ODCS v3.1.0, '{misplaced_key}' must be indented "
                        "inside a 'schema' object."
                    )
        return data
