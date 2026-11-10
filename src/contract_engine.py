"""
contract_engine.py - Data Contract Engine for Enterprise Financial Settlement Ledgers.

Responsibilities:
1. Load human-readable YAML contracts into strongly typed Pydantic v2 schemas.
2. Build Pydantic v2 record validation models dynamically or statically.
3. Compare incoming contracts against baseline contracts to classify drift:
   - BREAKING changes (dropped required fields, incompatible type conversions,
     removed currency codes, tightened nullability).
   - NON-BREAKING changes (new optional fields with defaults, relaxed constraints).
4. Inspect incoming batch DataFrames against contracts before processing.
5. Abort pipeline execution cleanly by raising SchemaDriftError on breaking changes.
"""

from __future__ import annotations

import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, Union

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, create_model, field_validator


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class SchemaDriftError(Exception):
    """Raised when a breaking schema drift is detected between contracts or batch data."""

    def __init__(self, message: str, diff: Optional[ContractDiff] = None):
        super().__init__(message)
        self.diff = diff
        self.message = message


# ==============================================================================
# 2. Data Contract Definitions (Pydantic v2)
# ==============================================================================

class ReferenceDefinition(BaseModel):
    table: str
    column: str


class FieldDefinition(BaseModel):
    name: str
    type: str  # string, float, double, integer, timestamp, boolean
    required: bool = True
    nullable: bool = False
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    default: Optional[Any] = None
    format: Optional[str] = None
    description: Optional[str] = None
    references: Optional[ReferenceDefinition] = None

    model_config = ConfigDict(extra="ignore")


class SchemaDefinition(BaseModel):
    fields: List[FieldDefinition]

    model_config = ConfigDict(extra="ignore")

    def get_field(self, name: str) -> Optional[FieldDefinition]:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    @property
    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    @property
    def required_field_names(self) -> List[str]:
        return [f.name for f in self.fields if f.required]


class DataContract(BaseModel):
    contract_name: str
    version: str
    domain: Optional[str] = "financial_services"
    owner: Optional[str] = None
    target_table: str
    description: Optional[str] = None
    schema_def: SchemaDefinition = Field(alias="schema")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def schema(self) -> SchemaDefinition:
        """Convenience property for accessing schema definition."""
        return self.schema_def

    @classmethod
    def from_yaml(cls, path_or_stream: Union[str, Path]) -> DataContract:
        """Loads and parses a YAML contract file into a validated DataContract model."""
        path = Path(path_or_stream)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found at: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)


# ==============================================================================
# 3. Schema Drift Classifier & Diff Models
# ==============================================================================

class ChangeType(str, Enum):
    BREAKING = "BREAKING"
    NON_BREAKING = "NON_BREAKING"


class SchemaChange(BaseModel):
    change_type: ChangeType
    category: str
    field_name: str
    description: str
    baseline_value: Optional[Any] = None
    incoming_value: Optional[Any] = None


class ContractDiff(BaseModel):
    baseline_version: str
    incoming_version: str
    changes: List[SchemaChange] = Field(default_factory=list)

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.change_type == ChangeType.BREAKING for c in self.changes)

    @property
    def breaking_changes(self) -> List[SchemaChange]:
        return [c for c in self.changes if c.change_type == ChangeType.BREAKING]

    @property
    def non_breaking_changes(self) -> List[SchemaChange]:
        return [c for c in self.changes if c.change_type == ChangeType.NON_BREAKING]

    def summary(self) -> str:
        lines = [
            f"=== Contract Diff: Baseline {self.baseline_version} -> Incoming {self.incoming_version} ===",
            f"Overall Status: {'BREAKING DRIFT DETECTED' if self.has_breaking_changes else 'COMPATIBLE / NON-BREAKING'}",
            f"Breaking Changes Count: {len(self.breaking_changes)}",
            f"Non-Breaking Changes Count: {len(self.non_breaking_changes)}",
        ]
        if self.breaking_changes:
            lines.append("\n[BREAKING CHANGES]:")
            for c in self.breaking_changes:
                lines.append(f"  - [{c.category}] Field '{c.field_name}': {c.description} (Baseline: {c.baseline_value} -> Incoming: {c.incoming_value})")
        if self.non_breaking_changes:
            lines.append("\n[NON-BREAKING CHANGES]:")
            for c in self.non_breaking_changes:
                lines.append(f"  + [{c.category}] Field '{c.field_name}': {c.description} (Baseline: {c.baseline_value} -> Incoming: {c.incoming_value})")
        return "\n".join(lines)


# Type compatibility rules for analytical storage
TYPE_COMPATIBILITY = {
    # target_type: set of allowed source_types that can be safely widened
    "float": {"float", "double", "integer", "int"},
    "double": {"float", "double", "integer", "int"},
    "string": {"string", "varchar", "text"},
    "integer": {"integer", "int"},
    "timestamp": {"timestamp", "datetime"},
}


def compare_contracts(baseline: DataContract, incoming: DataContract) -> ContractDiff:
    """
    Compares baseline contract against incoming contract.
    Classifies every schema discrepancy into BREAKING or NON-BREAKING.
    """
    diff = ContractDiff(
        baseline_version=baseline.version,
        incoming_version=incoming.version,
        changes=[]
    )

    baseline_fields = {f.name: f for f in baseline.schema.fields}
    incoming_fields = {f.name: f for f in incoming.schema.fields}

    # 1. Check for dropped fields
    for name, b_field in baseline_fields.items():
        if name not in incoming_fields:
            if b_field.required:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="DROPPED_REQUIRED_FIELD",
                    field_name=name,
                    description=f"Required field '{name}' was dropped from incoming contract.",
                    baseline_value=b_field.model_dump(),
                    incoming_value=None
                ))
            else:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="DROPPED_OPTIONAL_FIELD",
                    field_name=name,
                    description=f"Optional field '{name}' was removed from incoming contract.",
                    baseline_value=b_field.model_dump(),
                    incoming_value=None
                ))

    # 2. Check for added fields
    for name, inc_field in incoming_fields.items():
        if name not in baseline_fields:
            if inc_field.required and inc_field.default is None:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="NEW_REQUIRED_FIELD_WITHOUT_DEFAULT",
                    field_name=name,
                    description=f"New required field '{name}' added without default value.",
                    baseline_value=None,
                    incoming_value=inc_field.model_dump()
                ))
            else:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="NEW_OPTIONAL_FIELD",
                    field_name=name,
                    description=f"New optional field '{name}' added with default: {inc_field.default}.",
                    baseline_value=None,
                    incoming_value=inc_field.model_dump()
                ))

    # 3. Check modifications to shared fields
    for name in set(baseline_fields.keys()).intersection(set(incoming_fields.keys())):
        b_f = baseline_fields[name]
        i_f = incoming_fields[name]

        # A. Type conversion check
        b_type = b_f.type.lower()
        i_type = i_f.type.lower()
        if b_type != i_type:
            # Check if widening is compatible
            allowed_widening = TYPE_COMPATIBILITY.get(i_type, set())
            if b_type in allowed_widening:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="COMPATIBLE_TYPE_WIDENING",
                    field_name=name,
                    description=f"Field '{name}' type safely widened from {b_type} to {i_type}.",
                    baseline_value=b_type,
                    incoming_value=i_type
                ))
            else:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="INCOMPATIBLE_TYPE_CONVERSION",
                    field_name=name,
                    description=f"Incompatible type conversion for '{name}': {b_type} -> {i_type}.",
                    baseline_value=b_type,
                    incoming_value=i_type
                ))

        # B. Nullability check
        if b_f.nullable != i_f.nullable:
            if not b_f.nullable and i_f.nullable:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="RELAXED_NULLABILITY",
                    field_name=name,
                    description=f"Field '{name}' nullability relaxed (false -> true).",
                    baseline_value=b_f.nullable,
                    incoming_value=i_f.nullable
                ))
            elif b_f.nullable and not i_f.nullable:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="TIGHTENED_NULLABILITY",
                    field_name=name,
                    description=f"Field '{name}' nullability tightened (true -> false), breaking existing nulls.",
                    baseline_value=b_f.nullable,
                    incoming_value=i_f.nullable
                ))

        # C. Allowed values (e.g., currency codes, statuses)
        if b_f.allowed_values is not None:
            if i_f.allowed_values is not None:
                b_set = set(b_f.allowed_values)
                i_set = set(i_f.allowed_values)
                removed_values = b_set - i_set
                added_values = i_set - b_set

                if removed_values:
                    diff.changes.append(SchemaChange(
                        change_type=ChangeType.BREAKING,
                        category="REMOVED_ALLOWED_VALUES",
                        field_name=name,
                        description=f"Field '{name}' removed previously allowed values: {sorted(list(removed_values))}.",
                        baseline_value=b_f.allowed_values,
                        incoming_value=i_f.allowed_values
                    ))
                if added_values:
                    diff.changes.append(SchemaChange(
                        change_type=ChangeType.NON_BREAKING,
                        category="EXPANDED_ALLOWED_VALUES",
                        field_name=name,
                        description=f"Field '{name}' expanded allowed values with: {sorted(list(added_values))}.",
                        baseline_value=b_f.allowed_values,
                        incoming_value=i_f.allowed_values
                    ))
            else:
                # incoming has no restriction where baseline had restriction -> relaxing constraints
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="RELAXED_ALLOWED_VALUES",
                    field_name=name,
                    description=f"Field '{name}' removed allowed_values restriction entirely.",
                    baseline_value=b_f.allowed_values,
                    incoming_value=None
                ))

        # D. Min value constraint
        if b_f.min_value is not None and i_f.min_value is not None:
            if i_f.min_value > b_f.min_value:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="TIGHTENED_MIN_VALUE",
                    field_name=name,
                    description=f"Field '{name}' tightened min_value from {b_f.min_value} to {i_f.min_value}.",
                    baseline_value=b_f.min_value,
                    incoming_value=i_f.min_value
                ))
            elif i_f.min_value < b_f.min_value:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="RELAXED_MIN_VALUE",
                    field_name=name,
                    description=f"Field '{name}' relaxed min_value from {b_f.min_value} to {i_f.min_value}.",
                    baseline_value=b_f.min_value,
                    incoming_value=i_f.min_value
                ))

    return diff


def inspect_batch_schema(df: pd.DataFrame, contract: DataContract) -> ContractDiff:
    """
    Inspects incoming DataFrame columns and physical dtypes against the target contract.
    Detects dropped required columns, type mismatches, and extra producer columns.
    """
    diff = ContractDiff(
        baseline_version=contract.version,
        incoming_version=f"batch_file_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        changes=[]
    )

    batch_columns = set(df.columns)
    contract_fields = {f.name: f for f in contract.schema.fields}

    # 1. Missing required columns
    for f in contract.schema.fields:
        if f.name not in batch_columns:
            if f.required:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    category="MISSING_REQUIRED_COLUMN",
                    field_name=f.name,
                    description=f"Incoming batch is missing required column '{f.name}'.",
                    baseline_value=f.type,
                    incoming_value=None
                ))
            else:
                diff.changes.append(SchemaChange(
                    change_type=ChangeType.NON_BREAKING,
                    category="MISSING_OPTIONAL_COLUMN",
                    field_name=f.name,
                    description=f"Incoming batch omitted optional column '{f.name}'. Default will be applied.",
                    baseline_value=f.default,
                    incoming_value=None
                ))

    # 2. Check for unexpected / uncontracted columns
    for col in batch_columns:
        if col not in contract_fields:
            diff.changes.append(SchemaChange(
                change_type=ChangeType.NON_BREAKING,
                category="UNCONTRACTED_EXTRA_COLUMN",
                field_name=col,
                description=f"Incoming batch contains uncontracted extra column '{col}'. Will be preserved in raw payload.",
                baseline_value=None,
                incoming_value=str(df[col].dtype)
            ))

    # 3. Check for severe column type incompatibility
    for name, f in contract_fields.items():
        if name in batch_columns and len(df) > 0:
            series = df[name]
            expected_type = f.type.lower()
            actual_dtype = str(series.dtype).lower()

            if expected_type in ("float", "double"):
                # If series is object and has non-convertible strings across all rows
                if actual_dtype in ("object", "string"):
                    # Check if all non-null values are non-numeric strings
                    sample_non_null = series.dropna()
                    if len(sample_non_null) > 0:
                        numeric_success = pd.to_numeric(sample_non_null, errors="coerce").notnull().all()
                        if not numeric_success:
                            # If entire column is text that cannot be parsed as floats, classify as batch type mismatch
                            diff.changes.append(SchemaChange(
                                change_type=ChangeType.BREAKING,
                                category="INCOMPATIBLE_BATCH_DTYPE",
                                field_name=name,
                                description=f"Column '{name}' has dtype '{actual_dtype}' but contract requires '{expected_type}'. Contains unparseable text.",
                                baseline_value=expected_type,
                                incoming_value=actual_dtype
                            ))

    return diff


# ==============================================================================
# 4. Pydantic v2 Row Validation Model
# ==============================================================================

class SettlementRecordModel(BaseModel):
    """
    Production Pydantic v2 validation model for fact_settlement_ledger records.
    Validates field constraints, formats, allowed lists, and business logic.
    """
    transaction_id: str
    account_id: str
    settlement_amount: float
    currency: str
    settlement_type: str
    status: str
    event_timestamp: datetime.datetime
    fx_rate: Optional[float] = 1.0
    counterparty_lei: Optional[str] = None

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id(cls, v: Any) -> str:
        if not v or not str(v).strip():
            raise ValueError("transaction_id cannot be null, empty, or whitespace")
        return str(v).strip()

    @field_validator("account_id")
    @classmethod
    def validate_account_id(cls, v: Any) -> str:
        if not v or not str(v).strip():
            raise ValueError("account_id cannot be null, empty, or whitespace")
        return str(v).strip()

    @field_validator("settlement_amount")
    @classmethod
    def validate_settlement_amount(cls, v: Any) -> float:
        try:
            val = float(v)
        except (ValueError, TypeError):
            raise ValueError(f"settlement_amount '{v}' cannot be parsed as a numeric float")

        if val <= 0.0:
            raise ValueError(f"settlement_amount must be strictly positive and non-zero (> 0.00). Got: {val}")

        return round(val, 2)

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: Any) -> str:
        allowed = {"USD", "EUR", "GBP", "INR", "JPY", "SGD"}
        code = str(v).strip().upper() if v else ""
        if code not in allowed:
            raise ValueError(f"currency '{code}' is not permitted. Permitted currencies: {sorted(list(allowed))}")
        return code

    @field_validator("settlement_type")
    @classmethod
    def validate_settlement_type(cls, v: Any) -> str:
        allowed = {"INVOICE_SETTLEMENT", "TREASURY_TRANSFER", "MERCHANT_PAYOUT", "REFUND"}
        st = str(v).strip().upper() if v else ""
        if st not in allowed:
            raise ValueError(f"settlement_type '{st}' is not valid. Permitted types: {sorted(list(allowed))}")
        return st

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Any) -> str:
        allowed = {"COMPLETED", "PENDING", "FLAGGED"}
        st = str(v).strip().upper() if v else ""
        if st not in allowed:
            raise ValueError(f"status '{st}' is not valid. Permitted statuses: {sorted(list(allowed))}")
        return st

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def validate_event_timestamp(cls, v: Any) -> datetime.datetime:
        if isinstance(v, datetime.datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=datetime.timezone.utc)
            return v.astimezone(datetime.timezone.utc)

        if not v or not str(v).strip():
            raise ValueError("event_timestamp cannot be empty or null")

        val_str = str(v).strip()
        # Parse ISO-8601
        try:
            # Handle ISO formats including trailing Z
            dt = datetime.datetime.fromisoformat(val_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        except Exception as e:
            raise ValueError(f"event_timestamp '{val_str}' is not a valid ISO-8601 timestamp: {e}")
