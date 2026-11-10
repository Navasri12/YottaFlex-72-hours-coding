"""
test_contracts.py - Test suite for YAML contract parsing and schema drift classification.

Tests:
1. Baseline contract loading and Pydantic v2 model parsing.
2. Non-breaking contract evolution (optional fields with defaults).
3. Breaking contract evolution:
   - Dropped required fields.
   - Incompatible type conversions (float to string).
   - Removed currency codes.
4. Batch-level schema drift inspection:
   - Missing required column detection.
   - Batch data type mismatch detection.
   - Uncontracted extra producer column classification.
5. Immediate pipeline abort on breaking schema drift.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.contract_engine import (
    ChangeType,
    DataContract,
    SchemaDriftError,
    compare_contracts,
    inspect_batch_schema,
)
from src.pipeline import run_ingestion_pipeline


@pytest.fixture
def baseline_contract() -> DataContract:
    return DataContract.from_yaml("contracts/settlements_contract_v1.yaml")


@pytest.fixture
def valid_v2_contract() -> DataContract:
    return DataContract.from_yaml("contracts/settlements_contract_v2_valid.yaml")


@pytest.fixture
def breaking_v2_contract() -> DataContract:
    return DataContract.from_yaml("contracts/settlements_contract_v2_breaking.yaml")


def test_baseline_contract_loading(baseline_contract: DataContract):
    """Verifies baseline YAML contract loads and parses cleanly into DataContract model."""
    assert baseline_contract.contract_name == "fact_settlement_ledger"
    assert baseline_contract.version == "1.0.0"
    assert baseline_contract.target_table == "fact_settlement_ledger"

    field_names = baseline_contract.schema.field_names
    assert "transaction_id" in field_names
    assert "account_id" in field_names
    assert "settlement_amount" in field_names
    assert "currency" in field_names
    assert "settlement_type" in field_names
    assert "status" in field_names
    assert "event_timestamp" in field_names

    # Check allowed currencies
    curr_field = baseline_contract.schema.get_field("currency")
    assert curr_field is not None
    assert set(curr_field.allowed_values) == {"USD", "EUR", "GBP", "INR", "JPY", "SGD"}


def test_non_breaking_contract_evolution(baseline_contract: DataContract, valid_v2_contract: DataContract):
    """Asserts evolutionary changes with optional defaults are classified as non-breaking."""
    diff = compare_contracts(baseline_contract, valid_v2_contract)

    assert not diff.has_breaking_changes, "Non-breaking contract must not trigger breaking drift flag"
    assert len(diff.breaking_changes) == 0

    non_breaking_categories = [c.category for c in diff.non_breaking_changes]
    assert "NEW_OPTIONAL_FIELD" in non_breaking_categories

    added_fields = [c.field_name for c in diff.non_breaking_changes]
    assert "fx_rate" in added_fields
    assert "counterparty_lei" in added_fields


def test_breaking_contract_evolution(baseline_contract: DataContract, breaking_v2_contract: DataContract):
    """Asserts breaking changes (dropped PK/FK, type conversion, removed currencies) are caught."""
    diff = compare_contracts(baseline_contract, breaking_v2_contract)

    assert diff.has_breaking_changes, "Breaking contract must trigger breaking drift flag"
    assert len(diff.breaking_changes) >= 3

    categories = {c.category for c in diff.breaking_changes}

    # 1. Dropped required foreign key account_id
    assert "DROPPED_REQUIRED_FIELD" in categories
    dropped_change = next(c for c in diff.breaking_changes if c.category == "DROPPED_REQUIRED_FIELD")
    assert dropped_change.field_name == "account_id"

    # 2. Incompatible type conversion for settlement_amount (float -> string)
    assert "INCOMPATIBLE_TYPE_CONVERSION" in categories
    type_change = next(c for c in diff.breaking_changes if c.category == "INCOMPATIBLE_TYPE_CONVERSION")
    assert type_change.field_name == "settlement_amount"
    assert type_change.baseline_value == "float"
    assert type_change.incoming_value == "string"

    # 3. Removed allowed currencies (GBP, INR, JPY, SGD removed)
    assert "REMOVED_ALLOWED_VALUES" in categories
    curr_change = next(c for c in diff.breaking_changes if c.category == "REMOVED_ALLOWED_VALUES" and c.field_name == "currency")
    assert "GBP" in str(curr_change.description)


def test_batch_schema_drift_missing_required_column(baseline_contract: DataContract):
    """Verifies that an incoming batch missing a required column is flagged as breaking."""
    bad_df = pd.DataFrame({
        "transaction_id": ["TXN_001"],
        # account_id is omitted!
        "settlement_amount": [150.0],
        "currency": ["USD"],
        "settlement_type": ["INVOICE_SETTLEMENT"],
        "status": ["COMPLETED"],
        "event_timestamp": ["2026-11-09T10:00:00Z"],
    })

    diff = inspect_batch_schema(bad_df, baseline_contract)
    assert diff.has_breaking_changes
    missing_change = next(c for c in diff.breaking_changes if c.category == "MISSING_REQUIRED_COLUMN")
    assert missing_change.field_name == "account_id"


def test_batch_schema_drift_extra_uncontracted_column(baseline_contract: DataContract):
    """Verifies uncontracted extra columns are classified as NON_BREAKING."""
    df_with_extra = pd.DataFrame({
        "transaction_id": ["TXN_001"],
        "account_id": ["CORP_1001"],
        "settlement_amount": [150.0],
        "currency": ["USD"],
        "settlement_type": ["INVOICE_SETTLEMENT"],
        "status": ["COMPLETED"],
        "event_timestamp": ["2026-11-09T10:00:00Z"],
        "uncontracted_producer_flag": ["TEST_FLAG"],
    })

    diff = inspect_batch_schema(df_with_extra, baseline_contract)
    assert not diff.has_breaking_changes
    extra_change = next(c for c in diff.non_breaking_changes if c.category == "UNCONTRACTED_EXTRA_COLUMN")
    assert extra_change.field_name == "uncontracted_producer_flag"


def test_pipeline_aborts_on_breaking_schema_drift(tmp_path):
    """Asserts pipeline execution aborts cleanly with SchemaDriftError when breaking contract is provided."""
    test_batch = tmp_path / "test_batch.csv"
    test_batch.write_text(
        "transaction_id,settlement_amount,currency,settlement_type,status,event_timestamp\n"
        "TXN_999,$100.00,USD,INVOICE_SETTLEMENT,COMPLETED,2026-11-09T10:00:00Z\n"
    )

    with pytest.raises(SchemaDriftError) as exc_info:
        run_ingestion_pipeline(
            batch_path=test_batch,
            contract_path="contracts/settlements_contract_v2_breaking.yaml",
            baseline_contract_path="contracts/settlements_contract_v1.yaml",
            db_path=":memory:",
            halt_on_breaking_drift=True,
        )

    assert "Breaking schema drift detected" in str(exc_info.value)

