"""
test_pipeline.py - Comprehensive functional test suite for the ingestion pipeline.

Tests:
1. Happy path ingestion (all records valid).
2. Null / missing primary key quarantine (ERR_MISSING_REQUIRED_FIELD).
3. Negative and zero settlement amounts quarantine (ERR_NON_POSITIVE_AMOUNT).
4. Unsupported currency quarantine (ERR_UNSUPPORTED_CURRENCY).
5. Foreign key referential integrity failure quarantine (ERR_FK_ACCOUNT_NOT_FOUND).
6. Malformed ISO-8601 timestamps quarantine (ERR_MALFORMED_TIMESTAMP).
7. In-batch duplicate primary key deduplication (ERR_DUPLICATE_IN_BATCH).
8. Empty batch file resilience (zero-row clean handling).
"""

from __future__ import annotations

import datetime
from pathlib import Path
import duckdb
import pandas as pd
import pytest

from src.pipeline import run_ingestion_pipeline
from src.storage import StorageEngine


@pytest.fixture
def memory_db_path():
    return ":memory:"


def test_happy_path_ingestion(tmp_path):
    """Asserts valid records are 100% ingested into fact_settlement_ledger with 0 in DLQ."""
    batch_file = tmp_path / "clean_batch.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_HP_001",
            "account_id": "CORP_1001",
            "settlement_amount": 10500.25,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        },
        {
            "transaction_id": "TXN_HP_002",
            "account_id": "CORP_1002",
            "settlement_amount": 7500.50,
            "currency": "EUR",
            "settlement_type": "TREASURY_TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:05:00Z",
        },
        {
            "transaction_id": "TXN_HP_003",
            "account_id": "CORP_1003",
            "settlement_amount": 250000.00,
            "currency": "INR",
            "settlement_type": "MERCHANT_PAYOUT",
            "status": "PENDING",
            "event_timestamp": "2026-11-09T12:10:00Z",
        },
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.total_input_rows == 3
    assert report.clean_rows_loaded == 3
    assert report.quarantined_rows == 0
    assert report.row_reconciliation_status == "BALANCED"
    assert report.financial_reconciliation_status == "BALANCED"
    assert report.clean_amount_loaded == round(10500.25 + 7500.50 + 250000.00, 2)


def test_quarantine_null_primary_key(tmp_path):
    """Asserts row with missing transaction_id routes to quarantine_settlements."""
    batch_file = tmp_path / "null_pk_batch.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "",  # Empty PK
            "account_id": "CORP_1001",
            "settlement_amount": 500.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        },
        {
            "transaction_id": "TXN_VALID_01",
            "account_id": "CORP_1001",
            "settlement_amount": 1000.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:05:00Z",
        },
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.total_input_rows == 2
    assert report.clean_rows_loaded == 1
    assert report.quarantined_rows == 1
    assert report.error_summary.get("ERR_MISSING_REQUIRED_FIELD") == 1
    assert report.row_reconciliation_status == "BALANCED"


def test_quarantine_negative_and_zero_amount(tmp_path):
    """Asserts negative and zero amounts are quarantined with ERR_NON_POSITIVE_AMOUNT."""
    batch_file = tmp_path / "bad_amounts.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_NEG",
            "account_id": "CORP_1001",
            "settlement_amount": -150.00,  # Negative
            "currency": "USD",
            "settlement_type": "REFUND",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        },
        {
            "transaction_id": "TXN_ZERO",
            "account_id": "CORP_1002",
            "settlement_amount": 0.00,  # Zero
            "currency": "EUR",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:05:00Z",
        },
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.clean_rows_loaded == 0
    assert report.quarantined_rows == 2
    assert report.error_summary.get("ERR_NON_POSITIVE_AMOUNT") == 2
    assert report.unparseable_or_non_positive_amount_count == 2
    assert report.row_reconciliation_status == "BALANCED"


def test_quarantine_unsupported_currency(tmp_path):
    """Asserts uncontracted currency codes are quarantined with ERR_UNSUPPORTED_CURRENCY."""
    batch_file = tmp_path / "bad_currency.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_CURR_1",
            "account_id": "CORP_1001",
            "settlement_amount": 100.00,
            "currency": "BITCOIN",  # Unsupported
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        },
        {
            "transaction_id": "TXN_CURR_2",
            "account_id": "CORP_1001",
            "settlement_amount": 200.00,
            "currency": "XYZ",  # Unsupported
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:05:00Z",
        },
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.clean_rows_loaded == 0
    assert report.quarantined_rows == 2
    assert report.error_summary.get("ERR_UNSUPPORTED_CURRENCY") == 2


def test_quarantine_invalid_foreign_key(tmp_path):
    """Asserts foreign key violation routes to DLQ with ERR_FK_ACCOUNT_NOT_FOUND."""
    batch_file = tmp_path / "bad_fk.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_FK_BAD",
            "account_id": "CORP_9999",  # Non-existent account
            "settlement_amount": 8000.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        }
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.clean_rows_loaded == 0
    assert report.quarantined_rows == 1
    assert report.error_summary.get("ERR_FK_ACCOUNT_NOT_FOUND") == 1


def test_quarantine_malformed_timestamp(tmp_path):
    """Asserts non-ISO timestamps are quarantined with ERR_MALFORMED_TIMESTAMP."""
    batch_file = tmp_path / "bad_ts.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_TS_BAD",
            "account_id": "CORP_1001",
            "settlement_amount": 500.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-99-99T99:99:99",
        }
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.clean_rows_loaded == 0
    assert report.quarantined_rows == 1
    assert report.error_summary.get("ERR_MALFORMED_TIMESTAMP") == 1


def test_in_batch_duplicate_primary_key(tmp_path):
    """Asserts first instance of duplicate PK is ingested, second instance is quarantined."""
    batch_file = tmp_path / "dup_pk.csv"
    df = pd.DataFrame([
        {
            "transaction_id": "TXN_SAME_ID",
            "account_id": "CORP_1001",
            "settlement_amount": 1200.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:00:00Z",
        },
        {
            "transaction_id": "TXN_SAME_ID",  # Duplicate in same batch
            "account_id": "CORP_1001",
            "settlement_amount": 1200.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-11-09T12:01:00Z",
        },
    ])
    df.to_csv(batch_file, index=False)

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.total_input_rows == 2
    assert report.clean_rows_loaded == 1
    assert report.quarantined_rows == 1
    assert report.error_summary.get("ERR_DUPLICATE_IN_BATCH") == 1


def test_empty_file_handling(tmp_path):
    """Asserts pipeline handles empty (0 byte or 0 row) batch files gracefully."""
    empty_file = tmp_path / "empty_batch.csv"
    empty_file.write_text("")

    storage = StorageEngine(":memory:")
    report = run_ingestion_pipeline(
        batch_path=empty_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        storage_engine=storage,
    )

    assert report.total_input_rows == 0
    assert report.clean_rows_loaded == 0
    assert report.quarantined_rows == 0
    assert report.row_reconciliation_status == "BALANCED"
    assert report.financial_reconciliation_status == "BALANCED"

