"""
test_idempotency.py - Strict verification of pipeline idempotency and upsert semantics.

Verifies:
1. Ingesting the identical batch multiple times results in zero row inflation in fact_settlement_ledger.
2. Ingesting the identical batch multiple times maintains exact sum(settlement_amount) without distortion.
3. Ingesting the identical batch multiple times preserves idempotent dead-letter queue records.
4. In-place update upsert semantics on primary key (transaction_id).
"""

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import pytest

from src.generator import generate_starter_batch
from src.pipeline import run_ingestion_pipeline
from src.storage import StorageEngine


def test_starter_batch_rerun_idempotency(tmp_path):
    """
    Reruns the starter batch (25 rows with mixed valid/invalid rows) twice.
    Asserts row count and total monetary amount remain strictly identical.
    """
    db_file = tmp_path / "idempotent_test.duckdb"
    batch_file = tmp_path / "starter_idempotency.csv"
    generate_starter_batch(output_path=batch_file, seed=42)

    # First Ingestion Run
    report_1 = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        db_path=db_file,
    )

    con = duckdb.connect(str(db_file))
    count_run1 = con.execute("SELECT COUNT(*) FROM fact_settlement_ledger").fetchone()[0]
    amount_run1 = round(con.execute("SELECT SUM(settlement_amount) FROM fact_settlement_ledger").fetchone()[0], 2)
    quarantine_run1 = con.execute("SELECT COUNT(*) FROM quarantine_settlements").fetchone()[0]
    con.close()

    assert count_run1 == 13, f"Expected 13 clean rows, got {count_run1}"
    assert amount_run1 == 9237102.30, f"Expected 9,237,102.30, got {amount_run1}"
    assert quarantine_run1 == 12, f"Expected 12 quarantined rows, got {quarantine_run1}"

    # Second Ingestion Run (Identical Batch)
    report_2 = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        db_path=db_file,
    )

    con = duckdb.connect(str(db_file))
    count_run2 = con.execute("SELECT COUNT(*) FROM fact_settlement_ledger").fetchone()[0]
    amount_run2 = round(con.execute("SELECT SUM(settlement_amount) FROM fact_settlement_ledger").fetchone()[0], 2)
    quarantine_run2 = con.execute("SELECT COUNT(*) FROM quarantine_settlements").fetchone()[0]
    con.close()

    # STRICT IDEMPOTENCY ASSERTIONS
    assert count_run2 == count_run1, f"Row count changed after rerun! ({count_run1} -> {count_run2})"
    assert amount_run2 == amount_run1, f"Settlement amount distorted after rerun! ({amount_run1} -> {amount_run2})"
    assert quarantine_run2 == quarantine_run1, f"DLQ inflated after rerun! ({quarantine_run1} -> {quarantine_run2})"

    # Third Ingestion Run (Triple Check)
    report_3 = run_ingestion_pipeline(
        batch_path=batch_file,
        contract_path="contracts/settlements_contract_v1.yaml",
        db_path=db_file,
    )

    con = duckdb.connect(str(db_file))
    count_run3 = con.execute("SELECT COUNT(*) FROM fact_settlement_ledger").fetchone()[0]
    amount_run3 = round(con.execute("SELECT SUM(settlement_amount) FROM fact_settlement_ledger").fetchone()[0], 2)
    con.close()

    assert count_run3 == count_run1
    assert amount_run3 == amount_run1


def test_in_place_upsert_update(tmp_path):
    """
    Tests that re-ingesting an existing transaction_id with updated attributes
    updates the record in-place rather than creating duplicate rows.
    """
    db_file = tmp_path / "upsert_test.duckdb"
    batch_v1 = tmp_path / "batch_v1.csv"
    batch_v2 = tmp_path / "batch_v2.csv"

    # Batch 1: Initial transaction
    df1 = pd.DataFrame([{
        "transaction_id": "TXN_UPSERT_001",
        "account_id": "CORP_1001",
        "settlement_amount": 5000.00,
        "currency": "USD",
        "settlement_type": "INVOICE_SETTLEMENT",
        "status": "PENDING",
        "event_timestamp": "2026-11-09T10:00:00Z",
    }])
    df1.to_csv(batch_v1, index=False)

    run_ingestion_pipeline(
        batch_path=batch_v1,
        contract_path="contracts/settlements_contract_v1.yaml",
        db_path=db_file,
    )

    con = duckdb.connect(str(db_file))
    row1 = con.execute("SELECT settlement_amount, status FROM fact_settlement_ledger WHERE transaction_id = 'TXN_UPSERT_001'").fetchone()
    con.close()
    assert row1[0] == 5000.00
    assert row1[1] == "PENDING"

    # Batch 2: Updated status and revised amount for same transaction_id
    df2 = pd.DataFrame([{
        "transaction_id": "TXN_UPSERT_001",
        "account_id": "CORP_1001",
        "settlement_amount": 5500.00,
        "currency": "USD",
        "settlement_type": "INVOICE_SETTLEMENT",
        "status": "COMPLETED",
        "event_timestamp": "2026-11-09T10:05:00Z",
    }])
    df2.to_csv(batch_v2, index=False)

    run_ingestion_pipeline(
        batch_path=batch_v2,
        contract_path="contracts/settlements_contract_v1.yaml",
        db_path=db_file,
    )

    con = duckdb.connect(str(db_file))
    total_count = con.execute("SELECT COUNT(*) FROM fact_settlement_ledger").fetchone()[0]
    row2 = con.execute("SELECT settlement_amount, status FROM fact_settlement_ledger WHERE transaction_id = 'TXN_UPSERT_001'").fetchone()
    con.close()

    assert total_count == 1, "Upsert should not insert duplicate row for existing primary key"
    assert row2[0] == 5500.00, "Settlement amount should be updated in-place"
    assert row2[1] == "COMPLETED", "Status should be updated in-place to COMPLETED"

