"""
generator.py - Deterministic Synthetic B2B Settlement Data Generator.

Generates realistic enterprise settlement batches with seeded pseudo-randomness (seed=42).
Supports:
1. Starter batch (25 rows) with explicit intentional edge cases and anomalies.
2. Scalable benchmark batch (100,000+ rows) for high-throughput performance benchmarking.
"""

from __future__ import annotations

import datetime
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd


VALID_ACCOUNTS = [
    "CORP_1001",  # Acme Global FinTech Inc. (US, USD)
    "CORP_1002",  # Apex Logistics Europe B.V. (DE, EUR)
    "CORP_1003",  # Bharat Cloud Payments Ltd. (IN, INR)
    "CORP_1004",  # Highland Energy Trading Ltd. (GB, GBP)
    "CORP_1005",  # Merlion Maritime Holdings Pte. (SG, SGD)
    "CORP_1006",  # Tokyo Precision Robotics K.K. (JP, JPY)
]

VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "SGD"]
VALID_SETTLEMENT_TYPES = ["INVOICE_SETTLEMENT", "TREASURY_TRANSFER", "MERCHANT_PAYOUT", "REFUND"]
VALID_STATUSES = ["COMPLETED", "PENDING", "FLAGGED"]


def generate_starter_batch(
    output_path: Optional[Union[str, Path]] = "data/starter_batch.csv",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generates a deterministic 25-row starter dataset with explicit intentional edge cases.

    Intentional Edge Cases Included:
    - Row 6:  Missing transaction_id (null / empty PK)
    - Row 7:  Non-existent account_id ('CORP_9999' -> FK violation against dim_corporate_accounts)
    - Row 8:  Negative settlement_amount (-1250.50)
    - Row 9:  Zero settlement_amount (0.00)
    - Row 10: Unsupported currency code ('XYZ')
    - Row 11: Unsupported currency code ('BITCOIN')
    - Row 12: Malformed timestamp numeric ('2026-13-45T99:99:99')
    - Row 13: Malformed timestamp text ('invalid-iso-date-string')
    - Row 14: Duplicate Primary Key first instance ('TXN_DUP_001')
    - Row 15: Duplicate Primary Key second instance ('TXN_DUP_001' -> in-batch duplicate)
    - Row 16: Missing account_id (None / empty FK)
    - Row 17: Invalid settlement_type ('UNAUTHORIZED_PAYMENT')
    - Row 18: Invalid status ('UNKNOWN_STATUS')
    - Uncontracted column 'uncontracted_debug_note' present across rows
    - 12 clean, valid B2B settlement records across diverse currencies and corporate accounts
    """
    random.seed(seed)
    np.random.seed(seed)

    base_time = datetime.datetime(2026, 11, 9, 10, 0, 0, tzinfo=datetime.timezone.utc)

    records: List[Dict[str, Any]] = [
        # --- Clean records (Rows 0 - 5) ---
        {
            "transaction_id": "TXN_90001",
            "account_id": "CORP_1001",
            "settlement_amount": 14500.50,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=10)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90002",
            "account_id": "CORP_1002",
            "settlement_amount": 8920.00,
            "currency": "EUR",
            "settlement_type": "TREASURY_TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=15)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90003",
            "account_id": "CORP_1004",
            "settlement_amount": 6340.75,
            "currency": "GBP",
            "settlement_type": "MERCHANT_PAYOUT",
            "status": "PENDING",
            "event_timestamp": (base_time + datetime.timedelta(minutes=20)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90004",
            "account_id": "CORP_1003",
            "settlement_amount": 540000.00,
            "currency": "INR",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=25)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90005",
            "account_id": "CORP_1005",
            "settlement_amount": 12850.30,
            "currency": "SGD",
            "settlement_type": "REFUND",
            "status": "FLAGGED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=30)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90006",
            "account_id": "CORP_1006",
            "settlement_amount": 2500000.00,
            "currency": "JPY",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=35)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },

        # --- Intentional Anomaly 1: Missing PK (Row 6) ---
        {
            "transaction_id": None,
            "account_id": "CORP_1001",
            "settlement_amount": 500.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=40)).isoformat(),
            "uncontracted_debug_note": "anomaly_null_pk",
        },

        # --- Intentional Anomaly 2: Invalid Foreign Key (Row 7) ---
        {
            "transaction_id": "TXN_90008",
            "account_id": "CORP_9999",  # Does NOT exist in dim_corporate_accounts
            "settlement_amount": 12000.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=45)).isoformat(),
            "uncontracted_debug_note": "anomaly_invalid_fk",
        },

        # --- Intentional Anomaly 3: Negative Settlement Amount (Row 8) ---
        {
            "transaction_id": "TXN_90009",
            "account_id": "CORP_1002",
            "settlement_amount": -1250.50,
            "currency": "EUR",
            "settlement_type": "REFUND",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=50)).isoformat(),
            "uncontracted_debug_note": "anomaly_negative_amount",
        },

        # --- Intentional Anomaly 4: Zero Settlement Amount (Row 9) ---
        {
            "transaction_id": "TXN_90010",
            "account_id": "CORP_1004",
            "settlement_amount": 0.00,
            "currency": "GBP",
            "settlement_type": "MERCHANT_PAYOUT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=55)).isoformat(),
            "uncontracted_debug_note": "anomaly_zero_amount",
        },

        # --- Intentional Anomaly 5: Unsupported Currency XYZ (Row 10) ---
        {
            "transaction_id": "TXN_90011",
            "account_id": "CORP_1001",
            "settlement_amount": 3400.00,
            "currency": "XYZ",
            "settlement_type": "TREASURY_TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=60)).isoformat(),
            "uncontracted_debug_note": "anomaly_bad_currency_xyz",
        },

        # --- Intentional Anomaly 6: Unsupported Currency BITCOIN (Row 11) ---
        {
            "transaction_id": "TXN_90012",
            "account_id": "CORP_1003",
            "settlement_amount": 95.00,
            "currency": "BITCOIN",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "PENDING",
            "event_timestamp": (base_time + datetime.timedelta(minutes=65)).isoformat(),
            "uncontracted_debug_note": "anomaly_bad_currency_crypto",
        },

        # --- Intentional Anomaly 7: Malformed Timestamp (Row 12) ---
        {
            "transaction_id": "TXN_90013",
            "account_id": "CORP_1005",
            "settlement_amount": 7800.00,
            "currency": "SGD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": "2026-13-45T99:99:99",
            "uncontracted_debug_note": "anomaly_malformed_timestamp_1",
        },

        # --- Intentional Anomaly 8: Malformed Timestamp String (Row 13) ---
        {
            "transaction_id": "TXN_90014",
            "account_id": "CORP_1006",
            "settlement_amount": 620000.00,
            "currency": "JPY",
            "settlement_type": "MERCHANT_PAYOUT",
            "status": "COMPLETED",
            "event_timestamp": "invalid-iso-date-string",
            "uncontracted_debug_note": "anomaly_malformed_timestamp_2",
        },

        # --- Intentional Anomaly 9a: Duplicate Primary Key (first instance) (Row 14) ---
        {
            "transaction_id": "TXN_DUP_001",
            "account_id": "CORP_1001",
            "settlement_amount": 5100.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=70)).isoformat(),
            "uncontracted_debug_note": "duplicate_pk_first_instance",
        },

        # --- Intentional Anomaly 9b: Duplicate Primary Key (second instance) (Row 15) ---
        {
            "transaction_id": "TXN_DUP_001",  # Same PK as Row 14
            "account_id": "CORP_1001",
            "settlement_amount": 5100.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=71)).isoformat(),
            "uncontracted_debug_note": "duplicate_pk_second_instance_quarantine",
        },

        # --- Intentional Anomaly 10: Missing Account ID (Row 16) ---
        {
            "transaction_id": "TXN_90017",
            "account_id": None,
            "settlement_amount": 2200.00,
            "currency": "USD",
            "settlement_type": "TREASURY_TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=75)).isoformat(),
            "uncontracted_debug_note": "anomaly_missing_account_id",
        },

        # --- Intentional Anomaly 11: Invalid Settlement Type (Row 17) ---
        {
            "transaction_id": "TXN_90018",
            "account_id": "CORP_1002",
            "settlement_amount": 3100.00,
            "currency": "EUR",
            "settlement_type": "UNAUTHORIZED_PAYMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=80)).isoformat(),
            "uncontracted_debug_note": "anomaly_invalid_settlement_type",
        },

        # --- Intentional Anomaly 12: Invalid Status (Row 18) ---
        {
            "transaction_id": "TXN_90019",
            "account_id": "CORP_1004",
            "settlement_amount": 1950.00,
            "currency": "GBP",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "UNKNOWN_STATUS",
            "event_timestamp": (base_time + datetime.timedelta(minutes=85)).isoformat(),
            "uncontracted_debug_note": "anomaly_invalid_status",
        },

        # --- Clean records continuing to row 24 (Rows 19 - 24) ---
        {
            "transaction_id": "TXN_90020",
            "account_id": "CORP_1001",
            "settlement_amount": 42000.00,
            "currency": "USD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=90)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90021",
            "account_id": "CORP_1002",
            "settlement_amount": 16400.25,
            "currency": "EUR",
            "settlement_type": "TREASURY_TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=95)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90022",
            "account_id": "CORP_1003",
            "settlement_amount": 1250000.00,
            "currency": "INR",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=100)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90023",
            "account_id": "CORP_1004",
            "settlement_amount": 7890.50,
            "currency": "GBP",
            "settlement_type": "MERCHANT_PAYOUT",
            "status": "PENDING",
            "event_timestamp": (base_time + datetime.timedelta(minutes=105)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90024",
            "account_id": "CORP_1005",
            "settlement_amount": 33100.00,
            "currency": "SGD",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=110)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
        {
            "transaction_id": "TXN_90025",
            "account_id": "CORP_1006",
            "settlement_amount": 4800000.00,
            "currency": "JPY",
            "settlement_type": "INVOICE_SETTLEMENT",
            "status": "COMPLETED",
            "event_timestamp": (base_time + datetime.timedelta(minutes=115)).isoformat(),
            "uncontracted_debug_note": "standard_batch_run_42",
        },
    ]

    df = pd.DataFrame(records)

    if output_path:
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_p, index=False)

    return df


def generate_benchmark_batch(
    num_rows: int = 100_000,
    output_path: Optional[Union[str, Path]] = "data/benchmark_batch.parquet",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generates a scalable synthetic dataset (100,000+ rows) for performance benchmarking.
    Maintains ~98% clean settlement records and ~2% controlled intentional anomalies.
    """
    random.seed(seed)
    np.random.seed(seed)

    base_time = datetime.datetime(2026, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    seconds_offsets = np.random.randint(0, 86400 * 7, size=num_rows)
    timestamps = [
        (base_time + datetime.timedelta(seconds=int(offset))).isoformat()
        for offset in seconds_offsets
    ]

    # Generate primary columns
    tx_ids = [f"TXN_{100000 + i:07d}" for i in range(num_rows)]
    accounts = np.random.choice(VALID_ACCOUNTS, size=num_rows)
    amounts = np.round(np.random.exponential(scale=5000.0, size=num_rows) + 50.0, 2)
    currencies = np.random.choice(VALID_CURRENCIES, size=num_rows)
    settlement_types = np.random.choice(VALID_SETTLEMENT_TYPES, size=num_rows)
    statuses = np.random.choice(VALID_STATUSES, size=num_rows, p=[0.85, 0.12, 0.03])

    df = pd.DataFrame({
        "transaction_id": tx_ids,
        "account_id": accounts,
        "settlement_amount": amounts,
        "currency": currencies,
        "settlement_type": settlement_types,
        "status": statuses,
        "event_timestamp": timestamps,
    })

    # Inject intentional anomalies (~2% of records)
    anomaly_count = max(10, int(num_rows * 0.02))
    anomaly_indices = np.random.choice(num_rows, size=anomaly_count, replace=False)

    for i, idx in enumerate(anomaly_indices):
        kind = i % 7
        if kind == 0:
            # Null PK
            df.at[idx, "transaction_id"] = None
        elif kind == 1:
            # Invalid FK
            df.at[idx, "account_id"] = f"CORP_{9900 + (i % 99)}"
        elif kind == 2:
            # Negative amount
            df.at[idx, "settlement_amount"] = -round(float(np.random.uniform(10.0, 500.0)), 2)
        elif kind == 3:
            # Zero amount
            df.at[idx, "settlement_amount"] = 0.00
        elif kind == 4:
            # Unsupported currency
            df.at[idx, "currency"] = random.choice(["BITCOIN", "XYZ", "DOGE", "ETH"])
        elif kind == 5:
            # Malformed timestamp
            df.at[idx, "event_timestamp"] = "2026-99-99T99:99:99Z"
        elif kind == 6:
            # In-batch duplicate PK
            if idx > 0:
                df.at[idx, "transaction_id"] = df.at[idx - 1, "transaction_id"]

    if output_path:
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        if str(output_path).endswith(".parquet"):
            df.to_parquet(out_p, index=False)
        else:
            df.to_csv(out_p, index=False)

    return df

