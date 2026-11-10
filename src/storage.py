"""
storage.py - DuckDB Analytical Storage & Staging Upsert Engine.

Responsibilities:
1. Initialize and maintain DuckDB connection and physical storage files.
2. Initialize core schemas:
   - dim_corporate_accounts (parent dimension)
   - fact_settlement_ledger (authoritative clean analytical storage)
   - quarantine_settlements (dead-letter queue / DLQ)
   - pipeline_runs (operational execution audit log)
3. Pre-populate dim_corporate_accounts with seed corporate profiles.
4. Execute idempotent staging-table merge / ON CONFLICT upsert on transaction_id.
5. Ingest quarantined records idempotently via deterministic hashing.
6. Provide analytical queries for reconciliation and health checks.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import duckdb
import pandas as pd


# ==============================================================================
# Seed Parent Corporate Accounts
# ==============================================================================

SEED_CORPORATE_ACCOUNTS = [
    {
        "account_id": "CORP_1001",
        "company_name": "Acme Global FinTech Inc.",
        "country_code": "US",
        "base_currency": "USD",
        "account_status": "ACTIVE",
        "credit_limit": 5000000.00,
    },
    {
        "account_id": "CORP_1002",
        "company_name": "Apex Logistics Europe B.V.",
        "country_code": "DE",
        "base_currency": "EUR",
        "account_status": "ACTIVE",
        "credit_limit": 3500000.00,
    },
    {
        "account_id": "CORP_1003",
        "company_name": "Bharat Cloud Payments Ltd.",
        "country_code": "IN",
        "base_currency": "INR",
        "account_status": "ACTIVE",
        "credit_limit": 250000000.00,
    },
    {
        "account_id": "CORP_1004",
        "company_name": "Highland Energy Trading Ltd.",
        "country_code": "GB",
        "base_currency": "GBP",
        "account_status": "ACTIVE",
        "credit_limit": 4000000.00,
    },
    {
        "account_id": "CORP_1005",
        "company_name": "Merlion Maritime Holdings Pte.",
        "country_code": "SG",
        "base_currency": "SGD",
        "account_status": "ACTIVE",
        "credit_limit": 6000000.00,
    },
    {
        "account_id": "CORP_1006",
        "company_name": "Tokyo Precision Robotics K.K.",
        "country_code": "JP",
        "base_currency": "JPY",
        "account_status": "ACTIVE",
        "credit_limit": 800000000.00,
    },
    {
        "account_id": "CORP_1007",
        "company_name": "Nordic BioTech Group AB",
        "country_code": "SE",
        "base_currency": "EUR",
        "account_status": "SUSPENDED",
        "credit_limit": 1000000.00,
    },
    {
        "account_id": "CORP_1008",
        "company_name": "Dormant Enterprise Corp",
        "country_code": "US",
        "base_currency": "USD",
        "account_status": "CLOSED",
        "credit_limit": 0.00,
    },
]


class StorageEngine:
    """
    Manages DuckDB warehouse storage, schema definitions, seed dimensions,
    idempotent upserts, dead-letter queue records, and audit logs.
    """

    def __init__(self, db_path: Union[str, Path] = "storage/settlements_warehouse.duckdb"):
        self.db_path_str = str(db_path)
        if self.db_path_str != ":memory:":
            path = Path(self.db_path_str)
            path.parent.mkdir(parents=True, exist_ok=True)

        self.con = duckdb.connect(database=self.db_path_str)
        self.init_schema()
        self.seed_dimensions()

    def close(self):
        """Closes the underlying DuckDB connection cleanly."""
        if self.con:
            self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def init_schema(self):
        """Initializes tables for parent dimension, fact ledger, DLQ, and audit logging."""
        # 1. Parent Dimension: dim_corporate_accounts
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dim_corporate_accounts (
                account_id VARCHAR PRIMARY KEY,
                company_name VARCHAR NOT NULL,
                country_code VARCHAR NOT NULL,
                base_currency VARCHAR NOT NULL,
                account_status VARCHAR NOT NULL,
                credit_limit DOUBLE NOT NULL
            );
        """)

        # 2. Authoritative Fact Ledger: fact_settlement_ledger
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS fact_settlement_ledger (
                transaction_id VARCHAR PRIMARY KEY,
                account_id VARCHAR NOT NULL,
                settlement_amount DOUBLE NOT NULL,
                currency VARCHAR NOT NULL,
                settlement_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                event_timestamp TIMESTAMP NOT NULL,
                ingested_at TIMESTAMP NOT NULL,
                source_file VARCHAR NOT NULL,
                fx_rate DOUBLE DEFAULT 1.0,
                counterparty_lei VARCHAR
            );
        """)

        # 3. Dead-Letter Queue (DLQ): quarantine_settlements
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_settlements (
                quarantine_id VARCHAR PRIMARY KEY,
                raw_payload_json VARCHAR NOT NULL,
                error_code VARCHAR NOT NULL,
                error_message VARCHAR NOT NULL,
                source_file VARCHAR NOT NULL,
                quarantined_at TIMESTAMP NOT NULL
            );
        """)

        # 4. Pipeline Execution Audit: pipeline_runs
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                batch_file VARCHAR NOT NULL,
                contract_version VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                input_rows BIGINT NOT NULL,
                clean_rows BIGINT NOT NULL,
                quarantined_rows BIGINT NOT NULL,
                execution_time_seconds DOUBLE NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP NOT NULL
            );
        """)

    def seed_dimensions(self):
        """Pre-populates parent corporate dimension accounts idempotently."""
        seed_df = pd.DataFrame(SEED_CORPORATE_ACCOUNTS)
        self.con.register("temp_seed_accounts", seed_df)
        self.con.execute("""
            INSERT OR REPLACE INTO dim_corporate_accounts
            SELECT account_id, company_name, country_code, base_currency, account_status, credit_limit
            FROM temp_seed_accounts;
        """)
        self.con.unregister("temp_seed_accounts")

    def get_valid_account_ids(self, include_suspended: bool = True) -> Set[str]:
        """
        Retrieves valid account IDs from dim_corporate_accounts.
        Excludes CLOSED accounts by default.
        """
        if include_suspended:
            rows = self.con.execute(
                "SELECT account_id FROM dim_corporate_accounts WHERE account_status != 'CLOSED'"
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT account_id FROM dim_corporate_accounts WHERE account_status = 'ACTIVE'"
            ).fetchall()
        return {r[0] for r in rows}

    def load_clean_settlements(self, clean_records: List[Dict[str, Any]]) -> int:
        """
        Idempotently inserts or updates clean settlement records into fact_settlement_ledger.
        Uses temporary staging table and ON CONFLICT (transaction_id) DO UPDATE.
        Returns the number of rows merged.
        """
        if not clean_records:
            return 0

        df = pd.DataFrame(clean_records)
        # Ensure optional fields exist if missing
        if "fx_rate" not in df.columns:
            df["fx_rate"] = 1.0
        if "counterparty_lei" not in df.columns:
            df["counterparty_lei"] = None

        self.con.register("staging_clean_settlements", df)
        self.con.execute("""
            INSERT INTO fact_settlement_ledger (
                transaction_id, account_id, settlement_amount, currency, settlement_type,
                status, event_timestamp, ingested_at, source_file, fx_rate, counterparty_lei
            )
            SELECT 
                transaction_id, account_id, settlement_amount, currency, settlement_type,
                status, event_timestamp, ingested_at, source_file, 
                COALESCE(fx_rate, 1.0), counterparty_lei
            FROM staging_clean_settlements
            ON CONFLICT (transaction_id) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                settlement_amount = EXCLUDED.settlement_amount,
                currency = EXCLUDED.currency,
                settlement_type = EXCLUDED.settlement_type,
                status = EXCLUDED.status,
                event_timestamp = EXCLUDED.event_timestamp,
                ingested_at = EXCLUDED.ingested_at,
                source_file = EXCLUDED.source_file,
                fx_rate = EXCLUDED.fx_rate,
                counterparty_lei = EXCLUDED.counterparty_lei;
        """)
        self.con.unregister("staging_clean_settlements")
        return len(clean_records)

    def load_quarantine_records(self, quarantine_records: List[Dict[str, Any]]) -> int:
        """
        Idempotently inserts quarantined records into quarantine_settlements (DLQ).
        Derives deterministic quarantine_id from (source_file, raw_payload_json, error_code).
        Returns number of records quarantined.
        """
        if not quarantine_records:
            return 0

        formatted_records = []
        for r in quarantine_records:
            raw_payload = r.get("raw_payload_json")
            if isinstance(raw_payload, dict):
                raw_str = json.dumps(raw_payload, sort_keys=True, default=str)
            else:
                raw_str = str(raw_payload)

            error_code = str(r.get("error_code", "ERR_UNKNOWN"))
            source_file = str(r.get("source_file", "unknown_source"))
            quarantined_at = r.get("quarantined_at")

            # Deterministic hash for idempotency across reruns
            hash_input = f"{source_file}:{raw_str}:{error_code}".encode("utf-8")
            quarantine_id = f"Q_{hashlib.sha256(hash_input).hexdigest()[:16]}"

            formatted_records.append({
                "quarantine_id": quarantine_id,
                "raw_payload_json": raw_str,
                "error_code": error_code,
                "error_message": str(r.get("error_message", "")),
                "source_file": source_file,
                "quarantined_at": quarantined_at,
            })

        q_df = pd.DataFrame(formatted_records)
        self.con.register("staging_quarantine", q_df)
        self.con.execute("""
            INSERT OR REPLACE INTO quarantine_settlements
            SELECT quarantine_id, raw_payload_json, error_code, error_message, source_file, quarantined_at
            FROM staging_quarantine;
        """)
        self.con.unregister("staging_quarantine")
        return len(formatted_records)

    def log_pipeline_run(
        self,
        run_id: str,
        batch_file: str,
        contract_version: str,
        status: str,
        input_rows: int,
        clean_rows: int,
        quarantined_rows: int,
        execution_time_seconds: float,
        started_at: Any,
        completed_at: Any,
    ):
        """Logs execution summary to pipeline_runs audit table."""
        self.con.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs (
                run_id, batch_file, contract_version, status,
                input_rows, clean_rows, quarantined_rows,
                execution_time_seconds, started_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                batch_file,
                contract_version,
                status,
                input_rows,
                clean_rows,
                quarantined_rows,
                execution_time_seconds,
                started_at,
                completed_at,
            ],
        )

    def get_ledger_metrics(self) -> Dict[str, Any]:
        """Calculates total rows and aggregate financial amounts in fact_settlement_ledger."""
        res = self.con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COALESCE(SUM(settlement_amount), 0.0) as total_amount,
                COUNT(DISTINCT account_id) as unique_accounts,
                COUNT(DISTINCT currency) as unique_currencies
            FROM fact_settlement_ledger;
        """).fetchone()

        # Currency breakdown
        curr_res = self.con.execute("""
            SELECT currency, COUNT(*) as txn_count, ROUND(SUM(settlement_amount), 2) as total_amount
            FROM fact_settlement_ledger
            GROUP BY currency
            ORDER BY total_amount DESC;
        """).fetchall()

        return {
            "total_rows": res[0] if res else 0,
            "total_amount": round(res[1], 2) if res else 0.0,
            "unique_accounts": res[2] if res else 0,
            "unique_currencies": res[3] if res else 0,
            "currency_breakdown": [
                {"currency": r[0], "txn_count": r[1], "total_amount": float(r[2])}
                for r in curr_res
            ],
        }

    def get_quarantine_metrics(self) -> Dict[str, Any]:
        """Calculates total rows and breakdown of error codes in quarantine_settlements."""
        res = self.con.execute("""
            SELECT COUNT(*) as total_quarantined
            FROM quarantine_settlements;
        """).fetchone()

        err_res = self.con.execute("""
            SELECT error_code, COUNT(*) as err_count
            FROM quarantine_settlements
            GROUP BY error_code
            ORDER BY err_count DESC;
        """).fetchall()

        return {
            "total_quarantined": res[0] if res else 0,
            "error_breakdown": {r[0]: r[1] for r in err_res},
        }

