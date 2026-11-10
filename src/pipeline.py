"""
pipeline.py - Production Ingestion, Contract Enforcement & Idempotent Load Pipeline.

Coordinates the end-to-end data lifecycle:
1. Contract evolution verification (breaking vs. non-breaking diff).
2. Batch schema inspection against the target contract.
3. Strict row-level validation & business rules:
   - Primary key nullability & in-batch uniqueness.
   - Foreign key referential integrity against dim_corporate_accounts.
   - Positive non-zero monetary amounts (> 0.00).
   - Permitted ISO-4217 currencies.
   - Allowed settlement types & statuses.
   - ISO-8601 UTC event timestamps.
4. Deterministic routing:
   - Valid records -> fact_settlement_ledger (idempotent upsert via DuckDB staging).
   - Anomalies -> quarantine_settlements (Dead-Letter Queue with raw payload & error code).
5. Mathematical reconciliation & operational audit logging.
"""

from __future__ import annotations

import datetime
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

import numpy as np
import pandas as pd

from src.contract_engine import (
    ChangeType,
    ContractDiff,
    DataContract,
    SchemaDriftError,
    SettlementRecordModel,
    compare_contracts,
    inspect_batch_schema,
)
from src.reporter import ReconciliationReport, build_reconciliation_report
from src.storage import StorageEngine


def run_ingestion_pipeline(
    batch_path: Union[str, Path],
    contract_path: Union[str, Path] = "contracts/settlements_contract_v1.yaml",
    baseline_contract_path: Optional[Union[str, Path]] = None,
    db_path: Union[str, Path] = "storage/settlements_warehouse.duckdb",
    storage_engine: Optional[StorageEngine] = None,
    output_summary_path: Optional[Union[str, Path]] = "outputs/run_summary.json",
    halt_on_breaking_drift: bool = True,
) -> ReconciliationReport:
    """
    Executes the end-to-end ingestion pipeline for a given transaction batch file.
    """
    t_start = time.perf_counter()
    run_id = f"RUN_{uuid.uuid4().hex[:12].upper()}"
    started_at = datetime.datetime.now(datetime.timezone.utc)
    batch_file_str = str(batch_path)
    source_filename = Path(batch_path).name

    # ==========================================================================
    # Step 1: Load Target Data Contract
    # ==========================================================================
    contract = DataContract.from_yaml(contract_path)
    schema_drift_summary = None

    # Compare against baseline contract if explicitly specified
    if baseline_contract_path and Path(baseline_contract_path).resolve() != Path(contract_path).resolve():
        baseline_contract = DataContract.from_yaml(baseline_contract_path)
        contract_diff = compare_contracts(baseline_contract, contract)
        schema_drift_summary = contract_diff.model_dump()

        if contract_diff.has_breaking_changes and halt_on_breaking_drift:
            raise SchemaDriftError(
                f"Halting pipeline: Breaking schema drift detected between baseline v{baseline_contract.version} "
                f"and incoming contract v{contract.version}.\n{contract_diff.summary()}",
                diff=contract_diff,
            )

    # ==========================================================================
    # Step 2: Read Batch File (CSV or Parquet)
    # ==========================================================================
    batch_p = Path(batch_path)
    if not batch_p.exists():
        raise FileNotFoundError(f"Input batch file not found: {batch_p}")

    # Handle zero-byte or empty file
    if batch_p.stat().st_size == 0:
        return _handle_empty_batch(
            run_id=run_id,
            batch_file=source_filename,
            contract_version=contract.version,
            started_at=started_at,
            t_start=t_start,
            storage_engine=storage_engine,
            db_path=db_path,
            output_summary_path=output_summary_path,
            schema_drift_summary=schema_drift_summary,
        )

    if batch_p.suffix.lower() == ".parquet":
        df = pd.read_parquet(batch_p)
    else:
        df = pd.read_csv(batch_p, dtype=object, keep_default_na=False)

    total_input_rows = len(df)
    if total_input_rows == 0:
        return _handle_empty_batch(
            run_id=run_id,
            batch_file=source_filename,
            contract_version=contract.version,
            started_at=started_at,
            t_start=t_start,
            storage_engine=storage_engine,
            db_path=db_path,
            output_summary_path=output_summary_path,
            schema_drift_summary=schema_drift_summary,
        )

    # ==========================================================================
    # Step 3: Inspect Batch Schema Against Target Contract
    # ==========================================================================
    batch_diff = inspect_batch_schema(df, contract)
    if batch_diff.has_breaking_changes:
        if schema_drift_summary is None:
            schema_drift_summary = batch_diff.model_dump()
        else:
            schema_drift_summary["batch_drift"] = batch_diff.model_dump()

        if halt_on_breaking_drift:
            raise SchemaDriftError(
                f"Halting pipeline: Batch schema violates contract v{contract.version}.\n{batch_diff.summary()}",
                diff=batch_diff,
            )

    # ==========================================================================
    # Step 4: Storage Connection & Referential Dimension Check
    # ==========================================================================
    should_close_storage = False
    if storage_engine is None:
        storage = StorageEngine(db_path=db_path)
        should_close_storage = True
    else:
        storage = storage_engine

    try:
        valid_accounts = storage.get_valid_account_ids(include_suspended=True)

        # Allowed sets from contract
        allowed_currencies = set(contract.schema.get_field("currency").allowed_values or [])
        allowed_types = set(contract.schema.get_field("settlement_type").allowed_values or [])
        allowed_statuses = set(contract.schema.get_field("status").allowed_values or [])

        # ======================================================================
        # Step 5: Deterministic Row Validation & Anomaly Quarantine
        # ======================================================================
        clean_records: List[Dict[str, Any]] = []
        quarantine_records: List[Dict[str, Any]] = []
        error_counts: Dict[str, int] = {}
        seen_batch_txns: Set[str] = set()

        total_input_amount_parsed = 0.0
        quarantined_amount_parsed = 0.0
        unparseable_or_non_positive_count = 0

        ingested_at_ts = datetime.datetime.now(datetime.timezone.utc)

        # Convert records to dictionary for resilient row-level evaluation
        records = df.to_dict(orient="records")

        for row in records:
            # Clean empty strings into None for clean handling
            row_dict = {
                k: (None if (v is None or (isinstance(v, str) and v.strip() in ("", "None", "nan", "NaN", "null"))) else v)
                for k, v in row.items()
            }

            raw_tx_id = row_dict.get("transaction_id")
            raw_acc_id = row_dict.get("account_id")
            raw_amount = row_dict.get("settlement_amount")
            raw_curr = row_dict.get("currency")
            raw_type = row_dict.get("settlement_type")
            raw_stat = row_dict.get("status")
            raw_ts = row_dict.get("event_timestamp")

            # Assess raw settlement amount parseability for financial reconciliation
            is_valid_numeric_amount = False
            parsed_row_amount = 0.0
            if raw_amount is not None:
                try:
                    parsed_row_amount = round(float(raw_amount), 2)
                    if parsed_row_amount > 0.0:
                        is_valid_numeric_amount = True
                        total_input_amount_parsed += parsed_row_amount
                except (ValueError, TypeError):
                    is_valid_numeric_amount = False

            # --- Rule 1: Missing Required Primary Key ---
            if raw_tx_id is None or not str(raw_tx_id).strip():
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_MISSING_REQUIRED_FIELD",
                    error_message="Primary key 'transaction_id' is null, empty, or missing.",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                if not is_valid_numeric_amount:
                    unparseable_or_non_positive_count += 1
                else:
                    quarantined_amount_parsed += parsed_row_amount
                continue

            tx_id_str = str(raw_tx_id).strip()

            # --- Rule 2: In-Batch Duplicate Primary Key ---
            if tx_id_str in seen_batch_txns:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_DUPLICATE_IN_BATCH",
                    error_message=f"Duplicate primary key '{tx_id_str}' encountered within the same input batch.",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                if not is_valid_numeric_amount:
                    unparseable_or_non_positive_count += 1
                else:
                    quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 3: Missing Required Foreign Key ---
            if raw_acc_id is None or not str(raw_acc_id).strip():
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_MISSING_REQUIRED_FIELD",
                    error_message="Foreign key 'account_id' is null, empty, or missing.",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                if not is_valid_numeric_amount:
                    unparseable_or_non_positive_count += 1
                else:
                    quarantined_amount_parsed += parsed_row_amount
                continue

            acc_id_str = str(raw_acc_id).strip()

            # --- Rule 4: Foreign Key Referential Integrity Check ---
            if acc_id_str not in valid_accounts:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_FK_ACCOUNT_NOT_FOUND",
                    error_message=f"Foreign key violation: account_id '{acc_id_str}' does not exist in dim_corporate_accounts.",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                if not is_valid_numeric_amount:
                    unparseable_or_non_positive_count += 1
                else:
                    quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 5: Settlement Amount Validation (Strictly > 0.00) ---
            if not is_valid_numeric_amount:
                # Determine specific error code
                try:
                    num_val = float(raw_amount)
                    if num_val <= 0.0:
                        err_c = "ERR_NON_POSITIVE_AMOUNT"
                        err_m = f"Monetary settlement_amount must be strictly positive (> 0.00). Received: {num_val}"
                    else:
                        err_c = "ERR_INVALID_AMOUNT"
                        err_m = f"Invalid settlement_amount '{raw_amount}'"
                except Exception:
                    err_c = "ERR_INVALID_AMOUNT"
                    err_m = f"settlement_amount '{raw_amount}' is not a valid numeric float."

                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code=err_c,
                    error_message=err_m,
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                unparseable_or_non_positive_count += 1
                continue

            # --- Rule 6: Currency Validation ---
            curr_str = str(raw_curr).strip().upper() if raw_curr else ""
            if curr_str not in allowed_currencies:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_UNSUPPORTED_CURRENCY",
                    error_message=f"Currency '{curr_str}' is unsupported. Allowed currency set: {sorted(list(allowed_currencies))}",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 7: Settlement Type Validation ---
            type_str = str(raw_type).strip().upper() if raw_type else ""
            if type_str not in allowed_types:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_INVALID_SETTLEMENT_TYPE",
                    error_message=f"Settlement type '{type_str}' is invalid. Allowed types: {sorted(list(allowed_types))}",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 8: Status Validation ---
            stat_str = str(raw_stat).strip().upper() if raw_stat else ""
            if stat_str not in allowed_statuses:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_INVALID_STATUS",
                    error_message=f"Settlement status '{stat_str}' is invalid. Allowed statuses: {sorted(list(allowed_statuses))}",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 9: ISO-8601 Timestamp Validation ---
            try:
                if isinstance(raw_ts, datetime.datetime):
                    dt = raw_ts
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                elif isinstance(raw_ts, (pd.Timestamp, np.datetime64)):
                    dt = pd.to_datetime(raw_ts).to_pydatetime()
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                elif isinstance(raw_ts, str) and raw_ts.strip():
                    dt = datetime.datetime.fromisoformat(raw_ts.strip().replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                else:
                    raise ValueError("Null or empty timestamp")
            except Exception as e:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_MALFORMED_TIMESTAMP",
                    error_message=f"event_timestamp '{raw_ts}' fails ISO-8601 parsing: {e}",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                quarantined_amount_parsed += parsed_row_amount
                continue

            # --- Rule 10: Pydantic v2 Record Model Validation ---
            try:
                record_payload = {
                    "transaction_id": tx_id_str,
                    "account_id": acc_id_str,
                    "settlement_amount": parsed_row_amount,
                    "currency": curr_str,
                    "settlement_type": type_str,
                    "status": stat_str,
                    "event_timestamp": dt,
                    "fx_rate": float(row_dict.get("fx_rate", 1.0) or 1.0),
                    "counterparty_lei": row_dict.get("counterparty_lei"),
                }
                validated_model = SettlementRecordModel.model_validate(record_payload)
            except Exception as e:
                _quarantine_row(
                    quarantine_records, error_counts, row_dict,
                    error_code="ERR_SCHEMA_VALIDATION_FAILURE",
                    error_message=f"Pydantic validation failure: {e}",
                    source_file=source_filename,
                    quarantined_at=ingested_at_ts,
                )
                quarantined_amount_parsed += parsed_row_amount
                continue

            # Record is clean!
            seen_batch_txns.add(tx_id_str)
            clean_record_dict = validated_model.model_dump()
            clean_record_dict["ingested_at"] = ingested_at_ts
            clean_record_dict["source_file"] = source_filename
            clean_records.append(clean_record_dict)

        # ======================================================================
        # Step 6: Idempotent Warehouse Persistence
        # ======================================================================
        storage.load_clean_settlements(clean_records)
        storage.load_quarantine_records(quarantine_records)

        # Calculate metrics
        clean_amount_total = sum(r["settlement_amount"] for r in clean_records)
        completed_at = datetime.datetime.now(datetime.timezone.utc)
        execution_time = time.perf_counter() - t_start

        # Query warehouse metrics
        warehouse_metrics = storage.get_ledger_metrics()

        # Operational status for run audit
        if len(quarantine_records) > 0:
            run_status = "PARTIAL_SUCCESS_WITH_QUARANTINE"
        else:
            run_status = "SUCCESS"

        storage.log_pipeline_run(
            run_id=run_id,
            batch_file=source_filename,
            contract_version=contract.version,
            status=run_status,
            input_rows=total_input_rows,
            clean_rows=len(clean_records),
            quarantined_rows=len(quarantine_records),
            execution_time_seconds=execution_time,
            started_at=started_at,
            completed_at=completed_at,
        )

        # ======================================================================
        # Step 7: Build & Save Reconciliation Report
        # ======================================================================
        report = build_reconciliation_report(
            run_id=run_id,
            batch_file=source_filename,
            contract_version=contract.version,
            total_input_rows=total_input_rows,
            clean_rows=len(clean_records),
            quarantined_rows=len(quarantine_records),
            total_input_amount=total_input_amount_parsed,
            clean_amount=clean_amount_total,
            quarantined_parsed_amount=quarantined_amount_parsed,
            unparseable_count=unparseable_or_non_positive_count,
            execution_time=execution_time,
            error_summary=error_counts,
            currency_breakdown=warehouse_metrics.get("currency_breakdown", []),
            schema_drift_summary=schema_drift_summary,
        )

        if output_summary_path:
            report.save_json(output_summary_path)

        return report

    finally:
        if should_close_storage:
            storage.close()


def _quarantine_row(
    quarantine_list: List[Dict[str, Any]],
    error_counts: Dict[str, int],
    row_dict: Dict[str, Any],
    error_code: str,
    error_message: str,
    source_file: str,
    quarantined_at: datetime.datetime,
):
    """Appends an anomaly payload to the quarantine batch and updates error tally."""
    quarantine_list.append({
        "raw_payload_json": row_dict,
        "error_code": error_code,
        "error_message": error_message,
        "source_file": source_file,
        "quarantined_at": quarantined_at,
    })
    error_counts[error_code] = error_counts.get(error_code, 0) + 1


def _handle_empty_batch(
    run_id: str,
    batch_file: str,
    contract_version: str,
    started_at: datetime.datetime,
    t_start: float,
    storage_engine: Optional[StorageEngine],
    db_path: Union[str, Path],
    output_summary_path: Optional[Union[str, Path]],
    schema_drift_summary: Optional[Dict[str, Any]],
) -> ReconciliationReport:
    """Safely handles empty input batch files without crashing."""
    execution_time = time.perf_counter() - t_start
    should_close = False
    if storage_engine is None:
        storage = StorageEngine(db_path=db_path)
        should_close = True
    else:
        storage = storage_engine

    try:
        storage.log_pipeline_run(
            run_id=run_id,
            batch_file=batch_file,
            contract_version=contract_version,
            status="SUCCESS_EMPTY_BATCH",
            input_rows=0,
            clean_rows=0,
            quarantined_rows=0,
            execution_time_seconds=execution_time,
            started_at=started_at,
            completed_at=datetime.datetime.now(datetime.timezone.utc),
        )
    finally:
        if should_close:
            storage.close()

    report = build_reconciliation_report(
        run_id=run_id,
        batch_file=batch_file,
        contract_version=contract_version,
        total_input_rows=0,
        clean_rows=0,
        quarantined_rows=0,
        total_input_amount=0.0,
        clean_amount=0.0,
        quarantined_parsed_amount=0.0,
        unparseable_count=0,
        execution_time=execution_time,
        error_summary={},
        currency_breakdown=[],
        schema_drift_summary=schema_drift_summary,
    )
    if output_summary_path:
        report.save_json(output_summary_path)

    return report

