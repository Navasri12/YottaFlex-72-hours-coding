"""
reporter.py - Financial Reconciliation Engine & Run Summary Reporter.

Responsibilities:
1. Calculate and verify strict mathematical reconciliation invariants:
   - Invariant 1 (Row Conservation): Input Rows == Clean Rows Loaded + Quarantined Rows
   - Invariant 2 (Financial Conservation): Batch Parsed Amount == Clean Amount + Quarantined Parsed Amount
2. Formats and prints an executive summary report to the terminal.
3. Serializes execution metadata and audit metrics to outputs/run_summary.json.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class ReconciliationReport(BaseModel):
    run_id: str
    timestamp: str
    batch_file: str
    contract_version: str
    pipeline_status: str
    execution_time_seconds: float

    # Row Invariants
    total_input_rows: int
    clean_rows_loaded: int
    quarantined_rows: int
    row_discrepancy: int
    row_reconciliation_status: str

    # Financial Invariants
    total_input_amount_parsed: float
    clean_amount_loaded: float
    quarantined_amount_parsed: float
    amount_discrepancy: float
    financial_reconciliation_status: str
    unparseable_or_non_positive_amount_count: int

    # Breakdowns
    error_summary: Dict[str, int] = Field(default_factory=dict)
    currency_breakdown: List[Dict[str, Any]] = Field(default_factory=list)
    schema_drift_summary: Optional[Dict[str, Any]] = None

    def save_json(self, output_path: Union[str, Path] = "outputs/run_summary.json") -> Path:
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w", encoding="utf-8") as f:
            f.write(self.model_dump_json(indent=2))
        return out_p

    def print_summary(self):
        """Prints a high-visibility terminal report with explicit reconciliation checks."""
        row_icon = "✓ PASSED" if self.row_reconciliation_status == "BALANCED" else "✗ FAILED"
        fin_icon = "✓ PASSED" if self.financial_reconciliation_status == "BALANCED" else "✗ FAILED"

        print("\n" + "=" * 80)
        print("          YOTTAFLEX ENTERPRISE INGESTION & RECONCILIATION REPORT")
        print("=" * 80)
        print(f" Run ID:               {self.run_id}")
        print(f" Timestamp (UTC):       {self.timestamp}")
        print(f" Batch File:           {self.batch_file}")
        print(f" Target Contract:      v{self.contract_version}")
        print(f" Pipeline Status:      {self.pipeline_status}")
        print(f" Execution Duration:   {self.execution_time_seconds:.4f} seconds")
        print("-" * 80)

        print("\n[1] RECORD CONSERVATION RECONCILIATION:")
        print(f"    • Total Input Rows:            {self.total_input_rows:>10,d}")
        print(f"    • Clean Rows Loaded:           {self.clean_rows_loaded:>10,d}")
        print(f"    • Quarantined Rows (DLQ):      {self.quarantined_rows:>10,d}")
        print(f"    • Discrepancy (Rows Dropped):  {self.row_discrepancy:>10,d}")
        print(f"    -> Row Reconciliation Status:  [{row_icon}] {self.row_reconciliation_status}")

        print("\n[2] FINANCIAL VALUE RECONCILIATION:")
        print(f"    • Parsed Input Volume:        ${self.total_input_amount_parsed:>14,.2f}")
        print(f"    • Clean Volume Loaded:        ${self.clean_amount_loaded:>14,.2f}")
        print(f"    • Quarantined Parsed Volume:  ${self.quarantined_amount_parsed:>14,.2f}")
        print(f"    • Monetary Discrepancy:       ${self.amount_discrepancy:>14,.2f}")
        print(f"    • Invalid/Negative Amounts:    {self.unparseable_or_non_positive_amount_count:>10,d} rows")
        print(f"    -> Financial Balance Status:   [{fin_icon}] {self.financial_reconciliation_status}")

        if self.error_summary:
            print("\n[3] DEAD-LETTER QUEUE (DLQ) ERROR DISTRIBUTION:")
            for err_code, count in self.error_summary.items():
                print(f"    • {err_code:<32}: {count:>5,d} record(s)")

        if self.currency_breakdown:
            print("\n[4] AUTHORITATIVE SETTLEMENT CURRENCY DISTRIBUTION (fact_settlement_ledger):")
            print(f"    {'CURRENCY':<10} {'TXN COUNT':<12} {'SETTLED VOLUME':<20}")
            print("    " + "-" * 42)
            for item in self.currency_breakdown:
                curr = item.get("currency", "UNKNOWN")
                cnt = item.get("txn_count", 0)
                vol = float(item.get("total_amount", 0.0))
                print(f"    {curr:<10} {cnt:<12,d} ${vol:>17,.2f}")

        if self.schema_drift_summary:
            print("\n[5] SCHEMA DRIFT AUDIT:")
            b_cnt = len(self.schema_drift_summary.get("breaking_changes", []))
            nb_cnt = len(self.schema_drift_summary.get("non_breaking_changes", []))
            print(f"    • Breaking Changes:     {b_cnt}")
            print(f"    • Non-Breaking Changes: {nb_cnt}")

        print("=" * 80 + "\n")


def build_reconciliation_report(
    run_id: str,
    batch_file: str,
    contract_version: str,
    total_input_rows: int,
    clean_rows: int,
    quarantined_rows: int,
    total_input_amount: float,
    clean_amount: float,
    quarantined_parsed_amount: float,
    unparseable_count: int,
    execution_time: float,
    error_summary: Dict[str, int],
    currency_breakdown: List[Dict[str, Any]],
    schema_drift_summary: Optional[Dict[str, Any]] = None,
) -> ReconciliationReport:
    """Computes mathematical reconciliation invariants and returns a typed report."""
    row_discrepancy = total_input_rows - (clean_rows + quarantined_rows)
    row_status = "BALANCED" if row_discrepancy == 0 else "DISCREPANCY"

    clean_amount_rounded = round(clean_amount, 2)
    quarantined_amount_rounded = round(quarantined_parsed_amount, 2)
    total_input_rounded = round(total_input_amount, 2)
    amount_discrepancy = round(total_input_rounded - (clean_amount_rounded + quarantined_amount_rounded), 2)

    # Consider balanced if discrepancy is strictly zero
    financial_status = "BALANCED" if abs(amount_discrepancy) < 0.001 else "DISCREPANCY"

    if quarantined_rows > 0:
        pipeline_status = "PARTIAL_SUCCESS_WITH_QUARANTINE"
    else:
        pipeline_status = "SUCCESS"

    return ReconciliationReport(
        run_id=run_id,
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        batch_file=str(batch_file),
        contract_version=str(contract_version),
        pipeline_status=pipeline_status,
        execution_time_seconds=execution_time,
        total_input_rows=total_input_rows,
        clean_rows_loaded=clean_rows,
        quarantined_rows=quarantined_rows,
        row_discrepancy=row_discrepancy,
        row_reconciliation_status=row_status,
        total_input_amount_parsed=total_input_rounded,
        clean_amount_loaded=clean_amount_rounded,
        quarantined_amount_parsed=quarantined_amount_rounded,
        amount_discrepancy=amount_discrepancy,
        financial_reconciliation_status=financial_status,
        unparseable_or_non_positive_amount_count=unparseable_count,
        error_summary=error_summary,
        currency_breakdown=currency_breakdown,
        schema_drift_summary=schema_drift_summary,
    )
