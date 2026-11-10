#!/usr/bin/env python3
"""
run_pipeline.py - Unified CLI Entrypoint for YottaFlex Data Contract & Ingestion Engine.

Usage Examples:
  1. Generate synthetic starter batch and run ingestion with baseline contract:
     python run_pipeline.py --generate --rows 25 --batch data/starter_batch.csv --report

  2. Ingest an existing batch using non-breaking v2 contract:
     python run_pipeline.py --batch data/starter_batch.csv --contract contracts/settlements_contract_v2_valid.yaml --report

  3. Test breaking contract evolution (detects schema drift and halts safely):
     python run_pipeline.py --batch data/starter_batch.csv --contract contracts/settlements_contract_v2_breaking.yaml --baseline-contract contracts/settlements_contract_v1.yaml

  4. Run high-throughput 100,000+ row benchmark:
     python run_pipeline.py --benchmark --rows 100000 --report
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time

from src.contract_engine import SchemaDriftError
from src.generator import generate_benchmark_batch, generate_starter_batch
from src.pipeline import run_ingestion_pipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("YottaFlexEngine")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="YottaFlex Enterprise B2B Settlement Ledger Data Contract & Ingestion Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest 25-row starter batch with baseline contract:
  python run_pipeline.py --generate --rows 25 --batch data/starter_batch.csv --report

  # Benchmark 100k row high-throughput ingestion:
  python run_pipeline.py --benchmark --rows 100000 --report

  # Validate breaking contract rejection:
  python run_pipeline.py --batch data/starter_batch.csv --contract contracts/settlements_contract_v2_breaking.yaml --baseline-contract contracts/settlements_contract_v1.yaml
        """,
    )

    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate synthetic data batch before running the pipeline.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=25,
        help="Number of rows to generate (default: 25; benchmark: 100000+).",
    )
    parser.add_argument(
        "--batch",
        type=str,
        default="data/starter_batch.csv",
        help="Path to the input batch file (CSV or Parquet) to ingest.",
    )
    parser.add_argument(
        "--contract",
        type=str,
        default="contracts/settlements_contract_v1.yaml",
        help="Path to the target YAML contract schema definition.",
    )
    parser.add_argument(
        "--baseline-contract",
        type=str,
        default=None,
        help="Path to baseline YAML contract for cross-version schema drift comparison.",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="storage/settlements_warehouse.duckdb",
        help="Path to DuckDB warehouse storage file (default: storage/settlements_warehouse.duckdb, or :memory:).",
    )
    parser.add_argument(
        "--output-summary",
        type=str,
        default="outputs/run_summary.json",
        help="Destination path for run summary JSON report (default: outputs/run_summary.json).",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        default=True,
        help="Print reconciliation report to terminal (default: True).",
    )
    parser.add_argument(
        "--no-report",
        action="store_false",
        dest="report",
        help="Suppress terminal report output.",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Generate scalable dataset (Parquet) and benchmark high-throughput ingestion.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    batch_path = Path(args.batch)

    # 1. Handle Benchmark Flag
    if args.benchmark:
        benchmark_rows = max(args.rows, 100_000)
        benchmark_file = Path("data/benchmark_batch.parquet")
        logger.info(f"Running high-throughput benchmark: generating {benchmark_rows:,} records to {benchmark_file}...")
        t0 = time.perf_counter()
        generate_benchmark_batch(num_rows=benchmark_rows, output_path=benchmark_file, seed=42)
        gen_duration = time.perf_counter() - t0
        logger.info(f"Benchmark dataset generated in {gen_duration:.2f} seconds ({benchmark_rows / gen_duration:,.0f} rows/sec).")
        batch_path = benchmark_file

    # 2. Handle Generate Flag
    elif args.generate:
        logger.info(f"Generating synthetic batch ({args.rows} rows) to {batch_path}...")
        if args.rows == 25:
            generate_starter_batch(output_path=batch_path, seed=42)
        else:
            if batch_path.suffix.lower() == ".parquet":
                generate_benchmark_batch(num_rows=args.rows, output_path=batch_path, seed=42)
            else:
                generate_benchmark_batch(num_rows=args.rows, output_path=batch_path, seed=42)
        logger.info("Batch generation complete.")

    # 3. Check Input File Existence
    if not batch_path.exists():
        logger.error(f"Input batch file not found: {batch_path}. Use --generate to create it first.")
        return 1

    # 4. Execute Pipeline
    logger.info(f"Starting ingestion pipeline...")
    logger.info(f" • Input Batch:    {batch_path}")
    logger.info(f" • Target Contract: {args.contract}")
    logger.info(f" • Warehouse DB:   {args.db_path}")

    try:
        report = run_ingestion_pipeline(
            batch_path=batch_path,
            contract_path=args.contract,
            baseline_contract_path=args.baseline_contract,
            db_path=args.db_path,
            output_summary_path=args.output_summary,
            halt_on_breaking_drift=True,
        )

        if args.report:
            report.print_summary()

        logger.info(f"Pipeline finished successfully. Run ID: {report.run_id}. Summary saved to {args.output_summary}.")
        return 0

    except SchemaDriftError as e:
        logger.error(f"PIPELINE ABORTED: {e}")
        if e.diff:
            print("\n" + "=" * 80)
            print("                CRITICAL SCHEMA DRIFT VIOLATION DETECTED")
            print("=" * 80)
            print(e.diff.summary())
            print("=" * 80 + "\n")
        return 1

    except Exception as e:
        logger.exception(f"Fatal pipeline error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

