Act as a Principal Data Engineer and AI Forward Deployed Engineer. Build a complete, production-grade implementation of a 72-hour Data Engineering take-home challenge for the AI Data Forward Deployed Engineer (AI FDE, 0–3 years experience) role at YottaFlex AI Technologies.

BUSINESS CONTEXT & DOMAIN:
Build a reliable Data Contract & Ingestion Engine for an Enterprise B2B Multi-Currency Transaction & Settlement Ledger (`fact_settlement_ledger` referencing a parent corporate dimension `dim_corporate_accounts`). 
Clean settlement records published to DuckDB analytical storage serve as the authoritative upstream feed for downstream B2B Financial Risk, Liquidity Forecasting, and Fraud AI models. Any corrupted, drifted, or unvalidated currency data must never reach downstream analytical consumers.

The solution must:
1. Load human-readable YAML contracts, convert them into Pydantic v2 validation models, and compare incoming batch schemas against baseline contracts to explicitly detect and classify schema drift into:
   - BREAKING changes (dropped required fields, incompatible type conversions, removed currency codes, tightened nullability).
   - NON-BREAKING changes (new optional fields with defaults, relaxed constraints).
2. Validate incoming producer batches against schema types, nullability, permitted currency codes (e.g., USD, EUR, GBP, INR, JPY, SGD), positive non-zero settlement amounts, ISO-8601 timestamps, and foreign key integrity against `dim_corporate_accounts`.
3. Route records deterministically: valid rows publish idempotently to DuckDB; invalid/anomalous rows route to a Dead-Letter Queue (DLQ / `quarantine_settlements`) preserving the raw payload, explicit error code, error message, and ingestion metadata.
4. Guarantee idempotency: rerunning the same input batch multiple times must never duplicate rows or distort settlement totals.
5. Provide a deterministic synthetic data generator (seed=42) supporting both a starter batch (25 rows with explicit edge cases) and a scalable batch (100,000+ rows) for performance benchmarking.
6. Provide an automated reconciliation engine outputting execution metrics to an audit table (`pipeline_runs`) and an `outputs/run_summary.json` file.
7. Include an automated `pytest` suite covering all required negative scenarios, schema breakages, and edge cases.

TECH STACK & CONSTRAINTS:
- Language: Python 3.10+
- Storage & Ingestion: DuckDB (embedded OLAP engine) and Pandas/PyArrow
- Contracts & Validation: PyYAML and Pydantic v2
- Testing: pytest
- CLI: Python argparse
- OS Compatibility: Windows 11 / Cross-Platform friendly (standard relative paths, no OS-specific binaries)

PROJECT REPOSITORY STRUCTURE:
Create the complete, runnable codebase file by file with zero omissions or placeholder comments:
├── contracts/
│   ├── settlements_contract_v1.yaml         # Production baseline contract
│   ├── settlements_contract_v2_valid.yaml    # Non-breaking update (optional fx_rate or counterparty_lei column)
│   └── settlements_contract_v2_breaking.yaml # Breaking update (dropped account_id, settlement_amount as string)
├── src/
│   ├── __init__.py
│   ├── contract_engine.py  # YAML parser, Pydantic models, schema diffing logic (breaking vs. non-breaking classifier)
│   ├── generator.py        # Seeded (seed=42) synthetic data generator (small test batch & 100k+ row benchmark) with deliberate anomalies
│   ├── pipeline.py         # Ingestion, schema validation, foreign key check, business rules, DLQ routing, idempotent load
│   ├── storage.py          # DuckDB initialization, parent seed accounts, staging-merge upsert logic, DLQ & audit logging
│   └── reporter.py         # Generates reconciliation report and outputs run_summary.json
├── tests/
│   ├── __init__.py
│   ├── test_contracts.py   # Schema drift detection (breaking vs. non-breaking classification)
│   ├── test_pipeline.py    # Happy path, null PK, negative/zero amount, unsupported currency, invalid FK, malformed timestamps, empty file
│   └── test_idempotency.py # Ingesting identical batch twice asserts row count and sum(settlement_amount) remain unchanged
├── data/                   # Directory where generated batches are stored
├── outputs/                # Directory where run_summary.json and logs are saved
├── run_pipeline.py         # Unified CLI entrypoint with flags (--generate, --batch, --contract, --rows, --report)
├── requirements.txt
└── README.md               # Architecture flow, data dictionary, setup instructions, edge case matrix, and performance optimization note

DETAILED SPECIFICATIONS:

1. ENTITIES & RELATIONSHIPS:
- Parent Dimension: `dim_corporate_accounts`
  * `account_id` (PK, VARCHAR, e.g., 'CORP_1001')
  * `company_name` (VARCHAR)
  * `country_code` (VARCHAR, 2-letter ISO: 'US', 'IN', 'GB', 'DE', 'SG')
  * `base_currency` (VARCHAR, e.g., 'USD', 'INR', 'EUR')
  * `account_status` (VARCHAR: 'ACTIVE', 'SUSPENDED', 'CLOSED')
  * `credit_limit` (DOUBLE)
- Ingested Batch: `fact_settlement_ledger`
  * `transaction_id` (PK, VARCHAR, e.g., 'TXN_90001')
  * `account_id` (FK referencing `dim_corporate_accounts.account_id`)
  * `settlement_amount` (DOUBLE: strictly > 0.00, rounded to 2 decimal places)
  * `currency` (VARCHAR: allowed set ['USD', 'EUR', 'GBP', 'INR', 'JPY', 'SGD'])
  * `settlement_type` (VARCHAR: 'INVOICE_SETTLEMENT', 'TREASURY_TRANSFER', 'MERCHANT_PAYOUT', 'REFUND')
  * `status` (VARCHAR: 'COMPLETED', 'PENDING', 'FLAGGED')
  * `event_timestamp` (ISO-8601 UTC timestamp: YYYY-MM-DDTHH:MM:SSZ)
  * `ingested_at` (Auto-generated ISO timestamp)
  * `source_file` (VARCHAR)

2. CORE FUNCTIONAL MODULES:
- `contract_engine.py`:
  * Inspects incoming batch columns and types against the YAML contract.
  * Outputs a structured diff:
    - Breaking: Missing required columns, type mismatches (e.g., float to string), disallowed currency values.
    - Non-Breaking: Added optional fields with defaults, relaxed nullability.
  * Aborts pipeline execution if breaking changes are detected.
- `storage.py`:
  * Manages DuckDB connection (`storage/settlements_warehouse.duckdb`).
  * Pre-populates `dim_corporate_accounts` with realistic default corporate records.
  * Implements an idempotent staging-table merge / `INSERT OR REPLACE` on `transaction_id`.
  * Creates `quarantine_settlements` table: `quarantine_id`, `raw_payload_json`, `error_code`, `error_message`, `source_file`, `quarantined_at`.
  * Creates `pipeline_runs` table: `run_id`, `batch_file`, `contract_version`, `status`, `input_rows`, `clean_rows`, `quarantined_rows`, `execution_time_seconds`, `started_at`, `completed_at`.
- `generator.py`:
  * Uses `random.seed(42)` and `numpy.random.seed(42)`.
  * Injects intentional anomalies: missing `transaction_id`, non-existent `account_id` (FK violation), negative or zero `settlement_amount`, unsupported currencies (e.g., 'XYZ', 'BITCOIN'), unparseable ISO timestamps, in-batch duplicate primary keys, and extra uncontracted columns.
- `reporter.py`:
  * Calculates and prints the reconciliation check:
    1. Input Rows Count == Clean Rows Loaded + Quarantined Rows
    2. Batch Settlement Amount == Clean Amount + Quarantined Amount (where amount was parseable)
  * Saves formatted metadata to `outputs/run_summary.json`.
- `tests/`:
  * Complete `pytest` suite validating: (1) breaking schema alert, (2) non-breaking schema acceptance, (3) empty batch handling, (4) FK referential integrity failure, (5) negative amount quarantine, (6) duplicate PK in-batch deduplication, (7) rerun idempotency.

Please output the complete, production-ready code file by file with clear docstrings, realistic corporate data values, and clean terminal commands.