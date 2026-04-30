# CloudETLResim

CloudETLResim is a source-code reconstruction of a production-style AWS serverless ETL workflow. 
It models an API -> S3 -> RDS pipeline using Lambda, versioned S3 raw partitions, schema validation, retry/cooldown handling, simulated monitoring hooks, lineage via run_id, explicit MySQL upsert, conflict auditing, and bad-record capture. 
AWS deployment assets such as Step Functions, Lambda Layers, IAM, Secrets Manager, CloudWatch alarms, and database migrations are managed outside this repository.

A source-only reconstruction of a small AWS serverless ETL workflow.

This project simulates a previous cloud ETL flow with a smaller public dataset:

```text
Alpha Vantage API
  -> Lambda: AWS_API2S3.py
  -> S3 ODS/raw versioned object
  -> Lambda: AWS_S3toRDS.py
  -> RDS MySQL ADS table
```

## Scope

This repository intentionally contains only Lambda source code and project notes.

The following production resources are assumed to be managed in AWS deployment
or database migration tooling, not committed in this repository:

- Lambda runtime configuration
- Lambda Layer / dependency package
- IAM roles and policies
- S3 bucket
- RDS instance / security group / subnet / VPC wiring
- Step Functions state machine
- EventBridge trigger
- CloudWatch alarms and dashboards
- Secrets Manager entries
- CI/CD and infrastructure-as-code definitions
- Database schema/migration for `stock_prices`, `etl_load_audit`, and `etl_bad_records`

This is a reconstruction/demo of an older project, not a full production platform repository.

## Files

| File | Purpose |
|---|---|
| `AWS_API2S3.py` | Source -> ODS. Fetches Alpha Vantage daily stock data, validates it, and writes a versioned object to S3. |
| `AWS_S3toRDS.py` | ODS -> ADS. Reads the versioned S3 object, validates schema/fields, and upserts into RDS MySQL. |
| `README.md` | Explains architecture, assumptions, lineage, replay, orchestration, and production gaps intentionally left to AWS deployment. |

## Data Flow

### 1. API to S3

`AWS_API2S3.py`:

1. Reads `symbol`, `run_id`, `business_date`, and `max_daily_points` from the event when provided.
2. Fetches daily stock data from Alpha Vantage.
3. Runs schema, field, decimal, date, and empty-data validation.
4. Stores the payload in S3 with a versioned path.

Example S3 key:

```text
stock-data/raw/symbol=MSFT/dt=2026-04-30/run_id=8d5.../daily.json
```

The versioned path is deliberate. It avoids overwriting `latest_data.json` and allows replay/backfill from S3.

### 2. S3 to RDS

`AWS_S3toRDS.py`:

1. Receives `s3_bucket`, `s3_key`, `symbol`, and `run_id` from Step Functions or another orchestrator.
2. Reads the S3 object.
3. Validates payload structure and fields again before writing.
4. Upserts rows into `stock_prices`.
5. Writes lightweight audit records into `etl_load_audit`.
6. Writes validation failure records into `etl_bad_records`.

## Target Table Assumption

The RDS schema is assumed to exist because database creation/migration is managed
outside this source-only repository.

The upsert path assumes:

- `stock_prices` exists.
- `stock_prices` has a unique key on `(symbol, date)`.
- `stock_prices` has writable columns for `last_run_id` and `updated_at`.
- `etl_load_audit` exists for successful/failed load summaries.
- `etl_bad_records` exists for validation failure capture.

No runtime DDL is performed by the Lambda functions.

## Upsert Strategy

The original toy implementation used `INSERT IGNORE`.

This version uses explicit MySQL upsert:

```sql
INSERT INTO stock_prices (...)
VALUES (...)
ON DUPLICATE KEY UPDATE ...
```

Behavior:

| Case | Result |
|---|---|
| New `(symbol, date)` | Insert |
| Existing `(symbol, date)` | Update price fields, volume, `last_run_id`, and `updated_at` |
| Validation failure | Do not write to `stock_prices`; record failure in `etl_bad_records` when DB is reachable |
| Successful load | Record summary in `etl_load_audit` |

## Data Integrity

Data integrity here means more than "the Lambda ran successfully."

This version adds checks across three levels:

### 1. Object-level checks

- API response must be a JSON object.
- Alpha Vantage error messages are detected.
- Empty `Time Series (Daily)` is rejected.
- S3 payload must contain either:
  - `{ "metadata": ..., "data": ... }`, or
  - the older direct `{date: values}` layout.

### 2. Record-level checks

For each daily stock record:

- Date must match `YYYY-MM-DD`.
- Required fields must exist:
  - `1. open`
  - `2. high`
  - `3. low`
  - `4. close`
  - `5. volume`
- OHLC fields must be valid decimals.
- Volume must be a non-negative integer.

### 3. Load-level checks

- Empty dataset is rejected.
- Each run has a `run_id`.
- S3 object path includes `symbol`, `dt`, and `run_id`.
- RDS load records `record_count`, `inserted_or_updated_count`, and `conflict_count`.

## Lineage

Each run is traceable by:

| Field | Meaning |
|---|---|
| `run_id` | Logical ETL run identifier |
| `symbol` | Stock symbol processed |
| `s3_bucket` | Source ODS bucket |
| `s3_key` | Source ODS object |
| `record_count` | Number of validated records |
| `conflict_count` | Existing `(symbol, date)` records updated |
| `last_run_id` | Run that last wrote a given `stock_prices` row |

The S3 object also includes metadata:

```json
{
  "metadata": {
    "source": "alpha_vantage",
    "dataset": "TIME_SERIES_DAILY",
    "symbol": "MSFT",
    "run_id": "...",
    "record_count": 2,
    "created_at": "..."
  },
  "data": {}
}
```

## Retry, Cooldown, and Monitoring

Both Lambda files include a lightweight retry wrapper:

```text
attempt 1
  -> failure
  -> cooldown
attempt 2
  -> failure
  -> longer cooldown
attempt 3
  -> final failure
```

Both files also include a simulated internal monitoring hook:

```python
monitor_event("event_name", payload, level="INFO")
```

If `INTERNAL_MONITORING_API_URL` is set, the function attempts to POST to that endpoint.
If not set, it prints a structured monitoring stub line.

This keeps the repository self-contained while showing where internal observability integration would exist.

## Orchestration

The intended orchestration is AWS-managed, usually Step Functions plus EventBridge.

Minimal logical flow:

```text
EventBridge schedule or manual trigger
  -> Step 1: Invoke AWS_API2S3.py
       input:  { symbol, business_date, max_daily_points, run_id? }
       output: { s3_bucket, s3_key, symbol, run_id, record_count }

  -> Step 2: Invoke AWS_S3toRDS.py
       input:  output of Step 1
       output: load status and audit summary
```

Why orchestration matters:

| Concern | Why it matters |
|---|---|
| Retry ownership | Some retries are better handled by Step Functions instead of Lambda code. |
| State handoff | Lambda 1 must pass `s3_bucket`, `s3_key`, `symbol`, and `run_id` to Lambda 2. |
| Replay | A replay script can re-invoke Lambda 2 with an old `s3_key`. |
| Backfill | A backfill driver can invoke Lambda 1 with different dates/symbols or directly replay known S3 keys. |
| Failure routing | Failed state can go to alerting, DLQ, or manual remediation. |
| Auditability | The state machine execution ID can be mapped to `run_id`. |

This repository does not include the Step Functions definition because AWS deployment is assumed to be managed outside the source-only reconstruction.

## Failure Contract

The Lambda functions are intended for Step Functions-style orchestration.

By default, failed execution raises an exception after logging and simulated monitoring.
This allows the state machine `Retry` / `Catch` policy to classify the state as failed.

For local testing or API-Gateway-style response simulation, set:

```text
FAILURE_MODE=response
```

In that mode, the Lambda returns a `{ "statusCode": 500, ... }` payload instead of raising.

## Backfill and Replay

Because S3 now uses versioned partition paths, replay does not require refetching the API.

Replay example event for `AWS_S3toRDS.py`:

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "stock-data/raw/symbol=MSFT/dt=2026-04-30/run_id=abc/daily.json",
  "symbol": "MSFT",
  "run_id": "replay-20260430-001"
}
```

A separate backfill/replay script can enumerate historical S3 keys and invoke the second Lambda.

## Environment Variables

| Variable | Used by | Meaning |
|---|---|---|
| `S3_BUCKET_NAME` | `AWS_API2S3.py` | Destination S3 bucket |
| `ALPHAVANTAGE_API_KEY` | `AWS_API2S3.py` | API key; in real deployment this should be Secrets Manager-backed |
| `DEFAULT_SYMBOL` | Both | Default symbol, `MSFT` if unset |
| `MAX_DAILY_POINTS` | `AWS_API2S3.py` | Demo limit, defaults to `2` |
| `RETRY_ATTEMPTS` | Both | Retry attempts, defaults to `3` |
| `RETRY_COOLDOWN_SECONDS` | Both | Base cooldown seconds, defaults to `2` |
| `INTERNAL_MONITORING_API_URL` | Both | Optional simulated monitoring endpoint |
| `FAILURE_MODE` | Both | Defaults to `raise` for Step Functions; use `response` for local/API-style simulation |
| `RDS_HOST` | `AWS_S3toRDS.py` | RDS host |
| `RDS_USER` | `AWS_S3toRDS.py` | RDS user |
| `RDS_PASSWORD` | `AWS_S3toRDS.py` | RDS password; in real deployment this should be Secrets Manager-backed |
| `RDS_DATABASE` | `AWS_S3toRDS.py` | RDS database |
| `RDS_PORT` | `AWS_S3toRDS.py` | RDS port, defaults to `3306` |

## Deliberately Not Included

The following are deliberately not added in this repository version:

- Terraform / CDK / CloudFormation
- CI/CD pipeline
- Full AWS IAM policies
- Full Step Functions ASL definition
- Real Secrets Manager implementation
- Real monitoring backend
- Full multi-symbol production ingestion framework

Those belong to the AWS deployment layer or to a fuller platform repository, not this source-only reconstruction.
