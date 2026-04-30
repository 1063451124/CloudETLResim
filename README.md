# CloudETLResim

CloudETLResim is a compact AWS serverless ETL reconstruction.

It models a common cloud data flow:

```text
External API
  -> Lambda
  -> S3 raw layer
  -> Lambda
  -> RDS MySQL serving table
```

This version uses Alpha Vantage stock data as a lightweight public data source. The original workflow was larger; this repo keeps the same shape while using a smaller, easier-to-run example.

## What It Does

The pipeline has two Lambda stages.

### 1. API to S3

`AWS_API2S3.py` fetches daily stock data from Alpha Vantage, validates the response, and writes the result to S3.

The S3 path is versioned by symbol, date, and run ID:

```text
stock-data/raw/symbol=MSFT/dt=2026-04-30/run_id=<run_id>/daily.json
```

That layout avoids overwriting previous runs and makes replay/backfill straightforward.

### 2. S3 to RDS

`AWS_S3toRDS.py` reads the S3 object, validates records again, and writes the result into MySQL.

The load uses explicit upsert semantics:

```sql
INSERT INTO stock_prices (...)
VALUES (...)
ON DUPLICATE KEY UPDATE ...
```

This means a new `(symbol, date)` row is inserted, while an existing one is updated.

## Repository Layout

| File | Purpose |
|---|---|
| `AWS_API2S3.py` | Fetch source data, validate it, and write a versioned raw object to S3 |
| `AWS_S3toRDS.py` | Read raw data from S3, validate it, upsert into RDS, and write audit records |
| `README.md` | Project overview and execution contract |

## Pipeline Contract

The two Lambda functions are designed to be chained by AWS Step Functions or an equivalent orchestrator.

```text
Step 1: AWS_API2S3.py
  input:
    symbol
    business_date
    max_daily_points
    run_id

  output:
    s3_bucket
    s3_key
    symbol
    run_id
    record_count

Step 2: AWS_S3toRDS.py
  input:
    s3_bucket
    s3_key
    symbol
    run_id

  output:
    record_count
    inserted_or_updated_count
    conflict_count
```

Failures raise exceptions by default so orchestration-level retry and catch logic can handle them. For local testing, set:

```text
FAILURE_MODE=response
```

That returns a `statusCode` payload instead of raising.

## Data Quality

The pipeline validates data before storage and before database loading.

Checks include:

| Check | Purpose |
|---|---|
| API response shape | Rejects invalid or rate-limited Alpha Vantage responses |
| Empty dataset | Prevents empty writes |
| Required fields | Ensures OHLCV fields are present |
| Date format | Requires `YYYY-MM-DD` |
| Decimal fields | Validates open, high, low, close |
| Volume | Requires a non-negative integer |

Bad records are written to `etl_bad_records` when the database is reachable.

## Lineage and Audit

Each run carries a `run_id`.

The S3 object includes metadata such as:

```json
{
  "source": "alpha_vantage",
  "dataset": "TIME_SERIES_DAILY",
  "symbol": "MSFT",
  "run_id": "<run_id>",
  "record_count": 2,
  "created_at": "..."
}
```

The RDS load writes audit information to `etl_load_audit`, including:

| Field | Meaning |
|---|---|
| `run_id` | Logical ETL run ID |
| `symbol` | Stock symbol |
| `source_bucket` | Source S3 bucket |
| `source_key` | Source S3 object |
| `record_count` | Number of validated records |
| `inserted_or_updated_count` | Number of rows written |
| `conflict_count` | Number of rows that already existed before upsert |

The target `stock_prices` table also stores `last_run_id`, so each row can be traced back to the run that last updated it.

## Retry and Monitoring

Both Lambda files include a simple retry wrapper with cooldown.

Monitoring is represented through:

```python
monitor_event("event_name", payload, level="INFO")
```

If `INTERNAL_MONITORING_API_URL` is set, events are posted to that endpoint. Otherwise, they are printed as structured log lines. In a real AWS deployment, this hook maps naturally to CloudWatch metrics, internal observability APIs, or alerting pipelines.

## Replay and Backfill

Because raw data is stored in versioned S3 paths, the RDS load can be replayed without calling the external API again.

Example replay event:

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "stock-data/raw/symbol=MSFT/dt=2026-04-30/run_id=abc/daily.json",
  "symbol": "MSFT",
  "run_id": "replay-20260430-001"
}
```

A backfill driver can enumerate S3 keys and invoke `AWS_S3toRDS.py` for each object.

## AWS Runtime Assumptions

This repo focuses on the Lambda source code. In the AWS environment, these are expected to be managed outside the repo:

| Component | Role |
|---|---|
| Step Functions | Pipeline orchestration, retries, failure routing |
| EventBridge | Scheduled or manual trigger |
| Lambda Layer | Python dependencies |
| Secrets Manager | API keys and database credentials |
| IAM | Access to S3, RDS, logs, and secrets |
| CloudWatch | Logs, metrics, alarms |
| RDS migration | `stock_prices`, `etl_load_audit`, and `etl_bad_records` schema |

The Lambda code assumes the RDS schema already exists.

## Environment Variables

| Variable | Used by | Meaning |
|---|---|---|
| `S3_BUCKET_NAME` | `AWS_API2S3.py` | Destination S3 bucket |
| `ALPHAVANTAGE_API_KEY` | `AWS_API2S3.py` | Alpha Vantage API key |
| `DEFAULT_SYMBOL` | Both | Default stock symbol, `MSFT` if unset |
| `MAX_DAILY_POINTS` | `AWS_API2S3.py` | Demo record limit, defaults to `2` |
| `RETRY_ATTEMPTS` | Both | Retry attempts, defaults to `3` |
| `RETRY_COOLDOWN_SECONDS` | Both | Base cooldown seconds, defaults to `2` |
| `INTERNAL_MONITORING_API_URL` | Both | Optional monitoring endpoint |
| `FAILURE_MODE` | Both | Defaults to `raise`; use `response` for local testing |
| `RDS_HOST` | `AWS_S3toRDS.py` | RDS host |
| `RDS_USER` | `AWS_S3toRDS.py` | RDS user |
| `RDS_PASSWORD` | `AWS_S3toRDS.py` | RDS password |
| `RDS_DATABASE` | `AWS_S3toRDS.py` | RDS database |
| `RDS_PORT` | `AWS_S3toRDS.py` | RDS port, defaults to `3306` |

## Positioning

This is a source-code reconstruction of a production-style AWS ETL workflow. It keeps the public repo small while showing the core engineering concerns: versioned raw storage, replayability, validation, explicit upsert behavior, audit records, lineage, retry handling, and monitoring integration points.
