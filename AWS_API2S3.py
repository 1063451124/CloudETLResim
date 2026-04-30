"""
AWS_API2S3.py

Source -> ODS Lambda.

Scope note:
- This repository contains Lambda source code only.
- AWS deployment artifacts such as Lambda runtime, IAM role, S3 bucket, EventBridge,
  Step Functions, Lambda Layer, and environment wiring are assumed to be managed
  in the AWS account, not in this source-only reconstruction.
- In the original production setup, sensitive values such as API keys should be
  retrieved from AWS Secrets Manager. This demo keeps environment-variable access
  to make the code runnable in a lightweight simulation.
"""

import json
import os
import time
import uuid
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
import requests


DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "MSFT")
DEFAULT_MAX_DAILY_POINTS = int(os.getenv("MAX_DAILY_POINTS", "2"))
DEFAULT_RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
DEFAULT_RETRY_COOLDOWN_SECONDS = float(os.getenv("RETRY_COOLDOWN_SECONDS", "2"))


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def monitor_event(event_name, payload=None, level="INFO"):
    """
    Simulated internal monitoring hook.

    In a real AWS deployment this could call an internal observability API,
    emit CloudWatch Embedded Metrics Format, or forward events through Firehose.
    Here it is intentionally a lightweight stub.

    Optional environment variable:
        INTERNAL_MONITORING_API_URL=https://internal.example/etl/events
    """
    payload = payload or {}
    event = {
        "level": level,
        "event_name": event_name,
        "timestamp": now_utc_iso(),
        "payload": payload,
    }

    monitor_url = os.getenv("INTERNAL_MONITORING_API_URL")

    if not monitor_url:
        print(f"[MONITORING_STUB] {json.dumps(event, default=str)}")
        return

    try:
        body = json.dumps(event, default=str).encode("utf-8")
        request = urllib.request.Request(
            monitor_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            print(f"[MONITORING_API] event={event_name}, status={response.status}")
    except Exception as exc:
        # Monitoring failure must not break the ETL path.
        print(f"[MONITORING_STUB_FAILED] event={event_name}, error={str(exc)}")


def with_retry(operation_name, func, attempts=DEFAULT_RETRY_ATTEMPTS, cooldown_seconds=DEFAULT_RETRY_COOLDOWN_SECONDS):
    """
    Simple retry + cooldown wrapper.

    This is intentionally lightweight. In AWS production, Step Functions retry policy
    can also handle retry/backoff at orchestration level.
    """
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            monitor_event(
                "operation_attempt",
                {"operation": operation_name, "attempt": attempt, "max_attempts": attempts},
            )
            return func()
        except Exception as exc:
            last_error = exc
            monitor_event(
                "operation_failed_attempt",
                {
                    "operation": operation_name,
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                level="WARN",
            )

            if attempt < attempts:
                sleep_seconds = cooldown_seconds * attempt
                print(f"[RETRY] operation={operation_name}, attempt={attempt}, sleep={sleep_seconds:.2f}s")
                time.sleep(sleep_seconds)

    raise last_error


def decimal_from_string(value, field_name):
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"Invalid decimal field: {field_name}={value}") from exc


def validate_daily_record(date_str, values):
    """
    Data quality check for one Alpha Vantage daily record.

    Required Alpha Vantage fields:
    - 1. open
    - 2. high
    - 3. low
    - 4. close
    - 5. volume
    """
    required_fields = ["1. open", "2. high", "3. low", "4. close", "5. volume"]

    try:
        datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {date_str}") from exc

    missing_fields = [field for field in required_fields if field not in values]
    if missing_fields:
        raise ValueError(f"Missing required fields for {date_str}: {missing_fields}")

    normalized = {
        "1. open": decimal_from_string(values["1. open"], "1. open"),
        "2. high": decimal_from_string(values["2. high"], "2. high"),
        "3. low": decimal_from_string(values["3. low"], "3. low"),
        "4. close": decimal_from_string(values["4. close"], "4. close"),
    }

    try:
        volume = int(values["5. volume"])
        if volume < 0:
            raise ValueError("volume must be non-negative")
    except Exception as exc:
        raise ValueError(f"Invalid volume field for {date_str}: {values['5. volume']}") from exc

    normalized["5. volume"] = str(volume)
    return normalized


def validate_and_normalize_time_series(api_payload, max_daily_points):
    """
    Schema, field, and empty-data validation.

    This keeps the demo behavior of limiting the payload size, but makes the
    contract explicit and auditable.
    """
    if not isinstance(api_payload, dict):
        raise ValueError("API response must be a JSON object")

    if "Error Message" in api_payload:
        raise ValueError(f"Alpha Vantage error response: {api_payload['Error Message']}")

    if "Note" in api_payload:
        raise ValueError(f"Alpha Vantage rate-limit or note response: {api_payload['Note']}")

    time_series = api_payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict) or not time_series:
        raise ValueError("Empty or missing Time Series (Daily)")

    selected_items = list(time_series.items())[:max_daily_points]
    normalized = {}

    for date_str, values in selected_items:
        if not isinstance(values, dict):
            raise ValueError(f"Invalid daily record payload for {date_str}")
        normalized[date_str] = validate_daily_record(date_str, values)

    if not normalized:
        raise ValueError("No valid stock records found after validation")

    return normalized


def fetch_alpha_vantage_daily(symbol):
    """
    Fetch daily stock data from Alpha Vantage.

    Secret note:
    - For the original AWS deployment, ALPHAVANTAGE_API_KEY should be supplied
      through Secrets Manager or deployment-time secret injection.
    - The fallback 'demo' value is only for local simulation.
    """
    api_key = os.getenv("ALPHAVANTAGE_API_KEY", "demo")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
    }

    print(f"[1] Starting API request: symbol={symbol}, url={url}")

    response = requests.get(url, params=params, timeout=10)
    print(f"[1] API response status code: {response.status_code}")

    response.raise_for_status()
    return response.json()


def build_versioned_s3_key(symbol, run_id, business_date=None):
    """
    Versioned S3 partition path.

    This avoids overwriting latest_data.json and supports replay/backfill from S3.
    """
    business_date = business_date or datetime.now(timezone.utc).date().isoformat()

    return (
        f"stock-data/raw/"
        f"symbol={symbol}/"
        f"dt={business_date}/"
        f"run_id={run_id}/"
        f"daily.json"
    )


def save_to_s3(stock_data, symbol, run_id, business_date=None):
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    if not s3_bucket:
        raise RuntimeError("Missing required environment variable: S3_BUCKET_NAME")

    s3_key = build_versioned_s3_key(symbol=symbol, run_id=run_id, business_date=business_date)

    body = {
        "metadata": {
            "source": "alpha_vantage",
            "dataset": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "run_id": run_id,
            "record_count": len(stock_data),
            "created_at": now_utc_iso(),
            "note": "ODS/raw versioned payload. Replay/backfill can read this key.",
        },
        "data": stock_data,
    }

    print(f"[2] Preparing to store to S3: bucket={s3_bucket}, key={s3_key}")

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(body, indent=2, default=str),
        ContentType="application/json",
        Metadata={
            "symbol": symbol,
            "run_id": run_id,
            "source": "alpha_vantage",
            "record_count": str(len(stock_data)),
        },
    )

    print(f"[2] Data successfully stored to S3: {s3_key}")
    return s3_key


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "local")
    run_id = event.get("run_id") or str(uuid.uuid4())
    symbol = event.get("symbol") or DEFAULT_SYMBOL
    max_daily_points = int(event.get("max_daily_points") or DEFAULT_MAX_DAILY_POINTS)
    business_date = event.get("business_date")

    print(f"=== Lambda execution started: request_id={request_id}, run_id={run_id}, symbol={symbol} ===")

    monitor_event(
        "api_to_s3_started",
        {
            "request_id": request_id,
            "run_id": run_id,
            "symbol": symbol,
            "max_daily_points": max_daily_points,
        },
    )

    try:
        raw_payload = with_retry(
            "fetch_alpha_vantage_daily",
            lambda: fetch_alpha_vantage_daily(symbol),
        )

        stock_data = validate_and_normalize_time_series(raw_payload, max_daily_points=max_daily_points)

        s3_key = with_retry(
            "save_stock_payload_to_s3",
            lambda: save_to_s3(stock_data, symbol=symbol, run_id=run_id, business_date=business_date),
        )

        result = {
            "statusCode": 200,
            "s3_bucket": os.getenv("S3_BUCKET_NAME"),
            "s3_key": s3_key,
            "symbol": symbol,
            "run_id": run_id,
            "record_count": len(stock_data),
        }

        monitor_event("api_to_s3_succeeded", result)
        return result

    except Exception as exc:
        error_payload = {
            "request_id": request_id,
            "run_id": run_id,
            "symbol": symbol,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        print(f"[ERROR] Lambda execution failed: {json.dumps(error_payload)}")
        monitor_event("api_to_s3_failed", error_payload, level="ERROR")

        return {
            "statusCode": 500,
            "body": json.dumps(error_payload),
        }
