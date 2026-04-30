"""
AWS_S3toRDS.py

ODS -> ADS Lambda.

Scope note:
- This repository contains Lambda source code only.
- AWS deployment artifacts such as Lambda runtime, IAM role, VPC config,
  RDS networking, Step Functions, Lambda Layer, and environment wiring are
  assumed to be managed in AWS, not in this source-only reconstruction.
- In the original production setup, RDS credentials should be retrieved from
  AWS Secrets Manager. This demo keeps environment-variable access to make the
  code runnable in a lightweight simulation.
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
import mysql.connector
from mysql.connector import Error


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


def get_db_config():
    """
    Secret note:
    - For the original AWS deployment, RDS credentials should be read from
      Secrets Manager or injected by deployment tooling.
    - Environment-variable lookup is kept here for repo-level simulation.
    """
    db_config = {
        "host": os.getenv("RDS_HOST"),
        "user": os.getenv("RDS_USER"),
        "password": os.getenv("RDS_PASSWORD"),
        "database": os.getenv("RDS_DATABASE"),
        "port": int(os.getenv("RDS_PORT", "3306")),
    }

    missing = [key for key, value in db_config.items() if value in (None, "")]
    if missing:
        raise RuntimeError(f"Missing database configuration fields: {missing}")

    return db_config


def read_s3_payload(bucket, key):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    payload = json.loads(body)

    if isinstance(payload, dict) and "data" in payload:
        metadata = payload.get("metadata", {})
        data = payload["data"]
    else:
        # Backward compatibility for older latest_data.json layout.
        metadata = {}
        data = payload

    return metadata, data


def decimal_from_string(value, field_name):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"Invalid decimal field: {field_name}={value}") from exc


def validate_stock_payload(stock_data):
    """
    Schema, field, and empty-data validation before database write.
    """
    if not isinstance(stock_data, dict):
        raise ValueError("Stock payload must be a JSON object keyed by date")

    if not stock_data:
        raise ValueError("Stock payload is empty")

    required_fields = ["1. open", "2. high", "3. low", "4. close", "5. volume"]
    normalized_rows = []

    for date_str, values in stock_data.items():
        if not isinstance(values, dict):
            raise ValueError(f"Invalid record for date={date_str}: expected object")

        try:
            trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid date format: {date_str}") from exc

        missing_fields = [field for field in required_fields if field not in values]
        if missing_fields:
            raise ValueError(f"Missing fields for date={date_str}: {missing_fields}")

        open_price = decimal_from_string(values["1. open"], "1. open")
        high_price = decimal_from_string(values["2. high"], "2. high")
        low_price = decimal_from_string(values["3. low"], "3. low")
        close_price = decimal_from_string(values["4. close"], "4. close")

        try:
            volume = int(values["5. volume"])
            if volume < 0:
                raise ValueError("volume must be non-negative")
        except Exception as exc:
            raise ValueError(f"Invalid volume for date={date_str}: {values['5. volume']}") from exc

        normalized_rows.append(
            {
                "date": trade_date,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }
        )

    return normalized_rows


def ensure_audit_tables(cursor):
    """
    Lightweight audit tables.

    These are included to make conflict and failure records explicit.
    In a stricter production setup, table DDL would be managed by migration tooling.
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_load_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            source_bucket VARCHAR(255) NOT NULL,
            source_key VARCHAR(1024) NOT NULL,
            status VARCHAR(32) NOT NULL,
            record_count INT NOT NULL DEFAULT 0,
            inserted_or_updated_count INT NOT NULL DEFAULT 0,
            conflict_count INT NOT NULL DEFAULT 0,
            error_message TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_bad_records (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            source_bucket VARCHAR(255) NOT NULL,
            source_key VARCHAR(1024) NOT NULL,
            record_payload JSON NULL,
            error_message TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def insert_audit_record(
    cursor,
    run_id,
    symbol,
    bucket,
    key,
    status,
    record_count=0,
    inserted_or_updated_count=0,
    conflict_count=0,
    error_message=None,
):
    cursor.execute(
        """
        INSERT INTO etl_load_audit
        (
            run_id,
            symbol,
            source_bucket,
            source_key,
            status,
            record_count,
            inserted_or_updated_count,
            conflict_count,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            symbol,
            bucket,
            key,
            status,
            record_count,
            inserted_or_updated_count,
            conflict_count,
            error_message,
        ),
    )


def insert_bad_record(cursor, run_id, symbol, bucket, key, record_payload, error_message):
    cursor.execute(
        """
        INSERT INTO etl_bad_records
        (
            run_id,
            symbol,
            source_bucket,
            source_key,
            record_payload,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            symbol,
            bucket,
            key,
            json.dumps(record_payload, default=str),
            error_message,
        ),
    )


def save_to_rds(stock_data, symbol, run_id, source_bucket, source_key):
    """
    Explicit upsert strategy.

    Required target table assumption:
        UNIQUE KEY uk_stock_symbol_date (symbol, date)

    Conflict handling:
    - Existing symbol/date rows are updated.
    - The change is auditable through etl_load_audit.
    - Validation failures can be recorded in etl_bad_records.
    """
    db_config = get_db_config()
    connection = None
    cursor = None

    try:
        rows = validate_stock_payload(stock_data)

        connection = mysql.connector.connect(**db_config, connect_timeout=10)
        connection.autocommit = False
        cursor = connection.cursor()

        ensure_audit_tables(cursor)

        # Count existing keys before upsert for explicit conflict audit.
        conflict_count = 0
        for row in rows:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM stock_prices
                WHERE symbol = %s AND date = %s
                """,
                (symbol, row["date"]),
            )
            conflict_count += int(cursor.fetchone()[0])

        upsert_sql = """
        INSERT INTO stock_prices
            (symbol, date, open, high, low, close, volume, last_run_id, updated_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume),
            last_run_id = VALUES(last_run_id),
            updated_at = CURRENT_TIMESTAMP
        """

        for row in rows:
            cursor.execute(
                upsert_sql,
                (
                    symbol,
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    run_id,
                ),
            )

        inserted_or_updated_count = len(rows)

        insert_audit_record(
            cursor=cursor,
            run_id=run_id,
            symbol=symbol,
            bucket=source_bucket,
            key=source_key,
            status="SUCCESS",
            record_count=len(rows),
            inserted_or_updated_count=inserted_or_updated_count,
            conflict_count=conflict_count,
            error_message=None,
        )

        connection.commit()

        return {
            "record_count": len(rows),
            "inserted_or_updated_count": inserted_or_updated_count,
            "conflict_count": conflict_count,
        }

    except ValueError as exc:
        if connection is not None and connection.is_connected() and cursor is not None:
            try:
                ensure_audit_tables(cursor)
                insert_bad_record(
                    cursor=cursor,
                    run_id=run_id,
                    symbol=symbol,
                    bucket=source_bucket,
                    key=source_key,
                    record_payload=stock_data,
                    error_message=str(exc),
                )
                insert_audit_record(
                    cursor=cursor,
                    run_id=run_id,
                    symbol=symbol,
                    bucket=source_bucket,
                    key=source_key,
                    status="FAILED_VALIDATION",
                    error_message=str(exc),
                )
                connection.commit()
            except Exception:
                connection.rollback()

        raise

    except Error as exc:
        if connection is not None and connection.is_connected():
            connection.rollback()
        raise RuntimeError(f"Database error: {str(exc)}") from exc

    except Exception as exc:
        if connection is not None and connection.is_connected():
            connection.rollback()
        raise RuntimeError(f"Unexpected load error: {str(exc)}") from exc

    finally:
        if cursor is not None:
            cursor.close()
            print("[3] Cursor closed.")

        if connection is not None and connection.is_connected():
            connection.close()
            print("[3] Connection closed.")

        print("[3] Data storage process finished.")


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "local")

    print(f"=== Lambda2 execution started: request_id={request_id} ===")

    try:
        bucket = event["s3_bucket"]
        key = event["s3_key"]
        symbol = event.get("symbol", os.getenv("DEFAULT_SYMBOL", "MSFT"))
        run_id = event.get("run_id", "unknown-run-id")
    except KeyError as exc:
        error_payload = {
            "request_id": request_id,
            "error_type": "InvalidEvent",
            "error": f"Missing required event key: {str(exc)}",
        }
        monitor_event("s3_to_rds_invalid_event", error_payload, level="ERROR")
        return {
            "statusCode": 400,
            "body": json.dumps(error_payload),
        }

    monitor_event(
        "s3_to_rds_started",
        {
            "request_id": request_id,
            "run_id": run_id,
            "symbol": symbol,
            "s3_bucket": bucket,
            "s3_key": key,
        },
    )

    try:
        metadata, stock_data = with_retry(
            "read_s3_payload",
            lambda: read_s3_payload(bucket, key),
        )

        # Prefer metadata from the ODS object if present.
        symbol = metadata.get("symbol", symbol)
        run_id = metadata.get("run_id", run_id)

        result = with_retry(
            "save_to_rds",
            lambda: save_to_rds(
                stock_data=stock_data,
                symbol=symbol,
                run_id=run_id,
                source_bucket=bucket,
                source_key=key,
            ),
        )

        success_payload = {
            "request_id": request_id,
            "run_id": run_id,
            "symbol": symbol,
            "s3_bucket": bucket,
            "s3_key": key,
            **result,
        }

        monitor_event("s3_to_rds_succeeded", success_payload)

        return {
            "statusCode": 200,
            "body": json.dumps(success_payload, default=str),
        }

    except Exception as exc:
        error_payload = {
            "request_id": request_id,
            "run_id": run_id,
            "symbol": symbol,
            "s3_bucket": bucket,
            "s3_key": key,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

        print(f"[ERROR] Lambda2 failed: {json.dumps(error_payload, default=str)}")
        monitor_event("s3_to_rds_failed", error_payload, level="ERROR")

        return {
            "statusCode": 500,
            "body": json.dumps(error_payload, default=str),
        }
