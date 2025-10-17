from __future__ import annotations

import os
import base64
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

_S3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))

RESULTS_BUCKET_ENV = "RESULTS_BUCKET"
RESULTS_PREFIX_ENV = "RESULTS_PREFIX"
DEFAULT_EXPIRES_IN = 900


def _serialize_dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _is_http_event(event: Any) -> bool:
    return isinstance(event, dict) and "requestContext" in event


def _extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if not _is_http_event(event):
        return event
    body = event.get("body")
    if not body:
        return {}
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return json.loads(body)


def _http_response(body: Dict[str, Any], status_code: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    raw_event = event
    event = _extract_payload(event)
    bucket = os.environ[RESULTS_BUCKET_ENV]
    prefix = os.environ.get(RESULTS_PREFIX_ENV)

    job_id = event.get("job_id")
    if not job_id:
        message = "Request payload must include 'job_id'."
        if _is_http_event(raw_event):
            return _http_response({"status": "FAILED", "error": message}, status_code=400)
        raise KeyError(message)
    expires_in = int(event.get("expires_in", DEFAULT_EXPIRES_IN))

    key_prefix = f"{prefix.rstrip('/')}/{job_id}" if prefix else job_id

    response = _S3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
    contents = response.get("Contents", [])

    artifacts: List[Dict[str, Any]] = []
    for obj in contents:
        key = obj["Key"]
        if key.endswith("/"):
            continue
        url = _S3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
        artifacts.append(
            {
                "key": key,
                "filename": key.split("/")[-1],
                "size": obj.get("Size"),
                "last_modified": _serialize_dt(obj["LastModified"]),
                "url": url,
            }
        )

    body = {
        "job_id": job_id,
        "artifacts": artifacts,
        "bucket": bucket,
        "prefix": key_prefix,
    }
    if _is_http_event(raw_event):
        return _http_response(body)
    return body
