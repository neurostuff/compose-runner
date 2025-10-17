from __future__ import annotations

import os
import time
import base64
import json
from typing import Any, Dict, List

import boto3

_LOGS_CLIENT = boto3.client("logs", region_name=os.environ.get("AWS_REGION", "us-east-1"))

LOG_GROUP_ENV = "RUNNER_LOG_GROUP"
DEFAULT_LOOKBACK_MS_ENV = "DEFAULT_LOOKBACK_MS"

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
    job_id = event.get("job_id")
    if not job_id:
        message = "Request payload must include 'job_id'."
        if _is_http_event(raw_event):
            return _http_response({"status": "FAILED", "error": message}, status_code=400)
        raise KeyError(message)
    next_token = event.get("next_token")
    start_time = event.get("start_time")
    end_time = event.get("end_time")

    log_group = os.environ[LOG_GROUP_ENV]
    lookback_ms = int(os.environ.get(DEFAULT_LOOKBACK_MS_ENV, "3600000"))

    # Default window: look back from now if caller omitted explicit range.
    if start_time is None:
        now_ms = int(time.time() * 1000)
        end_time = end_time or now_ms
        start_time = end_time - lookback_ms

    params: Dict[str, Any] = {
        "logGroupName": log_group,
        "filterPattern": f'"{job_id}"',
        "startTime": int(start_time),
    }
    if end_time is not None:
        params["endTime"] = int(end_time)
    if next_token:
        params["nextToken"] = next_token

    response = _LOGS_CLIENT.filter_log_events(**params)
    events: List[Dict[str, Any]] = [
        {"timestamp": item["timestamp"], "message": item["message"]}
        for item in response.get("events", [])
    ]

    body = {
        "job_id": job_id,
        "events": events,
        "next_token": response.get("nextToken"),
    }
    if _is_http_event(raw_event):
        return _http_response(body)
    return body
