from __future__ import annotations

import os
import time
from typing import Any, Dict, List

import boto3

from compose_runner.aws_lambda.common import LambdaRequest

_LOGS_CLIENT = boto3.client("logs", region_name=os.environ.get("AWS_REGION", "us-east-1"))

LOG_GROUP_ENV = "RUNNER_LOG_GROUP"
DEFAULT_LOOKBACK_MS_ENV = "DEFAULT_LOOKBACK_MS"


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    request = LambdaRequest.parse(event)
    payload = request.payload
    artifact_prefix = payload.get("artifact_prefix")
    if not artifact_prefix:
        message = "Request payload must include 'artifact_prefix'."
        if request.is_http:
            return request.bad_request(message, status_code=400)
        raise KeyError(message)
    next_token = payload.get("next_token")
    start_time = payload.get("start_time")
    end_time = payload.get("end_time")

    log_group = os.environ[LOG_GROUP_ENV]
    lookback_ms = int(os.environ.get(DEFAULT_LOOKBACK_MS_ENV, "3600000"))

    # Default window: look back from now if caller omitted explicit range.
    if start_time is None:
        now_ms = int(time.time() * 1000)
        end_time = end_time or now_ms
        start_time = end_time - lookback_ms

    params: Dict[str, Any] = {
        "logGroupName": log_group,
        "filterPattern": f'"{artifact_prefix}"',
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
        "artifact_prefix": artifact_prefix,
        "events": events,
        "next_token": response.get("nextToken"),
    }
    return request.respond(body)
