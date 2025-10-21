from __future__ import annotations

import base64
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

_SFN = boto3.client("stepfunctions", region_name=os.environ.get("AWS_REGION", "us-east-1"))
_S3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))

RESULTS_BUCKET_ENV = "RESULTS_BUCKET"
RESULTS_PREFIX_ENV = "RESULTS_PREFIX"
METADATA_FILENAME = "metadata.json"


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


def _serialize_dt(value: datetime) -> str:
    return value.astimezone().isoformat()


def _metadata_key(prefix: Optional[str], artifact_prefix: str) -> str:
    if prefix:
        return f"{prefix.rstrip('/')}/{artifact_prefix}/{METADATA_FILENAME}"
    return f"{artifact_prefix}/{METADATA_FILENAME}"


def _load_metadata(bucket: str, prefix: Optional[str], artifact_prefix: str) -> Optional[Dict[str, Any]]:
    key = _metadata_key(prefix, artifact_prefix)
    try:
        response = _S3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise
    data = response["Body"].read()
    return json.loads(data.decode("utf-8"))


def _parse_output(output: Optional[str]) -> Dict[str, Any]:
    if not output:
        return {}
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        return {"raw_output": output}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    raw_event = event
    event = _extract_payload(event)

    job_id = event.get("job_id")
    if not job_id:
        message = "Request payload must include 'job_id'."
        if _is_http_event(raw_event):
            return _http_response({"status": "FAILED", "error": message}, status_code=400)
        raise KeyError(message)

    try:
        description = _SFN.describe_execution(executionArn=job_id)
    except ClientError as error:
        body = {"status": "FAILED", "error": error.response["Error"]["Message"]}
        if _is_http_event(raw_event):
            status_code = 404 if error.response["Error"]["Code"] == "ExecutionDoesNotExist" else 500
            return _http_response(body, status_code=status_code)
        raise

    status = description["status"]
    body: Dict[str, Any] = {
        "job_id": job_id,
        "status": status,
        "start_time": _serialize_dt(description["startDate"]),
    }
    if "stopDate" in description:
        body["stop_time"] = _serialize_dt(description["stopDate"])

    output_doc = _parse_output(description.get("output"))
    body["output"] = output_doc

    artifact_prefix = output_doc.get("artifact_prefix") or output_doc.get("run_id")
    body["artifact_prefix"] = artifact_prefix

    if status in {"SUCCEEDED", "FAILED"}:
        results_info = output_doc.get("results") or {}
        bucket = results_info.get("bucket") or os.environ.get(RESULTS_BUCKET_ENV)
        prefix = results_info.get("prefix") or os.environ.get(RESULTS_PREFIX_ENV)

        if bucket and artifact_prefix:
            metadata = _load_metadata(bucket, prefix, artifact_prefix)
            if metadata:
                body["result"] = metadata

        if status == "FAILED":
            body["error"] = output_doc.get("error")

    if _is_http_event(raw_event):
        return _http_response(body)
    return body
