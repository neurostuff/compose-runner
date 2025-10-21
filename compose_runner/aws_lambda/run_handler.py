from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from compose_runner.aws_lambda.common import LambdaRequest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_SFN_CLIENT = boto3.client("stepfunctions", region_name=os.environ.get("AWS_REGION", "us-east-1"))

STATE_MACHINE_ARN_ENV = "STATE_MACHINE_ARN"
RESULTS_BUCKET_ENV = "RESULTS_BUCKET"
RESULTS_PREFIX_ENV = "RESULTS_PREFIX"
NSC_KEY_ENV = "NSC_KEY"
NV_KEY_ENV = "NV_KEY"


def _log(job_id: str, message: str, **details: Any) -> None:
    payload = {"job_id": job_id, "message": message, **details}
    # Ensure consistent JSON logging for ingestion/filtering.
    logger.info(json.dumps(payload))


def _job_input(
    payload: Dict[str, Any],
    artifact_prefix: str,
    bucket: Optional[str],
    prefix: Optional[str],
    nsc_key: Optional[str],
    nv_key: Optional[str],
) -> Dict[str, Any]:
    no_upload_flag = bool(payload.get("no_upload", False))
    doc: Dict[str, Any] = {
        "artifact_prefix": artifact_prefix,
        "meta_analysis_id": payload["meta_analysis_id"],
        "environment": payload.get("environment", "production"),
        "no_upload": "true" if no_upload_flag else "false",
        "results": {"bucket": bucket or "", "prefix": prefix or ""},
    }
    n_cores = payload.get("n_cores")
    doc["n_cores"] = str(n_cores) if n_cores is not None else ""
    if nsc_key is not None:
        doc["nsc_key"] = nsc_key
    else:
        doc["nsc_key"] = ""
    if nv_key is not None:
        doc["nv_key"] = nv_key
    else:
        doc["nv_key"] = ""
    return doc


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    request = LambdaRequest.parse(event)
    payload = request.payload
    if STATE_MACHINE_ARN_ENV not in os.environ:
        raise RuntimeError(f"{STATE_MACHINE_ARN_ENV} environment variable must be set.")

    if "meta_analysis_id" not in payload:
        message = "Request payload must include 'meta_analysis_id'."
        if request.is_http:
            return request.bad_request(message, status_code=400)
        raise KeyError(message)

    artifact_prefix = payload.get("artifact_prefix") or str(uuid.uuid4())
    bucket = os.environ.get(RESULTS_BUCKET_ENV)
    prefix = os.environ.get(RESULTS_PREFIX_ENV)
    nsc_key = payload.get("nsc_key") or os.environ.get(NSC_KEY_ENV)
    nv_key = payload.get("nv_key") or os.environ.get(NV_KEY_ENV)

    job_input = _job_input(payload, artifact_prefix, bucket, prefix, nsc_key, nv_key)
    params = {
        "stateMachineArn": os.environ[STATE_MACHINE_ARN_ENV],
        "name": artifact_prefix,
        "input": json.dumps(job_input),
    }

    try:
        response = _SFN_CLIENT.start_execution(**params)
    except _SFN_CLIENT.exceptions.ExecutionAlreadyExists as exc:
        _log(artifact_prefix, "workflow.duplicate", error=str(exc))
        body = {
            "status": "FAILED",
            "error": "A job with the provided artifact_prefix already exists.",
            "artifact_prefix": artifact_prefix,
        }
        if request.is_http:
            return request.respond(body, status_code=409)
        raise ValueError(body["error"]) from exc
    except ClientError as exc:
        _log(artifact_prefix, "workflow.failed_to_queue", error=str(exc))
        message = "Failed to start compose-runner job."
        body = {"status": "FAILED", "error": message}
        if request.is_http:
            return request.respond(body, status_code=500)
        raise RuntimeError(message) from exc

    execution_arn = response["executionArn"]
    _log(artifact_prefix, "workflow.queued", execution_arn=execution_arn)

    body = {
        "job_id": execution_arn,
        "artifact_prefix": artifact_prefix,
        "status": "SUBMITTED",
        "status_url": f"/jobs/{execution_arn}",
    }
    return request.respond(body, status_code=202)
