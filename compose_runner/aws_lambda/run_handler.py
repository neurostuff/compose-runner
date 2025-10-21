from __future__ import annotations

import json
import logging
import os
import uuid
import urllib.error
import urllib.request
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

DEFAULT_TASK_SIZE = "standard"


def _log(job_id: str, message: str, **details: Any) -> None:
    payload = {"job_id": job_id, "message": message, **details}
    # Ensure consistent JSON logging for ingestion/filtering.
    logger.info(json.dumps(payload))


def _compose_api_base_url(environment: str) -> str:
    env = (environment or "production").lower()
    if env == "staging":
        return "https://synth.neurostore.xyz/api"
    if env == "local":
        return "http://localhost:81/api"
    return "https://compose.neurosynth.org/api"


def _fetch_meta_analysis(meta_analysis_id: str, environment: str) -> Optional[Dict[str, Any]]:
    base_url = _compose_api_base_url(environment).rstrip("/")
    url = f"{base_url}/meta-analyses/{meta_analysis_id}?nested=true"
    request = urllib.request.Request(url, headers={"User-Agent": "compose-runner/submit"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        logger.warning("Failed to fetch meta-analysis %s: %s", meta_analysis_id, exc)
        return None


def _requires_large_task(specification: Dict[str, Any]) -> bool:
    if not isinstance(specification, dict):
        return False
    corrector = specification.get("corrector")
    if not isinstance(corrector, dict):
        return False
    if corrector.get("type") != "FWECorrector":
        return False
    args = corrector.get("args")
    if not isinstance(args, dict):
        return False
    method = args.get("method")
    if method is None:
        kwargs = args.get("**kwargs")
        if isinstance(kwargs, dict):
            method = kwargs.get("method")
    if isinstance(method, str) and method.lower() == "montecarlo":
        return True
    return False


def _select_task_size(meta_analysis_id: str, environment: str, artifact_prefix: str) -> str:
    doc = _fetch_meta_analysis(meta_analysis_id, environment)
    if not doc:
        return DEFAULT_TASK_SIZE
    specification = doc.get("specification")
    try:
        if _requires_large_task(specification):
            _log(
                artifact_prefix,
                "workflow.task_size_selected",
                task_size="large",
                reason="montecarlo_fwe",
            )
            return "large"
    except Exception as exc:  # noqa: broad-except
        logger.warning("Failed to evaluate specification for %s: %s", meta_analysis_id, exc)
    return DEFAULT_TASK_SIZE


def _job_input(
    payload: Dict[str, Any],
    artifact_prefix: str,
    bucket: Optional[str],
    prefix: Optional[str],
    nsc_key: Optional[str],
    nv_key: Optional[str],
    task_size: str,
) -> Dict[str, Any]:
    no_upload_flag = bool(payload.get("no_upload", False))
    doc: Dict[str, Any] = {
        "artifact_prefix": artifact_prefix,
        "meta_analysis_id": payload["meta_analysis_id"],
        "environment": payload.get("environment", "production"),
        "no_upload": "true" if no_upload_flag else "false",
        "results": {"bucket": bucket or "", "prefix": prefix or ""},
        "task_size": task_size,
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

    environment = payload.get("environment", "production")
    task_size = _select_task_size(payload["meta_analysis_id"], environment, artifact_prefix)

    job_input = _job_input(payload, artifact_prefix, bucket, prefix, nsc_key, nv_key, task_size)
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
