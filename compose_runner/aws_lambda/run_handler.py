from __future__ import annotations

import json
import logging
import os
import base64
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import boto3

NUMBA_CACHE_DIR = Path(os.environ.get("NUMBA_CACHE_DIR", "/tmp/numba_cache"))
NUMBA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ["NUMBA_CACHE_DIR"] = str(NUMBA_CACHE_DIR)

from compose_runner.run import run as run_compose

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_S3_CLIENT = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


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


def _log(job_id: str, message: str, **details: Any) -> None:
    payload = {"job_id": job_id, "message": message, **details}
    # Ensure consistent JSON logging for ingestion/filtering.
    logger.info(json.dumps(payload))


def _iter_result_files(result_dir: Path) -> Iterable[Path]:
    for path in result_dir.iterdir():
        if path.is_file():
            yield path


def _upload_results(job_id: str, result_dir: Path, bucket: str, prefix: Optional[str]) -> None:
    base_prefix = f"{prefix.rstrip('/')}/{job_id}" if prefix else job_id
    for file_path in _iter_result_files(result_dir):
        key = f"{base_prefix}/{file_path.name}"
        _S3_CLIENT.upload_file(str(file_path), bucket, key)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    raw_event = event
    payload = _extract_payload(event)
    job_id = context.aws_request_id
    if "meta_analysis_id" not in payload:
        message = "Request payload must include 'meta_analysis_id'."
        _log(job_id, "workflow.failed", error=message)
        if _is_http_event(raw_event):
            return _http_response(
                {"job_id": job_id, "status": "FAILED", "error": message}, status_code=400
            )
        raise KeyError(message)
    meta_analysis_id = payload["meta_analysis_id"]
    environment = payload.get("environment", "production")
    nsc_key = payload.get("nsc_key") or os.environ.get("NSC_KEY")
    nv_key = payload.get("nv_key") or os.environ.get("NV_KEY")
    no_upload = bool(payload.get("no_upload", False))
    n_cores = payload.get("n_cores")

    result_dir = Path("/tmp") / job_id
    result_dir.mkdir(parents=True, exist_ok=True)

    bucket = os.environ.get("RESULTS_BUCKET")
    prefix = os.environ.get("RESULTS_PREFIX")

    _log(
        job_id,
        "workflow.start",
        meta_analysis_id=meta_analysis_id,
        environment=environment,
        no_upload=no_upload,
    )
    try:
        url, _ = run_compose(
            meta_analysis_id=meta_analysis_id,
            environment=environment,
            result_dir=str(result_dir),
            nsc_key=nsc_key,
            nv_key=nv_key,
            no_upload=no_upload,
            n_cores=n_cores,
        )
        _log(job_id, "workflow.completed", result_url=url)

        if bucket:
            _upload_results(job_id, result_dir, bucket, prefix)
            _log(job_id, "artifacts.uploaded", bucket=bucket, prefix=prefix)

        body = {
            "job_id": job_id,
            "status": "SUCCEEDED",
            "result_url": url,
            "artifacts_bucket": bucket,
            "artifacts_prefix": prefix,
        }
        if _is_http_event(raw_event):
            return _http_response(body)
        return body
    except Exception as exc:  # noqa: broad-except - bubble up but log context
        _log(job_id, "workflow.failed", error=str(exc))
        if _is_http_event(raw_event):
            return _http_response(
                {"job_id": job_id, "status": "FAILED", "error": str(exc)}, status_code=500
            )
        raise
    finally:
        if os.environ.get("DELETE_TMP", "true").lower() == "true":
            for path in _iter_result_files(result_dir):
                try:
                    path.unlink()
                except OSError:
                    _log(job_id, "cleanup.warning", file=str(path))
