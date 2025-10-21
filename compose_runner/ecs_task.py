from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import boto3

from compose_runner.run import run as run_compose

NUMBA_CACHE_DIR = Path(os.environ.get("NUMBA_CACHE_DIR", "/tmp/numba_cache"))
NUMBA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ["NUMBA_CACHE_DIR"] = str(NUMBA_CACHE_DIR)

logger = logging.getLogger("compose_runner.ecs_task")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

_S3_CLIENT = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))

RESULTS_BUCKET_ENV = "RESULTS_BUCKET"
RESULTS_PREFIX_ENV = "RESULTS_PREFIX"
ARTIFACT_PREFIX_ENV = "ARTIFACT_PREFIX"
META_ANALYSIS_ENV = "META_ANALYSIS_ID"
ENVIRONMENT_ENV = "ENVIRONMENT"
NSC_KEY_ENV = "NSC_KEY"
NV_KEY_ENV = "NV_KEY"
NO_UPLOAD_ENV = "NO_UPLOAD"
N_CORES_ENV = "N_CORES"
DELETE_TMP_ENV = "DELETE_TMP"
METADATA_FILENAME = "metadata.json"


def _log(artifact_prefix: str, message: str, **details: Any) -> None:
    payload = {"artifact_prefix": artifact_prefix, "message": message, **details}
    logger.info(json.dumps(payload))


def _iter_result_files(result_dir: Path) -> Iterable[Path]:
    for path in result_dir.iterdir():
        if path.is_file():
            yield path


def _upload_results(artifact_prefix: str, result_dir: Path, bucket: str, prefix: Optional[str]) -> None:
    base_prefix = f"{prefix.rstrip('/')}/{artifact_prefix}" if prefix else artifact_prefix
    for file_path in _iter_result_files(result_dir):
        key = f"{base_prefix}/{file_path.name}"
        _S3_CLIENT.upload_file(str(file_path), bucket, key)


def _write_metadata(bucket: str, prefix: Optional[str], artifact_prefix: str, metadata: Dict[str, Any]) -> None:
    base_prefix = f"{prefix.rstrip('/')}/{artifact_prefix}" if prefix else artifact_prefix
    key = f"{base_prefix}/{METADATA_FILENAME}"
    metadata["metadata_key"] = key
    _S3_CLIENT.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(metadata).encode("utf-8"),
        ContentType="application/json",
    )


def _bool_from_env(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.lower() in {"1", "true", "t", "yes", "y"}


def _resolve_n_cores(env_value: Optional[str]) -> Optional[int]:
    if env_value:
        return int(env_value)
    detected_cores = os.cpu_count()
    return detected_cores if detected_cores else None


def main() -> None:
    if ARTIFACT_PREFIX_ENV not in os.environ:
        raise RuntimeError(f"{ARTIFACT_PREFIX_ENV} environment variable must be set.")
    if META_ANALYSIS_ENV not in os.environ:
        raise RuntimeError(f"{META_ANALYSIS_ENV} environment variable must be set.")

    artifact_prefix = os.environ[ARTIFACT_PREFIX_ENV]
    meta_analysis_id = os.environ[META_ANALYSIS_ENV]
    environment = os.environ.get(ENVIRONMENT_ENV, "production")
    nsc_key = os.environ.get(NSC_KEY_ENV) or None
    nv_key = os.environ.get(NV_KEY_ENV) or None
    no_upload = _bool_from_env(os.environ.get(NO_UPLOAD_ENV))
    n_cores = _resolve_n_cores(os.environ.get(N_CORES_ENV))
    compose_runner_version = os.environ.get("COMPOSE_RUNNER_VERSION", "unknown")

    bucket = os.environ.get(RESULTS_BUCKET_ENV)
    prefix = os.environ.get(RESULTS_PREFIX_ENV)

    result_dir = Path("/tmp") / artifact_prefix
    result_dir.mkdir(parents=True, exist_ok=True)

    _log(
        artifact_prefix,
        "workflow.start",
        meta_analysis_id=meta_analysis_id,
        environment=environment,
        no_upload=no_upload,
        compose_runner_version=compose_runner_version,
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
        _log(artifact_prefix, "workflow.completed", result_url=url)

        metadata: Dict[str, Any] = {
            "artifact_prefix": artifact_prefix,
            "meta_analysis_id": meta_analysis_id,
            "result_url": url,
            "artifacts_bucket": bucket,
            "artifacts_prefix": prefix,
            "compose_runner_version": compose_runner_version,
        }

        if bucket:
            _upload_results(artifact_prefix, result_dir, bucket, prefix)
            _log(artifact_prefix, "artifacts.uploaded", bucket=bucket, prefix=prefix)
            _write_metadata(bucket, prefix, artifact_prefix, metadata)
            _log(artifact_prefix, "metadata.written", bucket=bucket, prefix=prefix)

        _log(artifact_prefix, "workflow.success", result_url=url)
    except Exception as exc:  # noqa: broad-except
        _log(artifact_prefix, "workflow.failed", error=str(exc))
        raise
    finally:
        delete_tmp = _bool_from_env(os.environ.get(DELETE_TMP_ENV, "true"))
        if delete_tmp:
            for path in _iter_result_files(result_dir):
                try:
                    path.unlink()
                except OSError:
                    _log(artifact_prefix, "cleanup.warning", file=str(path))


if __name__ == "__main__":
    main()
