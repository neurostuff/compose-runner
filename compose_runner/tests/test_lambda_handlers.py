from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from compose_runner.aws_lambda import log_poll_handler, results_handler, run_handler


class DummyContext:
    def __init__(self, request_id: str = "job-123") -> None:
        self.aws_request_id = request_id

    def get_remaining_time_in_millis(self) -> int:
        return 15_000


def _make_http_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "requestContext": {"http": {"method": "POST"}},
        "isBase64Encoded": False,
        "body": json.dumps(payload),
    }


def test_run_handler_http_success(monkeypatch, tmp_path):
    called = {}

    def fake_run(**kwargs):
        called.update(kwargs)
        return "https://result/url", None

    uploads = []

    class FakeS3:
        def upload_file(self, filename, bucket, key):
            uploads.append((filename, bucket, key))

    monkeypatch.setattr(run_handler, "run_compose", fake_run)
    monkeypatch.setattr(run_handler, "_S3_CLIENT", FakeS3())
    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    monkeypatch.setenv("RESULTS_PREFIX", "prefix")
    monkeypatch.setenv("NSC_KEY", "nsc")
    monkeypatch.setenv("NV_KEY", "nv")

    event = _make_http_event({"meta_analysis_id": "abc123", "environment": "production"})
    context = DummyContext("job-456")

    response = run_handler.handler(event, context)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["job_id"] == "job-456"
    assert body["status"] == "SUCCEEDED"
    assert called["meta_analysis_id"] == "abc123"
    assert called["environment"] == "production"
    assert called["nsc_key"] == "nsc"
    assert called["nv_key"] == "nv"
    assert uploads == []  # no files written during test


def test_run_handler_missing_meta_analysis(monkeypatch):
    event = _make_http_event({"environment": "production"})
    response = run_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "meta_analysis_id" in body["error"]


def test_log_poll_handler(monkeypatch):
    events_payload = [{"timestamp": 1, "message": '{"job_id":"id","message":"workflow.start"}'}]

    class FakeLogs:
        def filter_log_events(self, **kwargs):
            return {"events": events_payload, "nextToken": "token-1"}

    monkeypatch.setenv("RUNNER_LOG_GROUP", "/aws/lambda/test")
    monkeypatch.setenv("DEFAULT_LOOKBACK_MS", "1000")
    monkeypatch.setattr(log_poll_handler, "_LOGS_CLIENT", FakeLogs())

    event = {"job_id": "id"}
    result = log_poll_handler.handler(event, DummyContext())
    assert result["job_id"] == "id"
    assert result["next_token"] == "token-1"
    assert result["events"][0]["message"] == events_payload[0]["message"]


def test_log_poll_handler_http_missing_job_id(monkeypatch):
    monkeypatch.setenv("RUNNER_LOG_GROUP", "/aws/lambda/test")
    http_event = _make_http_event({})
    response = log_poll_handler.handler(http_event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "job_id" in body["error"]


def test_results_handler(monkeypatch):
    objects = [
        {"Key": "prefix/id/file1.nii.gz", "Size": 10, "LastModified": results_handler.datetime.now()}
    ]

    class FakeS3:
        def list_objects_v2(self, Bucket, Prefix):
            assert Bucket == "bucket"
            assert Prefix == "prefix/id"
            return {"Contents": objects}

        def generate_presigned_url(self, client_method, Params, ExpiresIn):
            assert client_method == "get_object"
            assert Params["Bucket"] == "bucket"
            assert Params["Key"] == objects[0]["Key"]
            assert ExpiresIn == 900
            return "https://signed/url"

    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    monkeypatch.setenv("RESULTS_PREFIX", "prefix")
    monkeypatch.setattr(results_handler, "_S3", FakeS3())

    event = _make_http_event({"job_id": "id"})
    response = results_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert body["job_id"] == "id"
    assert body["artifacts"][0]["url"] == "https://signed/url"
    assert body["artifacts"][0]["filename"] == "file1.nii.gz"


def test_results_handler_missing_job_id(monkeypatch):
    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    event = _make_http_event({})
    response = results_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "job_id" in body["error"]
