from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

from compose_runner.aws_lambda import log_poll_handler, results_handler, run_handler, status_handler


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


def test_requires_large_task_detection():
    spec = {"corrector": {"type": "FWECorrector", "args": {"method": "montecarlo"}}}
    assert run_handler._requires_large_task(spec)


def test_requires_large_task_false_when_method_differs():
    spec = {"corrector": {"type": "FWECorrector", "args": {"method": "bonferroni"}}}
    assert run_handler._requires_large_task(spec) is False


@pytest.mark.vcr(record_mode="once")
def test_select_task_size_uses_large_for_montecarlo():
    task_size = run_handler._select_task_size("ZPSvyvhZAopz", "staging", "artifact-test")
    assert task_size == "large"


@pytest.mark.vcr(record_mode="once")
def test_select_task_size_uses_standard_for_fdr():
    task_size = run_handler._select_task_size("VtFZJFniCKvG", "staging", "artifact-test")
    assert task_size == "standard"


def test_run_handler_http_success(monkeypatch, tmp_path):
    captured = {}

    class FakeSFN:
        def start_execution(self, **kwargs):
            captured.update(kwargs)
            return {"executionArn": "arn:aws:states:us-east-1:123:execution:state-machine:run-123"}

        class exceptions:
            class ExecutionAlreadyExists(Exception):
                ...

    monkeypatch.setattr(run_handler, "_SFN_CLIENT", FakeSFN())
    monkeypatch.setattr(run_handler, "_select_task_size", lambda *args: "standard")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:state-machine")
    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    monkeypatch.setenv("RESULTS_PREFIX", "prefix")
    monkeypatch.setenv("NSC_KEY", "nsc")
    monkeypatch.setenv("NV_KEY", "nv")

    event = _make_http_event(
        {"meta_analysis_id": "abc123", "environment": "production", "artifact_prefix": "artifact-123"}
    )
    context = DummyContext("unused")

    response = run_handler.handler(event, context)
    body = json.loads(response["body"])

    assert response["statusCode"] == 202
    assert body["job_id"].startswith("arn:aws:states")
    assert body["artifact_prefix"] == "artifact-123"
    assert body["status"] == "SUBMITTED"
    assert captured["name"] == "artifact-123"
    input_doc = json.loads(captured["input"])
    assert input_doc["artifact_prefix"] == "artifact-123"
    assert input_doc["meta_analysis_id"] == "abc123"
    assert input_doc["environment"] == "production"
    assert input_doc["results"]["bucket"] == "bucket"
    assert input_doc["results"]["prefix"] == "prefix"
    assert input_doc["nsc_key"] == "nsc"
    assert input_doc["nv_key"] == "nv"
    assert input_doc["task_size"] == "standard"


def test_run_handler_http_uses_large_task(monkeypatch):
    captured = {}

    class FakeSFN:
        def start_execution(self, **kwargs):
            captured.update(kwargs)
            return {"executionArn": "arn:aws:states:us-east-1:123:execution:state-machine:run-456"}

        class exceptions:
            class ExecutionAlreadyExists(Exception):
                ...

    monkeypatch.setattr(run_handler, "_SFN_CLIENT", FakeSFN())
    monkeypatch.setattr(run_handler, "_select_task_size", lambda *args: "large")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:state-machine")
    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    monkeypatch.setenv("RESULTS_PREFIX", "prefix")

    event = _make_http_event({"meta_analysis_id": "abc123"})
    response = run_handler.handler(event, DummyContext())
    assert response["statusCode"] == 202
    input_doc = json.loads(captured["input"])
    assert input_doc["task_size"] == "large"


def test_run_handler_missing_meta_analysis(monkeypatch):
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:state-machine")
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

    event = {"artifact_prefix": "id"}
    result = log_poll_handler.handler(event, DummyContext())
    assert result["artifact_prefix"] == "id"
    assert result["next_token"] == "token-1"
    assert result["events"][0]["message"] == events_payload[0]["message"]


def test_log_poll_handler_http_missing_job_id(monkeypatch):
    monkeypatch.setenv("RUNNER_LOG_GROUP", "/aws/lambda/test")
    http_event = _make_http_event({})
    response = log_poll_handler.handler(http_event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "artifact_prefix" in body["error"]


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

    event = _make_http_event({"artifact_prefix": "id"})
    response = results_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert body["artifact_prefix"] == "id"
    assert body["artifacts"][0]["url"] == "https://signed/url"
    assert body["artifacts"][0]["filename"] == "file1.nii.gz"


def test_status_handler_succeeded(monkeypatch):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stop = datetime(2024, 1, 1, 1, tzinfo=timezone.utc)
    output_payload = {"results": {"bucket": "bucket", "prefix": "prefix"}}

    class FakeBody:
        def __init__(self, data):
            self._data = data

        def read(self):
            return self._data

    class FakeSFN:
        def describe_execution(self, **kwargs):
            return {
                "status": "SUCCEEDED",
                "name": "artifact-1",
                "startDate": start,
                "stopDate": stop,
                "output": json.dumps(output_payload),
            }

    class FakeS3:
        def get_object(self, Bucket, Key):
            assert Bucket == "bucket"
            assert Key == "prefix/artifact-1/metadata.json"
            metadata = {"artifact_prefix": "artifact-1", "result_url": "https://results"}
            return {"Body": FakeBody(json.dumps(metadata).encode("utf-8"))}

    monkeypatch.setattr(status_handler, "_SFN", FakeSFN())
    monkeypatch.setattr(status_handler, "_S3", FakeS3())

    event = _make_http_event({"job_id": "arn:execution"})
    response = status_handler.handler(event, DummyContext())
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["status"] == "SUCCEEDED"
    assert body["artifact_prefix"] == "artifact-1"
    assert body["result"]["result_url"] == "https://results"


def test_status_handler_missing_job_id(monkeypatch):
    event = _make_http_event({})
    response = status_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "job_id" in body["error"]


def test_results_handler_missing_job_id(monkeypatch):
    monkeypatch.setenv("RESULTS_BUCKET", "bucket")
    event = _make_http_event({})
    response = results_handler.handler(event, DummyContext())
    body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert body["status"] == "FAILED"
    assert "artifact_prefix" in body["error"]
