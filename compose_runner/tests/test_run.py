from datetime import date, datetime, timezone
from uuid import UUID

import pytest
from neurosynth_compose_sdk.exceptions import ApiException as ComposeApiException

from compose_runner.run import Runner


@pytest.mark.vcr
def test_incorrect_id():
    runner = Runner(
        meta_analysis_id="made_up_id",
        environment="production",
    )

    with pytest.raises(ComposeApiException):
        runner.run_workflow()


@pytest.mark.vcr
def test_download_bundle():
    runner = Runner(
        meta_analysis_id="ataCTPAt2LMw",
        environment="production",
    )
    runner.download_bundle()
    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None


def test_snapshot_md5_serializes_sdk_scalars_like_api_strings():
    created_at = datetime(2023, 6, 19, 15, 29, 59, 132810, tzinfo=timezone.utc)
    live_payload = {
        "created_at": created_at,
        "id": UUID("00000000-0000-0000-0000-000000000001"),
        "studies": [{"created_at": created_at, "publication_date": date(2023, 6, 19)}],
    }
    api_payload = {
        "created_at": "2023-06-19T15:29:59.132810+00:00",
        "id": "00000000-0000-0000-0000-000000000001",
        "studies": [
            {
                "created_at": "2023-06-19T15:29:59.132810+00:00",
                "publication_date": "2023-06-19",
            }
        ],
    }

    assert Runner._snapshot_md5(live_payload) == Runner._snapshot_md5(api_payload)


def test_json_safe_payload_normalizes_datetimes_without_mutating_source():
    created_at = datetime(2023, 6, 19, 15, 29, 59, 132810, tzinfo=timezone.utc)
    payload = {"created_at": created_at, "tags": {"b", "a"}}

    normalized = Runner._json_safe_payload(payload)

    assert normalized == {
        "created_at": "2023-06-19T15:29:59.132810+00:00",
        "tags": ["a", "b"],
    }
    assert payload["created_at"] is created_at


def test_create_result_object_normalizes_uploaded_snapshots():
    created_at = datetime(2023, 6, 19, 15, 29, 59, 132810, tzinfo=timezone.utc)
    captured = {}

    class FakeComposeApi:
        def meta_analysis_results_post(self, result_init):
            captured["result_init"] = result_init
            return type("Result", (), {"id": "result-id"})()

    runner = Runner(meta_analysis_id="meta-id", environment="production")
    runner.compose_api = FakeComposeApi()
    runner.cached_studyset = {
        "id": UUID("00000000-0000-0000-0000-000000000001"),
        "created_at": created_at,
        "studies": [],
    }
    runner.cached_annotation = {
        "created_at": created_at,
        "notes": [],
    }

    runner.create_result_object()

    result_init = captured["result_init"]
    assert result_init.snapshot_studyset == {
        "created_at": "2023-06-19T15:29:59.132810+00:00",
        "id": "00000000-0000-0000-0000-000000000001",
        "studies": [],
    }
    assert result_init.snapshot_annotation == {
        "created_at": "2023-06-19T15:29:59.132810+00:00",
        "notes": [],
    }
    assert runner.result_id == "result-id"


@pytest.mark.vcr
def test_run_workflow():
    runner = Runner(
        meta_analysis_id="ataCTPAt2LMw",
        environment="production",
    )
    runner.run_workflow(n_cores=2, no_upload=True)


@pytest.mark.vcr
def test_run_database_workflow():
    runner = Runner(
        meta_analysis_id="SZDBPeTwArZY",
        environment="production",
    )
    runner.run_workflow(no_upload=True)


@pytest.mark.vcr
def test_run_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="7NUkZJ28QDpY",
        environment="production",
    )
    runner.run_workflow(no_upload=True)


@pytest.mark.vcr
def test_run_string_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="7NUkZJ28QDpY",
        environment="production",
    )
    runner.run_workflow(no_upload=True)


# def test_yifan_workflow():
#     runner = Runner(
#         meta_analysis_id="4WELjap2yCJm",
#     )
#     runner.run_workflow()


# @pytest.mark.vcr(record_mode="once")
# def test_mkdachis_comparison_workflow():
#     runner = Runner(
#         meta_analysis_id="6Grzwzs3t7YB",
#     )
#     runner.run_workflow()
