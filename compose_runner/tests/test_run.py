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
