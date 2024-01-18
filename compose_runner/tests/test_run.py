import pytest
from requests.exceptions import HTTPError

from compose_runner.run import Runner


@pytest.mark.vcr(record_mode="once")
def test_incorrect_id():
    runner = Runner(
        meta_analysis_id="made_up_id",
        environment="staging",
    )

    with pytest.raises(HTTPError):
        runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_download_bundle():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.download_bundle()
    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None


@pytest.mark.vcr(record_mode="once")
def test_run_workflow():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.run_workflow(n_cores=2)


@pytest.mark.vcr(record_mode="once")
def test_run_database_workflow():
    runner = Runner(
        meta_analysis_id="dRFtnAo9bhp3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_run_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="4CGQSSyaoWN3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_run_string_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="7joU2Siajs5X",
        environment="staging",
    )
    runner.run_workflow()

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
