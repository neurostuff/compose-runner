
import pytest
from pynsc.run import Runner


@pytest.mark.vcr(record_mode="once")
def test_download_bundle():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        staging=True,
    )
    runner.download_bundle()
    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None


@pytest.mark.vcr(record_mode="once")
def test_run_workflow():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        staging=True,
    )
    runner.run_workflow()
