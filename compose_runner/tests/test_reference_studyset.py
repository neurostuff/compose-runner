"""Tests for the reference studyset a database meta-analysis compares against.

The neurostore reference is served by the studyset release API as a tarball of
parquet tables, rebuilt nightly; the other databases are static NIMADS dumps
committed to a GitHub repo. Both have to arrive as a studyset with the user's own
studies taken out of it.
"""

import gzip
import io
import json
import tarfile

import pytest
import requests
from nimare.nimads import Studyset

from compose_runner.run import Runner, _extract_studyset_release, gen_release_url

_TARGET = "mni152_2mm"


def _analysis(analysis_id, x):
    return {
        "id": analysis_id,
        "name": analysis_id,
        "conditions": [],
        "weights": [],
        "points": [
            {
                "id": f"p-{analysis_id}",
                "coordinates": [x, x, x],
                "space": "MNI",
                "values": [],
            }
        ],
        "metadata": {},
        "images": [],
    }


def _reference_dict():
    """A three-study reference database, one of whose studies is the user's."""
    return {
        "id": "reference",
        "name": "reference",
        "studies": [
            {
                "id": f"s{i}",
                "name": f"s{i}",
                "metadata": {},
                "analyses": [_analysis(f"a{i}", i)],
            }
            for i in (1, 2, 3)
        ],
    }


def _release_archive(tmp_path, source=None):
    """A studyset release tarball, laid out as the API serves one."""
    release = tmp_path / "neurostore-studyset-nightly"
    Studyset(source or _reference_dict(), target=None).to_parquet(release)

    archive = tmp_path / "release.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(release, arcname=release.name)
    return archive.read_bytes()


class _FakeResponse:
    """Enough of a ``requests`` response for either reference loader."""

    def __init__(self, content, status=200):
        self.content = content
        self._status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def raise_for_status(self):
        if self._status >= 400:
            raise requests.exceptions.HTTPError(f"{self._status}")

    def iter_content(self, chunk_size=1):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start : start + chunk_size]

    def close(self):
        self.closed = True


def _runner(environment="production"):
    return Runner(meta_analysis_id="unused", environment=environment)


def _serve(monkeypatch, response):
    """Answer every reference request with ``response``, recording the URL."""
    requested = []

    def fake_get(url, **kwargs):
        requested.append((url, kwargs))
        return response

    monkeypatch.setattr(requests, "get", fake_get)
    return requested


@pytest.mark.parametrize(
    "environment,expected",
    [
        (
            "production",
            "https://neurostore.org/api/neurostore-studyset-releases/nightly/download",
        ),
        (
            "staging",
            "https://staging.neurostore.xyz/api"
            "/neurostore-studyset-releases/nightly/download",
        ),
    ],
)
def test_neurostore_reference_comes_from_this_environments_release_api(
    environment, expected
):
    runner = _runner(environment)

    assert runner.reference_studysets["neurostore"] == expected


def test_the_other_references_are_still_github_dumps():
    runner = _runner("production")

    assert runner.reference_studysets["neurosynth"].endswith("/main/neurosynth.json.gz")
    assert runner.reference_studysets["neuroquery"].endswith("/main/neuroquery.json.gz")


def test_gen_release_url_does_not_double_the_separator():
    assert gen_release_url("https://neurostore.org/api/", "2026-08") == (
        "https://neurostore.org/api/neurostore-studyset-releases/2026-08/download"
    )


def test_release_reference_loads_and_drops_the_users_studies(tmp_path, monkeypatch):
    runner = _runner()
    requested = _serve(monkeypatch, _FakeResponse(_release_archive(tmp_path)))

    reference = runner._load_reference_studyset("neurostore", {"s2"})

    assert sorted(reference.study_ids) == ["s1", "s3"]
    assert sorted(reference.ids) == ["s1-a1", "s3-a3"]
    # Streamed rather than buffered whole, and read from the release API.
    assert requested[0][1]["stream"] is True
    assert requested[0][0] == runner.reference_studysets["neurostore"]


def test_release_reference_is_in_the_target_space(tmp_path, monkeypatch):
    runner = _runner()
    _serve(monkeypatch, _FakeResponse(_release_archive(tmp_path)))

    reference = runner._load_reference_studyset("neurostore", set())

    assert set(reference.coordinates["space"]) == {_TARGET}


def test_release_reference_survives_the_archive_being_cleaned_up(
    tmp_path, monkeypatch
):
    """The tables are read eagerly, so nothing is left pointing at the temp dir."""
    runner = _runner()
    _serve(monkeypatch, _FakeResponse(_release_archive(tmp_path)))

    reference = runner._load_reference_studyset("neurostore", set())

    assert len(reference.coordinates) == 3


def test_a_reference_the_api_does_not_serve_is_named_in_the_error(monkeypatch):
    runner = _runner()
    _serve(monkeypatch, _FakeResponse(b"", status=404))

    with pytest.raises(requests.exceptions.HTTPError, match="neurostore"):
        runner._load_reference_studyset("neurostore", set())


def test_an_archive_without_a_manifest_is_not_a_release(tmp_path):
    archive = tmp_path / "not-a-release.tar.gz"
    stray = tmp_path / "stray.txt"
    stray.write_text("nothing to load")
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(stray, arcname="stray.txt")

    with pytest.raises(ValueError, match="not a studyset parquet release"):
        _extract_studyset_release(archive, tmp_path / "out")


def test_dump_reference_still_reads_gzipped_nimads(monkeypatch):
    runner = _runner()
    payload = gzip.compress(json.dumps(_reference_dict()).encode("utf-8"))
    requested = _serve(monkeypatch, _FakeResponse(payload))

    reference = runner._load_reference_studyset("neurosynth", {"s1"})

    assert sorted(reference.study_ids) == ["s2", "s3"]
    assert "stream" not in requested[0][1]
