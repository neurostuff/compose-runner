from click.testing import CliRunner

import compose_runner.cli as cli_module
from compose_runner.cli import cli


def test_cli(monkeypatch):
    calls = {}

    def fake_run(meta_analysis_id, environment, result_dir, nsc_key, nv_key, no_upload, n_cores):
        calls["args"] = {
            "meta_analysis_id": meta_analysis_id,
            "environment": environment,
            "result_dir": result_dir,
            "nsc_key": nsc_key,
            "nv_key": nv_key,
            "no_upload": no_upload,
            "n_cores": n_cores,
        }
        return "https://example.org/result", None

    monkeypatch.setattr(cli_module, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "3opENJpHxRsH",
            "--environment",
            "staging",
            "--n-cores",
            1,
            "--no-upload",
        ],
    )

    assert result.exit_code == 0
    assert calls["args"] == {
        "meta_analysis_id": "3opENJpHxRsH",
        "environment": "staging",
        "result_dir": None,
        "nsc_key": None,
        "nv_key": None,
        "no_upload": True,
        "n_cores": 1,
    }
    assert "https://example.org/result" in result.output
