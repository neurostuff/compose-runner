from click.testing import CliRunner

from compose_runner.cli import cli


def test_cli():
    runner = CliRunner()
    result = runner.invoke(cli, [
        "4nBwrGsqVWtt",
        '--environment', "staging",
        "--n-cores", 1,
        "--no-upload"])
    assert result.exit_code == 0
