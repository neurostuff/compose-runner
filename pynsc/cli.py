import click

from pynsc.run import run


@click.command()
@click.argument("meta-analysis-id", required=True)
@click.option("--result-dir", help="The directory to save results to.")
@click.option(
    "environment",
    "--environment",
    type=click.Choice(["production", "staging", "local"], case_sensitive=False),
    default="production",
    help="DEVELOPER USE ONLY Use another server instead of production server.",
)
@click.option("nsc_key", "--nsc-key", help="Neurosynth Compose api key.")
@click.option("nv_key", "--nv-key", help="Neurovault api key.")
def cli(meta_analysis_id, staging, result_dir, nsc_key, nv_key):
    """Execute and upload a meta-analysis workflow.

    META_ANALYSIS_ID is the id of the meta-analysis on neurosynth-compose.
    """
    url, _ = run(meta_analysis_id, staging, result_dir, nsc_key, nv_key)
    print(url)
