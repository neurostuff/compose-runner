import click

from pynsc.run import Runner


@click.command()
@click.argument("meta-analysis-id", required=True)
@click.option("--result-dir", help="The directory to save results to.")
@click.option(
    "staging",
    "--staging",
    is_flag=True,
    help="DEVELOPER USE ONLY Use staging server instead of production server."
)
@click.option("nsc_key", "--nsc-key", help="Neurosynth Compose api key.")
@click.option("nv_key", "--nv-key", help="Neurovault api key.")
def run(meta_analysis_id, staging, result_dir, nsc_key, nv_key):
    """Execute and upload a meta-analysis workflow.

    META_ANALYSIS_ID is the id of the meta-analysis on neurosynth-compose.
    """
    runner = Runner(
        meta_analysis_id=meta_analysis_id,
        staging=staging,
        result_dir=result_dir,
        nsc_key=nsc_key,
        nv_key=nv_key,
    )

    runner.run_workflow()

    return runner.results_object
