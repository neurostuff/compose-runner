# compose-runner

Python package to execute meta-analyses created using neurosynth compose and NiMARE
as the meta-analysis execution engine.

## AWS Lambda Deployment

This repository includes an AWS CDK application for provisioning the Lambda-based
execution environment and log polling function.

1. Create a virtual environment and install the CDK dependencies:
   ```bash
   cd infra/cdk
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. (One-time per account/region) bootstrap the CDK environment:
   ```bash
   cdk bootstrap
   ```
3. Deploy the stack (supplying the compose-runner version you want baked into the Lambda image):
   ```bash
   cdk deploy \
     -c composeRunnerVersion=$(hatch version) \
     -c resultsPrefix=compose-runner/results \
     -c runMemorySize=3008 \
     -c runTimeoutSeconds=900
   ```
   The deployment output includes HTTPS endpoints for submitting runs (`ComposeRunnerFunctionUrl`), polling logs (`ComposeRunnerLogPollerFunctionUrl`), and fetching presigned S3 URLs (`ComposeRunnerResultsFunctionUrl`).
   Omit `resultsBucketName` to let the stack create a managed bucket, or pass an
   existing bucket name via `-c resultsBucketName=<bucket>`.

The deployment builds the Lambda container image from `aws_lambda/Dockerfile`,
creates two functions (`ComposeRunnerFunction` and `ComposeRunnerLogPoller`),
and provisions the S3 bucket used to store generated artifacts (including
`meta_results.pkl`). The log poller function expects clients to call it with a
job ID (the run Lambda invocation request ID) and returns filtered CloudWatch Logs
entries for that job.
