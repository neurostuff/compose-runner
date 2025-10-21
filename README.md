# compose-runner

Python package to execute meta-analyses created using neurosynth compose and NiMARE
as the meta-analysis execution engine.

## AWS Deployment

This repository includes an AWS CDK application that turns compose-runner into a
serverless batch pipeline using Step Functions, AWS Lambda, and ECS Fargate.
The deployed architecture works like this:

- `ComposeRunnerSubmit` (Lambda Function URL) accepts HTTP requests, validates
  the meta-analysis payload, and starts a Step Functions execution. The response
  is immediate and returns both a durable `job_id` (the execution ARN) and the
  `artifact_prefix` used for S3 and log correlation.
- A Standard state machine runs a single Fargate task (`compose_runner.ecs_task`)
  and waits for completion. The container downloads inputs, executes the
  meta-analysis on up to 4 vCPU / 30 GiB of memory, uploads artifacts to S3, and
  writes `metadata.json` into the same prefix.
- `ComposeRunnerStatus` (Lambda Function URL) wraps `DescribeExecution`, merges
  metadata from S3, and exposes a simple status endpoint suitable for polling.
- `ComposeRunnerLogPoller` streams the ECS CloudWatch Logs for a given `artifact_prefix`,
  while `ComposeRunnerResultsFetcher` returns presigned URLs for stored artifacts.

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
3. Deploy the stack (supplying the compose-runner version you want baked into the images):
   ```bash
   cdk deploy \
     -c composeRunnerVersion=$(hatch version) \
     -c resultsPrefix=compose-runner/results \
     -c taskCpu=4096 \
     -c taskMemoryMiB=30720
   ```
   Pass `-c resultsBucketName=<bucket>` to use an existing S3 bucket, or omit it
   to let the stack create and retain a dedicated bucket. Additional knobs:

  - `-c stateMachineTimeoutSeconds=32400` to control the max wall clock per run
   - `-c submitTimeoutSeconds` / `-c statusTimeoutSeconds` / `-c pollTimeoutSeconds`
     to tune Lambda timeouts
   - `-c taskEphemeralStorageGiB` if the default 21 GiB scratch volume is insufficient

The deployment builds both the Lambda image (`aws_lambda/Dockerfile`) and the
Fargate task image (`Dockerfile`), provisions the Step Functions state machine,
and configures a public VPC so each task has outbound internet access.
The CloudFormation outputs list the HTTPS endpoints for submission, status,
logs, and artifact retrieval, alongside the Step Functions ARN.
