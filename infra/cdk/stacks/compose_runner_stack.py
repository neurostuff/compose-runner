from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct


class ComposeRunnerStack(Stack):
    """Infrastructure for running compose-runner via AWS Lambda."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Context configuration with sensible defaults.
        results_bucket_name = self.node.try_get_context("resultsBucketName")
        results_prefix = self.node.try_get_context("resultsPrefix") or "compose-runner/results"
        run_memory_size = int(self.node.try_get_context("runMemorySize") or 3008)
        run_timeout_seconds = int(self.node.try_get_context("runTimeoutSeconds") or 900)
        run_ephemeral_size = int(self.node.try_get_context("runEphemeralSize") or 10240)
        poll_memory_size = int(self.node.try_get_context("pollMemorySize") or 512)
        poll_timeout_seconds = int(self.node.try_get_context("pollTimeoutSeconds") or 30)
        poll_lookback_ms = int(self.node.try_get_context("pollLookbackMs") or 3600000)

        project_root = Path(__file__).resolve().parents[3]
        project_version = self.node.try_get_context("composeRunnerVersion")
        if not project_version:
            raise ValueError(
                "composeRunnerVersion context value is required. "
                "Pass it via `cdk deploy -c composeRunnerVersion=<version>`."
            )
        if run_memory_size > 3008:
            raise ValueError(
                "runMemorySize cannot exceed 3008 MB when using the Python 3.13 Lambda runtime. "
                "Pass a smaller value via `-c runMemorySize=<mb>` or adjust the default."
            )
        build_args = {"COMPOSE_RUNNER_VERSION": project_version}

        # Bucket for storing workflow artifacts.
        if results_bucket_name:
            results_bucket = s3.Bucket.from_bucket_name(
                self, "ComposeRunnerResultsBucket", results_bucket_name
            )
        else:
            results_bucket = s3.Bucket(
                self,
                "ComposeRunnerResults",
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
                versioned=True,
                removal_policy=RemovalPolicy.RETAIN,
            )

        run_code = lambda_.DockerImageCode.from_image_asset(
            str(project_root),
            file="aws_lambda/Dockerfile",
            build_args=build_args,
        )

        run_function = lambda_.DockerImageFunction(
            self,
            "ComposeRunnerFunction",
            code=run_code,
            memory_size=run_memory_size,
            timeout=Duration.seconds(run_timeout_seconds),
            ephemeral_storage_size=cdk.Size.mebibytes(run_ephemeral_size),
            environment={
                "RESULTS_BUCKET": results_bucket.bucket_name,
                "RESULTS_PREFIX": results_prefix,
            },
            description="Executes compose-runner meta-analyses and stores outputs in S3.",
        )

        results_bucket.grant_read_write(run_function)

        function_url = run_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )

        # Retain logs for observability and provide reference to polling lambda.
        log_group = run_function.log_group
        cfn_log_group = log_group.node.default_child
        if isinstance(cfn_log_group, logs.CfnLogGroup):
            cfn_log_group.apply_removal_policy(RemovalPolicy.RETAIN)

        poll_code = lambda_.DockerImageCode.from_image_asset(
            str(project_root),
            file="aws_lambda/Dockerfile",
            cmd=["compose_runner.aws_lambda.log_poll_handler.handler"],
            build_args=build_args,
        )

        poll_function = lambda_.DockerImageFunction(
            self,
            "ComposeRunnerLogPoller",
            code=poll_code,
            memory_size=poll_memory_size,
            timeout=Duration.seconds(poll_timeout_seconds),
            environment={
                "RUNNER_LOG_GROUP": log_group.log_group_name,
                "DEFAULT_LOOKBACK_MS": str(poll_lookback_ms),
            },
            description="Retrieves compose-runner Lambda logs for a job ID.",
        )

        log_group.grant_read(poll_function)

        # Allow polling lambda to filter events.
        poll_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:FilterLogEvents"],
                resources=[log_group.log_group_arn],
            )
        )

        poll_function_url = poll_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )

        results_code = lambda_.DockerImageCode.from_image_asset(
            str(project_root),
            file="aws_lambda/Dockerfile",
            cmd=["compose_runner.aws_lambda.results_handler.handler"],
            build_args=build_args,
        )

        results_function = lambda_.DockerImageFunction(
            self,
            "ComposeRunnerResultsFetcher",
            code=results_code,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment={
                "RESULTS_BUCKET": results_bucket.bucket_name,
                "RESULTS_PREFIX": results_prefix,
            },
            description="Provides presigned URLs for compose-runner artifacts in S3.",
        )

        results_bucket.grant_read(results_function)

        results_function_url = results_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )

        cdk.CfnOutput(self, "ComposeRunnerFunctionName", value=run_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerLogPollerFunctionName", value=poll_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerResultsBucketName", value=results_bucket.bucket_name)
        cdk.CfnOutput(self, "ComposeRunnerFunctionUrl", value=function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerLogPollerFunctionUrl", value=poll_function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerResultsFunctionName", value=results_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerResultsFunctionUrl", value=results_function_url.url)
