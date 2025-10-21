from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class ComposeRunnerStack(Stack):
    """Provision Step Functions + ECS infrastructure for compose-runner workflows."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Context configuration with sensible defaults.
        results_bucket_name = self.node.try_get_context("resultsBucketName")
        results_prefix = self.node.try_get_context("resultsPrefix") or "compose-runner/results"
        submit_memory_size = int(self.node.try_get_context("submitMemorySize") or 512)
        submit_timeout_seconds = int(self.node.try_get_context("submitTimeoutSeconds") or 30)
        status_memory_size = int(self.node.try_get_context("statusMemorySize") or 512)
        status_timeout_seconds = int(self.node.try_get_context("statusTimeoutSeconds") or 30)
        poll_memory_size = int(self.node.try_get_context("pollMemorySize") or 512)
        poll_timeout_seconds = int(self.node.try_get_context("pollTimeoutSeconds") or 30)
        poll_lookback_ms = int(self.node.try_get_context("pollLookbackMs") or 3600000)

        task_cpu = int(self.node.try_get_context("taskCpu") or 4096)
        task_memory_mib = int(self.node.try_get_context("taskMemoryMiB") or 30720)
        task_ephemeral_storage_gib = int(self.node.try_get_context("taskEphemeralStorageGiB") or 21)
        task_cpu_large = int(self.node.try_get_context("taskCpuLarge") or 16384)
        task_memory_large_mib = int(self.node.try_get_context("taskMemoryLargeMiB") or 65536)
        state_machine_timeout_seconds = int(self.node.try_get_context("stateMachineTimeoutSeconds") or 32400)

        if task_cpu_large >= 16384 and task_memory_large_mib < 32768:
            raise ValueError("taskMemoryLargeMiB must be at least 32768 MiB for 16 vCPU tasks.")

        project_root = Path(__file__).resolve().parents[3]
        project_version = self.node.try_get_context("composeRunnerVersion")
        if not project_version:
            raise ValueError(
                "composeRunnerVersion context value is required. "
                "Pass it via `cdk deploy -c composeRunnerVersion=<version>`."
            )
        if submit_memory_size > 3008:
            raise ValueError(
                "submitMemorySize cannot exceed 3008 MB when using the Python 3.13 Lambda runtime. "
                "Pass a smaller value via `-c submitMemorySize=<mb>` or adjust the default."
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

        # Build Docker image for ECS Fargate task.
        fargate_asset = ecr_assets.DockerImageAsset(
            self,
            "ComposeRunnerFargateImage",
            directory=str(project_root),
            file="Dockerfile",
            build_args=build_args,
        )

        # Networking for ECS tasks (public subnets with internet access).
        vpc = ec2.Vpc(
            self,
            "ComposeRunnerVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        task_security_group = ec2.SecurityGroup(
            self,
            "ComposeRunnerTaskSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Allow outbound internet access for compose-runner tasks.",
        )

        cluster = ecs.Cluster(self, "ComposeRunnerCluster", vpc=vpc)

        task_log_group = logs.LogGroup(
            self,
            "ComposeRunnerTaskLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "ComposeRunnerTaskDefinition",
            cpu=task_cpu,
            memory_limit_mib=task_memory_mib,
            ephemeral_storage_gib=task_ephemeral_storage_gib,
        )

        task_definition_large = ecs.FargateTaskDefinition(
            self,
            "ComposeRunnerLargeTaskDefinition",
            cpu=task_cpu_large,
            memory_limit_mib=task_memory_large_mib,
            ephemeral_storage_gib=task_ephemeral_storage_gib,
        )

        container_environment = {
            "RESULTS_BUCKET": results_bucket.bucket_name,
            "RESULTS_PREFIX": results_prefix,
            "DELETE_TMP": "true",
        }

        container = task_definition.add_container(
            "ComposeRunnerContainer",
            image=ecs.ContainerImage.from_docker_image_asset(fargate_asset),
            entry_point=["python", "-m", "compose_runner.ecs_task"],
            logging=ecs.LogDriver.aws_logs(
                log_group=task_log_group,
                stream_prefix="compose-runner",
            ),
            environment=container_environment,
        )

        container_large = task_definition_large.add_container(
            "ComposeRunnerLargeContainer",
            image=ecs.ContainerImage.from_docker_image_asset(fargate_asset),
            entry_point=["python", "-m", "compose_runner.ecs_task"],
            logging=ecs.LogDriver.aws_logs(
                log_group=task_log_group,
                stream_prefix="compose-runner",
            ),
            environment=container_environment,
        )

        results_bucket.grant_read_write(task_definition.task_role)
        results_bucket.grant_read_write(task_definition_large.task_role)

        container_env_overrides = [
            tasks.TaskEnvironmentVariable(
                name="ARTIFACT_PREFIX", value=sfn.JsonPath.string_at("$.artifact_prefix")
            ),
            tasks.TaskEnvironmentVariable(
                name="META_ANALYSIS_ID", value=sfn.JsonPath.string_at("$.meta_analysis_id")
            ),
            tasks.TaskEnvironmentVariable(
                name="ENVIRONMENT", value=sfn.JsonPath.string_at("$.environment")
            ),
            tasks.TaskEnvironmentVariable(name="NSC_KEY", value=sfn.JsonPath.string_at("$.nsc_key")),
            tasks.TaskEnvironmentVariable(name="NV_KEY", value=sfn.JsonPath.string_at("$.nv_key")),
            tasks.TaskEnvironmentVariable(name="NO_UPLOAD", value=sfn.JsonPath.string_at("$.no_upload")),
            tasks.TaskEnvironmentVariable(name="N_CORES", value=sfn.JsonPath.string_at("$.n_cores")),
            tasks.TaskEnvironmentVariable(
                name="RESULTS_BUCKET", value=sfn.JsonPath.string_at("$.results.bucket")
            ),
            tasks.TaskEnvironmentVariable(
                name="RESULTS_PREFIX", value=sfn.JsonPath.string_at("$.results.prefix")
            ),
        ]

        run_task_standard = tasks.EcsRunTask(
            self,
            "RunFargateJob",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=task_definition,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            assign_public_ip=True,
            security_groups=[task_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=container,
                    environment=container_env_overrides,
                )
            ],
            result_path="$.ecs",
        )

        run_task_large = tasks.EcsRunTask(
            self,
            "RunFargateJobLarge",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=task_definition_large,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            assign_public_ip=True,
            security_groups=[task_security_group],
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=container_large,
                    environment=container_env_overrides,
                )
            ],
            result_path="$.ecs",
        )

        run_task_standard.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(30),
            backoff_rate=2.0,
            max_attempts=2,
        )

        run_task_large.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(30),
            backoff_rate=2.0,
            max_attempts=2,
        )

        run_output = sfn.Pass(
            self,
            "ComposeRunnerOutput",
            parameters={
                "artifact_prefix.$": "$.artifact_prefix",
                "meta_analysis_id.$": "$.meta_analysis_id",
                "environment.$": "$.environment",
                "results.$": "$.results",
                "task_size.$": "$.task_size",
                "ecs.$": "$.ecs",
            },
        )

        definition_chain = sfn.Choice(
            self,
            "SelectFargateTask",
        ).when(
            sfn.Condition.string_equals("$.task_size", "large"),
            run_task_large.next(run_output),
        ).otherwise(
            run_task_standard.next(run_output)
        )

        state_machine = sfn.StateMachine(
            self,
            "ComposeRunnerStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition_chain),
            timeout=Duration.seconds(state_machine_timeout_seconds),
        )

        # Lambda image shared across handlers.
        lambda_code = lambda_.DockerImageCode.from_image_asset(
            str(project_root),
            file="aws_lambda/Dockerfile",
            build_args=build_args,
        )

        submit_function = lambda_.DockerImageFunction(
            self,
            "ComposeRunnerSubmit",
            code=lambda_code,
            memory_size=submit_memory_size,
            timeout=Duration.seconds(submit_timeout_seconds),
            environment={
                "STATE_MACHINE_ARN": state_machine.state_machine_arn,
                "RESULTS_BUCKET": results_bucket.bucket_name,
                "RESULTS_PREFIX": results_prefix,
            },
            description="Starts compose-runner Step Functions executions.",
        )
        state_machine.grant_start_execution(submit_function)

        submit_function_url = submit_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )

        status_code = lambda_.DockerImageCode.from_image_asset(
            str(project_root),
            file="aws_lambda/Dockerfile",
            cmd=["compose_runner.aws_lambda.status_handler.handler"],
            build_args=build_args,
        )

        status_function = lambda_.DockerImageFunction(
            self,
            "ComposeRunnerStatus",
            code=status_code,
            memory_size=status_memory_size,
            timeout=Duration.seconds(status_timeout_seconds),
            environment={
                "RESULTS_BUCKET": results_bucket.bucket_name,
                "RESULTS_PREFIX": results_prefix,
            },
            description="Reports Step Functions execution status and metadata.",
        )
        state_machine.grant_read(status_function)
        results_bucket.grant_read(status_function)

        status_function_url = status_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )

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
                "RUNNER_LOG_GROUP": task_log_group.log_group_name,
                "DEFAULT_LOOKBACK_MS": str(poll_lookback_ms),
            },
            description="Retrieves compose-runner ECS logs for a job ID.",
        )

        task_log_group.grant_read(poll_function)
        poll_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:FilterLogEvents"],
                resources=[task_log_group.log_group_arn],
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

        # Stack outputs for convenience.
        cdk.CfnOutput(self, "ComposeRunnerSubmitFunctionName", value=submit_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerSubmitFunctionUrl", value=submit_function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerStatusFunctionName", value=status_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerStatusFunctionUrl", value=status_function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerLogPollerFunctionName", value=poll_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerLogPollerFunctionUrl", value=poll_function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerResultsFunctionName", value=results_function.function_name)
        cdk.CfnOutput(self, "ComposeRunnerResultsFunctionUrl", value=results_function_url.url)
        cdk.CfnOutput(self, "ComposeRunnerResultsBucketName", value=results_bucket.bucket_name)
        cdk.CfnOutput(self, "ComposeRunnerStateMachineArn", value=state_machine.state_machine_arn)
        cdk.CfnOutput(self, "ComposeRunnerTaskLogGroupName", value=task_log_group.log_group_name)
