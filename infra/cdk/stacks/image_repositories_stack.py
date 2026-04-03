from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import RemovalPolicy, Stack, aws_ecr as ecr, aws_iam as iam
from constructs import Construct


class ComposeRunnerImageRepositoriesStack(Stack):
    """Provision ECR repositories used by compose-runner runtime images."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lifecycle_rules = [
            ecr.LifecycleRule(
                description="Keep only the latest 2 tagged images.",
                max_image_count=2,
                tag_status=ecr.TagStatus.TAGGED,
                tag_pattern_list=["*"],
            )
        ]

        self.ecs_image_repository = ecr.Repository(
            self,
            "ComposeRunnerEcsImageRepository",
            repository_name="compose-runner-ecs",
            image_scan_on_push=True,
            lifecycle_rules=lifecycle_rules,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.lambda_image_repository = ecr.Repository(
            self,
            "ComposeRunnerLambdaImageRepository",
            repository_name="compose-runner-lambda",
            image_scan_on_push=True,
            lifecycle_rules=lifecycle_rules,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.lambda_image_repository.add_to_resource_policy(
            iam.PolicyStatement(
                sid="LambdaECRImageRetrievalPolicy",
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=[
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer",
                ],
            )
        )

        cdk.CfnOutput(
            self,
            "ComposeRunnerEcsImageRepositoryName",
            value=self.ecs_image_repository.repository_name,
        )
        cdk.CfnOutput(
            self,
            "ComposeRunnerLambdaImageRepositoryName",
            value=self.lambda_image_repository.repository_name,
        )
