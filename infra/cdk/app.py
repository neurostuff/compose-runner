#!/usr/bin/env python3

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks import ComposeRunnerImageRepositoriesStack, ComposeRunnerStack


def main() -> None:
    app = cdk.App()
    env = cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    )

    image_repositories_stack = ComposeRunnerImageRepositoriesStack(
        app,
        "ComposeRunnerImageRepositoriesStack",
        env=env,
    )

    if app.node.try_get_context("composeRunnerVersion"):
        ComposeRunnerStack(
            app,
            "ComposeRunnerStack",
            ecs_image_repository=image_repositories_stack.ecs_image_repository,
            lambda_image_repository=image_repositories_stack.lambda_image_repository,
            env=env,
        )

    app.synth()


if __name__ == "__main__":
    main()
