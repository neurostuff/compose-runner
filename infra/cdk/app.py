#!/usr/bin/env python3

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.compose_runner_stack import ComposeRunnerStack


def main() -> None:
    app = cdk.App()

    ComposeRunnerStack(
        app,
        "ComposeRunnerStack",
        env=cdk.Environment(
            account=os.getenv("CDK_DEFAULT_ACCOUNT"),
            region=os.getenv("CDK_DEFAULT_REGION"),
        ),
    )

    app.synth()


if __name__ == "__main__":
    main()
