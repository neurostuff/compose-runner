from __future__ import annotations

import json
import unittest

import aws_cdk as cdk
from aws_cdk.assertions import Template

from stacks import ComposeRunnerImageRepositoriesStack, ComposeRunnerStack


class ComposeRunnerStackImageConfigTest(unittest.TestCase):
    def test_stack_uses_explicit_ecr_repositories(self) -> None:
        app = cdk.App(context={"composeRunnerVersion": "0.7.8"})

        image_repositories_stack = ComposeRunnerImageRepositoriesStack(
            app,
            "ComposeRunnerImageRepositoriesStack",
            env=cdk.Environment(account="631329474511", region="us-east-1"),
        )

        stack = ComposeRunnerStack(
            app,
            "ComposeRunnerStack",
            ecs_image_repository=image_repositories_stack.ecs_image_repository,
            lambda_image_repository=image_repositories_stack.lambda_image_repository,
            env=cdk.Environment(account="631329474511", region="us-east-1"),
        )

        repositories_template_json = json.dumps(Template.from_stack(image_repositories_stack).to_json())
        app_template_json = json.dumps(Template.from_stack(stack).to_json())

        self.assertIn("compose-runner-ecs", repositories_template_json)
        self.assertIn("compose-runner-lambda", repositories_template_json)
        self.assertIn("Keep only the latest 2 tagged images.", repositories_template_json)
        self.assertIn("LambdaECRImageRetrievalPolicy", repositories_template_json)

        self.assertIn("0.7.8", app_template_json)
        self.assertIn("ComposeRunnerImageRepositoriesStack:ExportsOutputRef", app_template_json)
        self.assertIn("TransitionResultsToIntelligentTiering", app_template_json)
        self.assertIn("INTELLIGENT_TIERING", app_template_json)
        self.assertNotIn("cdk-hnb659fds-container-assets", app_template_json)
        self.assertNotIn("AssetParameters", app_template_json)


if __name__ == "__main__":
    unittest.main()
