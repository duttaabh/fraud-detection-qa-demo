#!/usr/bin/env python3
"""CDK app entry point for the Fraud Detection & Quality Analysis stack."""

import aws_cdk as cdk

from infra.stack import FraudDetectionStack

app = cdk.App()
FraudDetectionStack(app, "FraudDetectionStack")
app.synth()
