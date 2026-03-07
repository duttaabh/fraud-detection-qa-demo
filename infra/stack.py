"""AWS CDK stack for the Fraud Detection & Quality Analysis application."""

import os
import subprocess

from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    Fn,
    BundlingOptions,
    ILocalBundling,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_glue as glue,
    aws_iam as iam,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    aws_cognito as cognito,
)
from constructs import Construct
import jsii


@jsii.implements(ILocalBundling)
class LocalPipBundling:
    """Local bundling: pip install + copy app code — no Docker needed."""

    def __init__(self, project_root: str):
        self._root = project_root

    def try_bundle(self, output_dir: str, *, image=None, **kwargs) -> bool:
        subprocess.check_call([
            "pip", "install",
            "-r", os.path.join(self._root, "api", "requirements.txt"),
            "-t", output_dir,
            "--platform", "manylinux2014_x86_64",
            "--implementation", "cp",
            "--python-version", "3.12",
            "--only-binary=:all:",
        ])
        # Copy application modules
        import shutil
        shutil.copytree(
            os.path.join(self._root, "api"), os.path.join(output_dir, "api"),
            dirs_exist_ok=True,
        )
        shutil.copy2(
            os.path.join(self._root, "config.py"), os.path.join(output_dir, "config.py"),
        )
        return True


class FraudDetectionStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- S3 Data Bucket ---
        self.data_bucket = s3.Bucket(
            self,
            "DataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        CfnOutput(self, "DataBucketName", value=self.data_bucket.bucket_name)

        # --- SKU Lookup Lambda ---
        self.sku_lambda = _lambda.Function(
            self,
            "SkuLookupFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambda/sku_lookup"),
            environment={"S3_BUCKET": self.data_bucket.bucket_name},
            timeout=Duration.seconds(30),
        )
        self.data_bucket.grant_read(self.sku_lambda, "raw/sku_catalog.csv")

        # --- SKU API Gateway (REST) ---
        self.sku_api = apigw.RestApi(
            self, "SkuApi", rest_api_name="SKU Microservice"
        )

        sku_resource = self.sku_api.root.add_resource("sku")
        sku_id_resource = sku_resource.add_resource("{sku_id}")
        sku_id_resource.add_method(
            "GET", apigw.LambdaIntegration(self.sku_lambda)
        )
        batch_resource = sku_resource.add_resource("batch")
        batch_resource.add_method(
            "POST", apigw.LambdaIntegration(self.sku_lambda)
        )

        CfnOutput(self, "SkuApiUrl", value=self.sku_api.url)

        # --- Glue IAM Role ---
        self.glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        self.data_bucket.grant_read_write(self.glue_role)

        # --- Deploy Glue scripts to S3 at cdk deploy time ---
        s3deploy.BucketDeployment(
            self,
            "GlueScriptsDeployment",
            sources=[s3deploy.Source.asset("glue")],
            destination_bucket=self.data_bucket,
            destination_key_prefix="glue-scripts",
        )

        # --- Glue Jobs ---
        glue_default_args = {
            "--S3_BUCKET": self.data_bucket.bucket_name,
            "--SKU_API_URL": self.sku_api.url,
        }

        self.ingestion_job = glue.CfnJob(
            self,
            "IngestionJob",
            name="fraud-detection-ingestion",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.data_bucket.bucket_name}/glue-scripts/ingestion_job.py",
                python_version="3",
            ),
            default_arguments=glue_default_args,
            glue_version="4.0",
            number_of_workers=20,
            worker_type="G.2X",
        )

        self.enrichment_job = glue.CfnJob(
            self,
            "EnrichmentJob",
            name="fraud-detection-enrichment",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.data_bucket.bucket_name}/glue-scripts/enrichment_job.py",
                python_version="3",
            ),
            default_arguments=glue_default_args,
            glue_version="4.0",
            number_of_workers=20,
            worker_type="G.2X",
        )

        self.feature_engineering_job = glue.CfnJob(
            self,
            "FeatureEngineeringJob",
            name="fraud-detection-feature-engineering",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.data_bucket.bucket_name}/glue-scripts/feature_engineering_job.py",
                python_version="3",
            ),
            default_arguments=glue_default_args,
            glue_version="4.0",
            number_of_workers=30,
            worker_type="G.2X",
        )

        # --- SageMaker IAM Role ---
        self.sagemaker_role = iam.Role(
            self,
            "SageMakerRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
        )
        self.data_bucket.grant_read_write(self.sagemaker_role)
        self.sagemaker_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:CreateTrainingJob",
                    "sagemaker:CreateTransformJob",
                    "sagemaker:CreateModel",
                    "sagemaker:DescribeTrainingJob",
                    "sagemaker:DescribeTransformJob",
                ],
                resources=["*"],
            )
        )
        self.sagemaker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecr:GetAuthorizationToken",
                         "ecr:BatchGetImage",
                         "ecr:GetDownloadUrlForLayer"],
                resources=["*"],
            )
        )
        self.sagemaker_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup",
                         "logs:CreateLogStream",
                         "logs:PutLogEvents"],
                resources=["*"],
            )
        )

        CfnOutput(self, "SageMakerRoleArn", value=self.sagemaker_role.role_arn)

        # --- FastAPI Backend Lambda (Mangum) ---
        # Bundle API code + pip deps locally (no Docker required).
        # --platform flags ensure Linux-compatible wheels even on macOS.
        api_bundle_dir = os.path.join(os.path.dirname(__file__), os.pardir)
        self.api_lambda = _lambda.Function(
            self,
            "ApiFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="api.main.handler",
            code=_lambda.Code.from_asset(
                api_bundle_dir,
                exclude=[
                    "dashboard",
                    "infra",
                    ".venv",
                    "tests",
                    "node_modules",
                    ".git",
                    ".hypothesis",
                    "__pycache__",
                    "*.pyc",
                    "cdk.out",
                    ".kiro",
                    ".pytest_cache",
                    ".vscode",
                    "glue",
                    "lambda",
                    "pipeline",
                    "scripts",
                ],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=["bash", "-c", "echo 'Docker fallback — should not reach here'"],
                    local=LocalPipBundling(api_bundle_dir),
                ),
            ),
            environment={
                "S3_BUCKET": self.data_bucket.bucket_name,
                "RESULTS_PREFIX": "results/",
            },
            timeout=Duration.seconds(30),
            memory_size=1024,
        )
        self.data_bucket.grant_read(self.api_lambda, "results/*")

        # Allow API Lambda to call Bedrock for fraud reasoning
        self.api_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=["arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-pro-v1:0"],
            )
        )

        # --- HTTP API Gateway for FastAPI ---
        self.http_api = apigwv2.HttpApi(
            self,
            "HttpApi",
            api_name="Fraud Detection API",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.ANY],
                allow_headers=["*"],
            ),
        )
        self.http_api.add_routes(
            path="/{proxy+}",
            methods=[apigwv2.HttpMethod.ANY],
            integration=apigwv2_integrations.HttpLambdaIntegration(
                "ApiIntegration", self.api_lambda
            ),
        )

        CfnOutput(self, "HttpApiUrl", value=self.http_api.url or "")

        # --- Dashboard S3 Bucket + CloudFront ---
        self.dashboard_bucket = s3.Bucket(
            self,
            "DashboardBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # API Gateway origin for /api/* requests
        api_domain = Fn.select(2, Fn.split("/", self.http_api.url or ""))
        api_origin = origins.HttpOrigin(
            api_domain,
            protocol_policy=cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
        )

        self.distribution = cloudfront.Distribution(
            self,
            "DashboardDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(
                    self.dashboard_bucket
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            additional_behaviors={
                "/api/*": cloudfront.BehaviorOptions(
                    origin=api_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                ),
            },
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_page_path="/index.html",
                    response_http_status=200,
                ),
            ],
        )

        # --- Cognito User Pool for Dashboard Auth ---
        self.user_pool = cognito.UserPool(
            self,
            "DashboardUserPool",
            user_pool_name="fraud-detection-dashboard-users",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Hosted UI domain (Cognito-provided prefix)
        self.user_pool_domain = self.user_pool.add_domain(
            "CognitoDomain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix="fraud-detection-dashboard",
            ),
        )

        # App client for the SPA (PKCE flow, no client secret)
        dashboard_url = f"https://{self.distribution.distribution_domain_name}"
        self.user_pool_client = self.user_pool.add_client(
            "DashboardAppClient",
            user_pool_client_name="dashboard-spa",
            generate_secret=False,
            auth_flows=cognito.AuthFlow(user_srp=True),
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(authorization_code_grant=True),
                scopes=[cognito.OAuthScope.OPENID, cognito.OAuthScope.EMAIL, cognito.OAuthScope.PROFILE],
                callback_urls=[dashboard_url, "http://localhost:5173"],
                logout_urls=[dashboard_url, "http://localhost:5173"],
            ),
            supported_identity_providers=[
                cognito.UserPoolClientIdentityProvider.COGNITO,
            ],
        )

        # Create demo user (email: demo@example.com, auto-generated password)
        import hashlib
        import time
        auto_password = hashlib.sha256(f"FraudDemo-{self.account}-{self.region}".encode()).hexdigest()[:12] + "A1!"
        self.demo_user = cognito.CfnUserPoolUser(
            self,
            "DemoUser",
            user_pool_id=self.user_pool.user_pool_id,
            username="demo@example.com",
            desired_delivery_mediums=["EMAIL"],
            force_alias_creation=False,
            user_attributes=[
                cognito.CfnUserPoolUser.AttributeTypeProperty(
                    name="email", value="demo@example.com"
                ),
                cognito.CfnUserPoolUser.AttributeTypeProperty(
                    name="email_verified", value="true"
                ),
            ],
        )

        CfnOutput(self, "CognitoUserPoolId", value=self.user_pool.user_pool_id)
        CfnOutput(self, "CognitoAppClientId", value=self.user_pool_client.user_pool_client_id)
        CfnOutput(
            self,
            "CognitoDomain",
            value=self.user_pool_domain.base_url(),
        )
        CfnOutput(
            self,
            "DemoUserNote",
            value="Demo user: demo@example.com — check email for temporary password, or set via: aws cognito-idp admin-set-user-password --user-pool-id <pool-id> --username demo@example.com --password <password> --permanent",
        )

        s3deploy.BucketDeployment(
            self,
            "DashboardDeployment",
            sources=[s3deploy.Source.asset("dashboard/dist")],
            destination_bucket=self.dashboard_bucket,
            distribution=self.distribution,
        )

        CfnOutput(
            self,
            "DashboardUrl",
            value=f"https://{self.distribution.distribution_domain_name}",
        )
