"""SageMaker Batch Transform Module for scoring claims with trained RCF model."""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)

RCF_IMAGE_URI = "382416733822.dkr.ecr.us-east-1.amazonaws.com/randomcutforest:latest"


@dataclass(frozen=True)
class SageMakerBatchTransformConfig:
    """Configuration for SageMaker batch transform jobs."""

    s3_bucket: str
    features_prefix: str = "features/"
    sagemaker_output_prefix: str = "sagemaker/output/"
    model_artifact_path: str = ""
    instance_type: str = "ml.m5.xlarge"
    instance_count: int = 2


class SageMakerBatchTransform:
    """Orchestrates SageMaker batch transform for RCF anomaly scoring."""

    def __init__(
        self,
        config: SageMakerBatchTransformConfig,
        sagemaker_client=None,
        max_poll_attempts: int = 480,
        poll_interval: int = 30,
    ):
        self.config = config
        self._sagemaker = sagemaker_client or boto3.client("sagemaker")
        self.max_poll_attempts = max_poll_attempts
        self.poll_interval = poll_interval
        self._timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    def create_model(self, model_artifact_path: str) -> str:
        """Create a SageMaker model from a trained RCF artifact.

        Args:
            model_artifact_path: S3 URI of the model.tar.gz artifact.

        Returns:
            The model name.

        Raises:
            RuntimeError: If the SageMaker API call fails.
        """
        model_name = f"rcf-model-{self._timestamp}"
        try:
            self._sagemaker.create_model(
                ModelName=model_name,
                PrimaryContainer={
                    "Image": RCF_IMAGE_URI,
                    "ModelDataUrl": model_artifact_path,
                },
                ExecutionRoleArn=self._resolve_role_arn(),
            )
            logger.info("Created SageMaker model: %s", model_name)
            return model_name
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create SageMaker model: {exc}"
            ) from exc

    def _resolve_role_arn(self) -> str:
        """Resolve the SageMaker execution role ARN from environment or config."""
        import os
        return os.environ.get("SAGEMAKER_ROLE_ARN", "")

    def create_transform_job(self, model_name: str) -> str:
        """Create a SageMaker batch transform job.

        Args:
            model_name: Name of the SageMaker model to use.

        Returns:
            The transform job name.

        Raises:
            RuntimeError: If the SageMaker API call fails.
        """
        job_name = f"rcf-transform-{self._timestamp}"
        input_uri = f"s3://{self.config.s3_bucket}/{self.config.features_prefix}training/train.csv"
        output_uri = f"s3://{self.config.s3_bucket}/{self.config.sagemaker_output_prefix}"

        try:
            self._sagemaker.create_transform_job(
                TransformJobName=job_name,
                ModelName=model_name,
                TransformInput={
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": input_uri,
                        }
                    },
                    "ContentType": "text/csv",
                    "SplitType": "Line",
                },
                TransformOutput={
                    "S3OutputPath": output_uri,
                    "AssembleWith": "Line",
                },
                TransformResources={
                    "InstanceType": self.config.instance_type,
                    "InstanceCount": self.config.instance_count,
                },
            )
            logger.info("Created transform job: %s", job_name)
            return job_name
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create SageMaker transform job: {exc}"
            ) from exc

    def wait_for_transform(self, job_name: str) -> str:
        """Poll the transform job until it completes, fails, or times out.

        Args:
            job_name: The SageMaker transform job name.

        Returns:
            The S3 output path of the transform results.

        Raises:
            RuntimeError: If the job fails, is stopped, or polling times out.
        """
        for attempt in range(1, self.max_poll_attempts + 1):
            response = self._sagemaker.describe_transform_job(
                TransformJobName=job_name
            )
            status = response["TransformJobStatus"]
            logger.info(
                "Transform job %s status: %s (poll %d/%d)",
                job_name, status, attempt, self.max_poll_attempts,
            )

            if status == "Completed":
                output_path = response["TransformOutput"]["S3OutputPath"]
                logger.info(
                    "Transform job %s completed. Output: %s", job_name, output_path
                )
                return output_path

            if status in ("Failed", "Stopped"):
                failure_reason = response.get(
                    "FailureReason", "No failure details available"
                )
                raise RuntimeError(
                    f"SageMaker transform job {job_name} {status.lower()}: "
                    f"{failure_reason}"
                )

            time.sleep(self.poll_interval)

        raise RuntimeError(
            f"SageMaker transform job {job_name} timed out after "
            f"{self.max_poll_attempts * self.poll_interval}s"
        )

    def run(self, model_artifact_path: str) -> dict[str, str]:
        """Execute the full batch transform flow.

        Args:
            model_artifact_path: S3 URI of the trained model artifact.

        Returns:
            Dict with 'output_path' and 'job_name' keys.
        """
        model_name = self.create_model(model_artifact_path)
        job_name = self.create_transform_job(model_name)
        output_path = self.wait_for_transform(job_name)

        logger.info("Batch transform complete for job %s", job_name)
        return {
            "output_path": output_path,
            "job_name": job_name,
        }
