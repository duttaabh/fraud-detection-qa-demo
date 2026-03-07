"""SageMaker Training Module for Random Cut Forest anomaly detection."""

import io
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# RCF algorithm container image URI for us-east-1
RCF_IMAGE_URI = "382416733822.dkr.ecr.us-east-1.amazonaws.com/randomcutforest:latest"


@dataclass(frozen=True)
class SageMakerTrainingConfig:
    """Configuration for SageMaker RCF training jobs."""

    s3_bucket: str
    features_prefix: str = "features/"
    models_prefix: str = "models/"
    sagemaker_role_arn: str = ""
    instance_type: str = "ml.m5.xlarge"
    instance_count: int = 1
    num_trees: int = 100
    num_samples_per_tree: int = 256


class SageMakerTrainingJob:
    """Orchestrates SageMaker Random Cut Forest training."""

    def __init__(
        self,
        config: SageMakerTrainingConfig,
        sagemaker_client=None,
        s3_client=None,
        max_poll_attempts: int = 480,
        poll_interval: int = 30,
    ):
        self.config = config
        self._sagemaker = sagemaker_client or boto3.client("sagemaker")
        self._s3 = s3_client or boto3.client("s3")
        self.max_poll_attempts = max_poll_attempts
        self.poll_interval = poll_interval
        self._timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    def get_feature_dim(self) -> int:
        """Read feature metadata from S3 to determine the feature dimension.

        Returns:
            The number of feature columns (excluding claim_id).

        Raises:
            RuntimeError: If the metadata file cannot be read.
        """
        metadata_key = f"{self.config.features_prefix}metadata/feature_metadata.json"
        try:
            response = self._s3.get_object(
                Bucket=self.config.s3_bucket, Key=metadata_key
            )
            metadata = json.loads(response["Body"].read().decode("utf-8"))
            feature_dim = metadata["feature_dim"]
            logger.info("Feature dimension from metadata: %d", feature_dim)
            return feature_dim
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read feature metadata from "
                f"s3://{self.config.s3_bucket}/{metadata_key}: {exc}"
            ) from exc

    def _prepare_training_csv(self) -> str:
        """Convert feature Parquet to CSV for SageMaker RCF training.

        RCF expects CSV without headers and without the claim_id column.
        Reads the Parquet in chunks via pyarrow to keep memory bounded,
        streams CSV parts to S3 via multipart upload.

        Returns:
            The S3 URI of the training CSV data prefix.
        """
        import pyarrow.parquet as pq

        parquet_key = f"{self.config.features_prefix}claim_features.parquet"
        training_prefix = f"{self.config.features_prefix}training/"
        training_csv_key = f"{training_prefix}train.csv"

        # List all parquet part files (Spark writes a directory of parts)
        parquet_keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=parquet_key):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".parquet"):
                    parquet_keys.append(k)

        # If no part files found, the key itself might be a single file
        if not parquet_keys:
            parquet_keys = [parquet_key]

        # Start multipart upload
        mpu = self._s3.create_multipart_upload(
            Bucket=self.config.s3_bucket, Key=training_csv_key
        )
        upload_id = mpu["UploadId"]
        parts: list[dict] = []
        part_number = 1
        buffer = io.BytesIO()
        min_part_size = 5 * 1024 * 1024  # 5 MB
        total_records = 0

        try:
            for pk in parquet_keys:
                resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=pk)
                pf = pq.ParquetFile(io.BytesIO(resp["Body"].read()))

                for batch in pf.iter_batches(batch_size=100_000):
                    df = batch.to_pandas()
                    if "claim_id" in df.columns:
                        df = df.drop(columns=["claim_id"])
                    csv_bytes = df.to_csv(index=False, header=False).encode("utf-8")
                    buffer.write(csv_bytes)
                    total_records += len(df)

                    if buffer.tell() >= min_part_size:
                        resp_part = self._s3.upload_part(
                            Bucket=self.config.s3_bucket,
                            Key=training_csv_key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=buffer.getvalue(),
                        )
                        parts.append({"ETag": resp_part["ETag"], "PartNumber": part_number})
                        part_number += 1
                        buffer = io.BytesIO()

            # Flush remaining
            if buffer.tell() > 0:
                if not parts:
                    # Single part — abort multipart upload and use simple put
                    self._s3.abort_multipart_upload(
                        Bucket=self.config.s3_bucket,
                        Key=training_csv_key,
                        UploadId=upload_id,
                    )
                    self._s3.put_object(
                        Bucket=self.config.s3_bucket,
                        Key=training_csv_key,
                        Body=buffer.getvalue(),
                    )
                    self._record_count = total_records
                    s3_uri = f"s3://{self.config.s3_bucket}/{training_prefix}"
                    logger.info("Prepared training CSV: %d records at %s", total_records, s3_uri)
                    return s3_uri
                else:
                    resp_part = self._s3.upload_part(
                        Bucket=self.config.s3_bucket,
                        Key=training_csv_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=buffer.getvalue(),
                    )
                    parts.append({"ETag": resp_part["ETag"], "PartNumber": part_number})

            self._s3.complete_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=training_csv_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            self._s3.abort_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=training_csv_key,
                UploadId=upload_id,
            )
            raise

        self._record_count = total_records
        s3_uri = f"s3://{self.config.s3_bucket}/{training_prefix}"
        logger.info("Prepared training CSV: %d records at %s", total_records, s3_uri)
        return s3_uri

    def create_training_job(self) -> str:
        """Create a SageMaker training job using the RCF algorithm.

        Reads feature data, converts to CSV, and submits the training job.

        Returns:
            The training job name.

        Raises:
            RuntimeError: If the SageMaker API call fails.
        """
        feature_dim = self.get_feature_dim()
        training_data_uri = self._prepare_training_csv()

        job_name = f"anomaly-detection-{self._timestamp}"
        output_path = f"s3://{self.config.s3_bucket}/{self.config.models_prefix}{self._timestamp}"

        hyperparameters = {
            "num_trees": str(self.config.num_trees),
            "num_samples_per_tree": str(self.config.num_samples_per_tree),
            "feature_dim": str(feature_dim),
        }

        try:
            self._sagemaker.create_training_job(
                TrainingJobName=job_name,
                AlgorithmSpecification={
                    "TrainingImage": RCF_IMAGE_URI,
                    "TrainingInputMode": "File",
                },
                RoleArn=self.config.sagemaker_role_arn,
                HyperParameters=hyperparameters,
                InputDataConfig=[
                    {
                        "ChannelName": "train",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": training_data_uri,
                                "S3DataDistributionType": "ShardedByS3Key",
                            }
                        },
                        "ContentType": "text/csv;label_size=0",
                    }
                ],
                OutputDataConfig={"S3OutputPath": output_path},
                ResourceConfig={
                    "InstanceType": self.config.instance_type,
                    "InstanceCount": self.config.instance_count,
                    "VolumeSizeInGB": 50,
                },
                StoppingCondition={"MaxRuntimeInSeconds": 14400},
            )
            logger.info("Created training job: %s", job_name)
            return job_name
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create SageMaker training job: {exc}"
            ) from exc

    def wait_for_training(self, job_name: str) -> str:
        """Poll the training job until it completes, fails, or times out.

        Args:
            job_name: The SageMaker training job name.

        Returns:
            The S3 path of the model artifact.

        Raises:
            RuntimeError: If the job fails, is stopped, or polling times out.
        """
        for attempt in range(1, self.max_poll_attempts + 1):
            response = self._sagemaker.describe_training_job(
                TrainingJobName=job_name
            )
            status = response["TrainingJobStatus"]
            logger.info(
                "Training job %s status: %s (poll %d/%d)",
                job_name, status, attempt, self.max_poll_attempts,
            )

            if status == "Completed":
                model_artifact = response["ModelArtifacts"]["S3ModelArtifacts"]
                logger.info(
                    "Training job %s completed. Model: %s", job_name, model_artifact
                )
                return model_artifact

            if status in ("Failed", "Stopped"):
                failure_reason = response.get(
                    "FailureReason", "No failure details available"
                )
                raise RuntimeError(
                    f"SageMaker training job {job_name} {status.lower()}: "
                    f"{failure_reason}"
                )

            time.sleep(self.poll_interval)

        raise RuntimeError(
            f"SageMaker training job {job_name} timed out after "
            f"{self.max_poll_attempts * self.poll_interval}s"
        )

    def write_training_manifest(
        self,
        job_name: str,
        model_artifact_path: str,
        metadata: dict,
    ) -> None:
        """Write a training manifest JSON to S3.

        Args:
            job_name: The SageMaker training job name.
            model_artifact_path: S3 path to the model artifact.
            metadata: Additional metadata (training_duration_seconds, record_count).
        """
        manifest = {
            "job_name": job_name,
            "model_artifact_s3_path": model_artifact_path,
            "training_duration_seconds": metadata.get("training_duration_seconds", 0),
            "record_count": metadata.get("record_count", 0),
            "hyperparameters": {
                "num_trees": self.config.num_trees,
                "num_samples_per_tree": self.config.num_samples_per_tree,
                "feature_dim": metadata.get("feature_dim", 0),
            },
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        manifest_key = f"{self.config.models_prefix}{self._timestamp}/manifest.json"
        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
        )
        logger.info("Wrote training manifest to s3://%s/%s", self.config.s3_bucket, manifest_key)

    def run(self) -> dict[str, str]:
        """Execute the full training flow.

        Orchestrates: get_feature_dim → create_training_job → wait_for_training
        → write_training_manifest.

        Returns:
            Dict with 'model_artifact_path' and 'manifest_path' keys.
        """
        start_time = time.time()

        feature_dim = self.get_feature_dim()

        job_name = self.create_training_job()
        model_artifact_path = self.wait_for_training(job_name)

        training_duration = int(time.time() - start_time)

        metadata = {
            "training_duration_seconds": training_duration,
            "record_count": getattr(self, "_record_count", 0),
            "feature_dim": feature_dim,
        }
        self.write_training_manifest(job_name, model_artifact_path, metadata)

        manifest_path = (
            f"s3://{self.config.s3_bucket}/"
            f"{self.config.models_prefix}{self._timestamp}/manifest.json"
        )

        logger.info("Training pipeline complete for job %s", job_name)
        return {
            "model_artifact_path": model_artifact_path,
            "manifest_path": manifest_path,
        }
