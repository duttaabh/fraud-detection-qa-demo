"""Unit tests for the SageMaker training module."""

import io
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from moto import mock_aws
import boto3

from pipeline.sagemaker_training import (
    SageMakerTrainingConfig,
    SageMakerTrainingJob,
    RCF_IMAGE_URI,
)

TEST_BUCKET = "test-fraud-detection"


@pytest.fixture
def training_config():
    return SageMakerTrainingConfig(
        s3_bucket=TEST_BUCKET,
        sagemaker_role_arn="arn:aws:iam::123456789012:role/SageMakerRole",
    )


@pytest.fixture
def s3_with_features(aws_credentials):
    """Set up S3 with feature data and metadata."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET)

        # Write feature metadata
        metadata = {
            "feature_columns": [
                "claim_amount",
                "days_between_contract_start_and_claim",
                "manufacturer_claim_frequency",
                "claim_type_repair",
                "claim_type_replacement",
                "claim_type_refund",
                "claim_type_unknown",
                "product_category_electronics",
                "product_category_appliances",
                "product_category_automotive",
                "product_category_furniture",
                "product_category_unknown",
            ],
            "feature_types": {
                "claim_amount": "continuous",
                "days_between_contract_start_and_claim": "continuous",
                "manufacturer_claim_frequency": "continuous",
            },
            "feature_dim": 12,
            "scaler_params_path": f"s3://{TEST_BUCKET}/features/metadata/scaler_params.json",
            "created_at": "2024-01-01T12:00:00Z",
        }
        s3.put_object(
            Bucket=TEST_BUCKET,
            Key="features/metadata/feature_metadata.json",
            Body=json.dumps(metadata).encode("utf-8"),
        )

        # Write feature Parquet
        df = pd.DataFrame({
            "claim_id": ["CLAIM-001", "CLAIM-002", "CLAIM-003"],
            "claim_amount": [100.0, 200.0, 300.0],
            "days_between_contract_start_and_claim": [10.0, 20.0, 30.0],
            "manufacturer_claim_frequency": [5.0, 10.0, 15.0],
            "claim_type_repair": [1, 0, 0],
            "claim_type_replacement": [0, 1, 0],
            "claim_type_refund": [0, 0, 1],
            "claim_type_unknown": [0, 0, 0],
            "product_category_electronics": [1, 0, 0],
            "product_category_appliances": [0, 1, 0],
            "product_category_automotive": [0, 0, 1],
            "product_category_furniture": [0, 0, 0],
            "product_category_unknown": [0, 0, 0],
        })
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        s3.put_object(
            Bucket=TEST_BUCKET,
            Key="features/claim_features.parquet",
            Body=buf.getvalue(),
        )

        yield s3


class TestGetFeatureDim:
    def test_reads_feature_dim_from_metadata(self, s3_with_features, training_config):
        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=MagicMock(),
            s3_client=s3_with_features,
        )
        assert job.get_feature_dim() == 12

    def test_raises_on_missing_metadata(self, aws_credentials, training_config):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=TEST_BUCKET)
            job = SageMakerTrainingJob(
                config=training_config,
                sagemaker_client=MagicMock(),
                s3_client=s3,
            )
            with pytest.raises(RuntimeError, match="Failed to read feature metadata"):
                job.get_feature_dim()


class TestPrepareTrainingCsv:
    def test_converts_parquet_to_csv_without_header(self, s3_with_features, training_config):
        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=MagicMock(),
            s3_client=s3_with_features,
        )
        uri = job._prepare_training_csv()

        assert uri == f"s3://{TEST_BUCKET}/features/training/"
        assert job._record_count == 3

        # Verify CSV was written without claim_id and without header
        response = s3_with_features.get_object(
            Bucket=TEST_BUCKET, Key="features/training/train.csv"
        )
        csv_content = response["Body"].read().decode("utf-8")
        lines = csv_content.strip().split("\n")
        assert len(lines) == 3  # 3 data rows, no header
        # First line should not contain "claim_id"
        assert "claim_id" not in lines[0]
        assert "CLAIM" not in csv_content


class TestCreateTrainingJob:
    def test_creates_job_with_correct_params(self, s3_with_features, training_config):
        mock_sm = MagicMock()
        mock_sm.create_training_job.return_value = {}

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=s3_with_features,
        )
        job_name = job.create_training_job()

        assert job_name.startswith("anomaly-detection-")
        mock_sm.create_training_job.assert_called_once()

        call_kwargs = mock_sm.create_training_job.call_args[1]
        assert call_kwargs["TrainingJobName"] == job_name
        assert call_kwargs["AlgorithmSpecification"]["TrainingImage"] == RCF_IMAGE_URI
        assert call_kwargs["RoleArn"] == training_config.sagemaker_role_arn
        assert call_kwargs["HyperParameters"]["num_trees"] == "100"
        assert call_kwargs["HyperParameters"]["num_samples_per_tree"] == "256"
        assert call_kwargs["HyperParameters"]["feature_dim"] == "12"
        assert call_kwargs["InputDataConfig"][0]["ContentType"] == "text/csv;label_size=0"
        assert (
            call_kwargs["InputDataConfig"][0]["DataSource"]["S3DataSource"][
                "S3DataDistributionType"
            ]
            == "ShardedByS3Key"
        )

    def test_raises_on_api_failure(self, s3_with_features, training_config):
        mock_sm = MagicMock()
        mock_sm.create_training_job.side_effect = Exception("AccessDenied")

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=s3_with_features,
        )
        with pytest.raises(RuntimeError, match="Failed to create SageMaker training job"):
            job.create_training_job()


class TestWaitForTraining:
    def test_returns_model_artifact_on_completion(self, training_config):
        mock_sm = MagicMock()
        mock_sm.describe_training_job.return_value = {
            "TrainingJobStatus": "Completed",
            "ModelArtifacts": {
                "S3ModelArtifacts": f"s3://{TEST_BUCKET}/models/20240101-120000/output/model.tar.gz"
            },
        }

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=MagicMock(),
            poll_interval=0,
        )
        result = job.wait_for_training("test-job")
        assert result == f"s3://{TEST_BUCKET}/models/20240101-120000/output/model.tar.gz"

    def test_raises_on_failed_job(self, training_config):
        mock_sm = MagicMock()
        mock_sm.describe_training_job.return_value = {
            "TrainingJobStatus": "Failed",
            "FailureReason": "InternalServerError: out of memory",
        }

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=MagicMock(),
            poll_interval=0,
        )
        with pytest.raises(RuntimeError, match="failed.*InternalServerError"):
            job.wait_for_training("test-job")

    def test_raises_on_stopped_job(self, training_config):
        mock_sm = MagicMock()
        mock_sm.describe_training_job.return_value = {
            "TrainingJobStatus": "Stopped",
        }

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=MagicMock(),
            poll_interval=0,
        )
        with pytest.raises(RuntimeError, match="stopped"):
            job.wait_for_training("test-job")

    def test_raises_on_timeout(self, training_config):
        mock_sm = MagicMock()
        mock_sm.describe_training_job.return_value = {
            "TrainingJobStatus": "InProgress",
        }

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=MagicMock(),
            max_poll_attempts=2,
            poll_interval=0,
        )
        with pytest.raises(RuntimeError, match="timed out"):
            job.wait_for_training("test-job")

    def test_polls_until_complete(self, training_config):
        mock_sm = MagicMock()
        mock_sm.describe_training_job.side_effect = [
            {"TrainingJobStatus": "InProgress"},
            {"TrainingJobStatus": "InProgress"},
            {
                "TrainingJobStatus": "Completed",
                "ModelArtifacts": {
                    "S3ModelArtifacts": f"s3://{TEST_BUCKET}/models/output/model.tar.gz"
                },
            },
        ]

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=MagicMock(),
            poll_interval=0,
        )
        result = job.wait_for_training("test-job")
        assert result == f"s3://{TEST_BUCKET}/models/output/model.tar.gz"
        assert mock_sm.describe_training_job.call_count == 3


class TestWriteTrainingManifest:
    def test_writes_manifest_with_all_fields(self, s3_with_features, training_config):
        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=MagicMock(),
            s3_client=s3_with_features,
        )

        model_path = f"s3://{TEST_BUCKET}/models/20240101-120000/output/model.tar.gz"
        metadata = {
            "training_duration_seconds": 300,
            "record_count": 88000000,
            "feature_dim": 12,
        }
        job.write_training_manifest("test-job", model_path, metadata)

        # Read back the manifest
        manifest_key = f"models/{job._timestamp}/manifest.json"
        response = s3_with_features.get_object(
            Bucket=TEST_BUCKET, Key=manifest_key
        )
        manifest = json.loads(response["Body"].read().decode("utf-8"))

        assert manifest["job_name"] == "test-job"
        assert manifest["model_artifact_s3_path"] == model_path
        assert manifest["training_duration_seconds"] == 300
        assert manifest["record_count"] == 88000000
        assert manifest["hyperparameters"]["num_trees"] == 100
        assert manifest["hyperparameters"]["num_samples_per_tree"] == 256
        assert manifest["hyperparameters"]["feature_dim"] == 12
        assert "created_at" in manifest


class TestRun:
    def test_orchestrates_full_flow(self, s3_with_features, training_config):
        mock_sm = MagicMock()
        mock_sm.create_training_job.return_value = {}
        model_path = f"s3://{TEST_BUCKET}/models/20240101-120000/output/model.tar.gz"
        mock_sm.describe_training_job.return_value = {
            "TrainingJobStatus": "Completed",
            "ModelArtifacts": {"S3ModelArtifacts": model_path},
        }

        job = SageMakerTrainingJob(
            config=training_config,
            sagemaker_client=mock_sm,
            s3_client=s3_with_features,
            poll_interval=0,
        )
        result = job.run()

        assert "model_artifact_path" in result
        assert "manifest_path" in result
        assert result["model_artifact_path"] == model_path
        assert "manifest.json" in result["manifest_path"]

        # Verify manifest was written
        manifest_key = f"models/{job._timestamp}/manifest.json"
        response = s3_with_features.get_object(
            Bucket=TEST_BUCKET, Key=manifest_key
        )
        manifest = json.loads(response["Body"].read().decode("utf-8"))
        assert manifest["job_name"].startswith("anomaly-detection-")
        assert manifest["record_count"] == 3  # from the test Parquet
