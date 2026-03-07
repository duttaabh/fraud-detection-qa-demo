"""Shared configuration for the fraud detection & quality analysis pipeline."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Central configuration for all pipeline components."""

    # S3
    s3_bucket: str = os.environ.get("S3_BUCKET", "fraud-detection-demo")
    raw_prefix: str = "raw/"
    ingested_prefix: str = "ingested/"
    enriched_prefix: str = "enriched/"
    retry_prefix: str = "retry/"
    features_prefix: str = "features/"
    models_prefix: str = "models/"
    sagemaker_output_prefix: str = "sagemaker/output/"
    results_prefix: str = "results/"

    # SKU Microservice
    sku_api_url: str = "https://api.example.com"
    sku_batch_size: int = 100

    # SageMaker
    sagemaker_role_arn: str = ""
    sagemaker_instance_type: str = "ml.m5.xlarge"
    sagemaker_training_instance_count: int = 1
    sagemaker_transform_instance_count: int = 5
    num_trees: int = 100
    num_samples_per_tree: int = 256

    # Thresholds
    fraud_threshold: float = 0.7
    quality_threshold: float = 2.0

    # Ingestion
    max_retries: int = 3
    retry_backoff_base: float = 2.0

    # Synthetic data
    num_contracts: int = 10_000
    num_claims: int = 10_000
    num_skus: int = 500
    num_manufacturers: int = 100

    # Data generation chunk size (rows per chunk for streaming upload)
    data_gen_chunk_size: int = 500_000

    # Results loader chunk size (rows per Parquet read chunk)
    results_chunk_size: int = 100_000
