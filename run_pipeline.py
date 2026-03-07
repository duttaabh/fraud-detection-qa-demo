"""Pipeline orchestration script for the fraud detection & quality analysis demo.

Executes the full batch flow:
  1. Synthetic data generation -> S3 upload
  2. Ingestion Glue job (CSV -> Parquet)
  3. Enrichment Glue job (SKU lookup + join)
  4. Feature engineering Glue job (enriched -> feature matrix)
  5. SageMaker RCF training
  6. SageMaker batch transform (scoring)
  7. Results loading (parse SageMaker output -> JSON to S3)

Each stage is idempotent: re-running with the same config produces the same output.

Usage:
    python run_pipeline.py                          # full pipeline
    python run_pipeline.py --skip-training          # reuse latest trained model
    python run_pipeline.py --stage feature_engineering  # run from a specific stage onward
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from enum import IntEnum

import boto3

from config import PipelineConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipeline stages (ordered)
# ---------------------------------------------------------------------------

class Stage(IntEnum):
    DATA_GENERATION = 1
    INGESTION = 2
    ENRICHMENT = 3
    FEATURE_ENGINEERING = 4
    SAGEMAKER_TRAINING = 5
    SAGEMAKER_BATCH_TRANSFORM = 6
    RESULTS_LOADING = 7


STAGE_NAMES: dict[str, Stage] = {
    "data_generation": Stage.DATA_GENERATION,
    "ingestion": Stage.INGESTION,
    "enrichment": Stage.ENRICHMENT,
    "feature_engineering": Stage.FEATURE_ENGINEERING,
    "training": Stage.SAGEMAKER_TRAINING,
    "transform": Stage.SAGEMAKER_BATCH_TRANSFORM,
    "results": Stage.RESULTS_LOADING,
}


@dataclass
class PipelineResult:
    """Aggregated result from a pipeline run."""
    stages_completed: list[str] = field(default_factory=list)
    data_generation: dict | None = None
    ingestion: dict | None = None
    enrichment: dict | None = None
    feature_engineering: dict | None = None
    training: dict | None = None
    transform: dict | None = None
    results_loading: dict | None = None
    errors: list[str] = field(default_factory=list)


class PipelineOrchestrator:
    """Orchestrates the full fraud-detection / quality-analysis batch pipeline."""

    def __init__(
        self,
        config: PipelineConfig,
        *,
        skip_training: bool = False,
        s3_client=None,
        sagemaker_client=None,
    ) -> None:
        self.config = config
        self.skip_training = skip_training
        self._s3 = s3_client or boto3.client("s3")
        self._sagemaker = sagemaker_client
        self._model_artifact_path: str | None = None

    # ------------------------------------------------------------------
    # Stage 1 - Synthetic data generation
    # ------------------------------------------------------------------

    def run_data_generation(self) -> dict[str, int]:
        from scripts.generate_data import DataGenerator, DataGeneratorConfig

        gen_config = DataGeneratorConfig.from_pipeline_config(self.config)
        generator = DataGenerator(gen_config, s3_client=self._s3)
        counts = generator.run()
        logger.info("Data generation complete: %s", counts)
        return counts

    # ------------------------------------------------------------------
    # Stage 2 - Ingestion
    # ------------------------------------------------------------------

    def run_ingestion(self) -> dict:
        from pyspark.sql import SparkSession
        from glue.ingestion_job import IngestionConfig, IngestionJob

        spark = SparkSession.builder.appName("ingestion").getOrCreate()
        ing_config = IngestionConfig(
            s3_bucket=self.config.s3_bucket,
            raw_prefix=self.config.raw_prefix,
            ingested_prefix=self.config.ingested_prefix,
            max_retries=self.config.max_retries,
            retry_backoff_base=self.config.retry_backoff_base,
        )
        job = IngestionJob(ing_config, spark)
        summary = job.run()
        logger.info("Ingestion complete: %s", summary)
        return {
            "contracts_extracted": summary.contracts_extracted,
            "claims_extracted": summary.claims_extracted,
            "status": summary.status,
            "errors": summary.errors,
        }

    # ------------------------------------------------------------------
    # Stage 3 - Enrichment
    # ------------------------------------------------------------------

    def run_enrichment(self) -> dict:
        from pyspark.sql import SparkSession
        from glue.enrichment_job import EnrichmentConfig, EnrichmentJob

        spark = SparkSession.builder.appName("enrichment").getOrCreate()
        enr_config = EnrichmentConfig(
            s3_bucket=self.config.s3_bucket,
            ingested_prefix=self.config.ingested_prefix,
            enriched_prefix=self.config.enriched_prefix,
            sku_api_url=self.config.sku_api_url,
            batch_size=self.config.sku_batch_size,
            retry_queue_prefix=self.config.retry_prefix,
        )
        job = EnrichmentJob(enr_config, spark)
        summary = job.run()
        logger.info("Enrichment complete: %s", summary)
        return {
            "claims_enriched": summary.claims_enriched,
            "claims_unknown_manufacturer": summary.claims_unknown_manufacturer,
            "claims_queued_for_retry": summary.claims_queued_for_retry,
            "status": summary.status,
            "errors": summary.errors,
        }

    # ------------------------------------------------------------------
    # Stage 4 - Feature Engineering
    # ------------------------------------------------------------------

    def run_feature_engineering(self) -> dict:
        from pyspark.sql import SparkSession
        from glue.feature_engineering_job import FeatureEngineeringConfig, FeatureEngineeringJob

        spark = SparkSession.builder.appName("feature_engineering").getOrCreate()
        fe_config = FeatureEngineeringConfig(
            s3_bucket=self.config.s3_bucket,
            enriched_prefix=self.config.enriched_prefix,
            ingested_prefix=self.config.ingested_prefix,
            features_prefix=self.config.features_prefix,
        )
        job = FeatureEngineeringJob(fe_config, spark)
        result = job.run()
        logger.info("Feature engineering complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Stage 5 - SageMaker Training
    # ------------------------------------------------------------------

    def run_sagemaker_training(self) -> dict:
        from pipeline.sagemaker_training import SageMakerTrainingConfig, SageMakerTrainingJob

        train_config = SageMakerTrainingConfig(
            s3_bucket=self.config.s3_bucket,
            features_prefix=self.config.features_prefix,
            models_prefix=self.config.models_prefix,
            sagemaker_role_arn=self.config.sagemaker_role_arn,
            instance_type=self.config.sagemaker_instance_type,
            instance_count=self.config.sagemaker_training_instance_count,
            num_trees=self.config.num_trees,
            num_samples_per_tree=self.config.num_samples_per_tree,
        )
        job = SageMakerTrainingJob(
            train_config,
            sagemaker_client=self._sagemaker,
            s3_client=self._s3,
        )
        result = job.run()
        self._model_artifact_path = result["model_artifact_path"]
        logger.info("SageMaker training complete: %s", result)
        return result

    def _find_latest_model(self) -> str:
        """Find the most recent model artifact by listing manifest files.

        Returns:
            S3 path to the model artifact.

        Raises:
            FileNotFoundError: If no trained model exists.
        """
        prefix = self.config.models_prefix
        paginator = self._s3.get_paginator("list_objects_v2")
        manifests: list[dict] = []

        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("/manifest.json"):
                    manifests.append(obj)

        if not manifests:
            raise FileNotFoundError(
                f"No trained model found under s3://{self.config.s3_bucket}/{prefix}. "
                f"Run training first or remove --skip-training."
            )

        # Sort by LastModified descending
        manifests.sort(key=lambda o: o["LastModified"], reverse=True)
        latest_key = manifests[0]["Key"]

        resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=latest_key)
        manifest = json.loads(resp["Body"].read().decode("utf-8"))
        return manifest["model_artifact_s3_path"]

    # ------------------------------------------------------------------
    # Stage 6 - SageMaker Batch Transform
    # ------------------------------------------------------------------

    def run_sagemaker_transform(self) -> dict:
        from pipeline.sagemaker_batch_transform import (
            SageMakerBatchTransformConfig,
            SageMakerBatchTransform,
        )

        if not self._model_artifact_path:
            raise RuntimeError("No model artifact path available. Run training first.")

        transform_config = SageMakerBatchTransformConfig(
            s3_bucket=self.config.s3_bucket,
            features_prefix=self.config.features_prefix,
            sagemaker_output_prefix=self.config.sagemaker_output_prefix,
            model_artifact_path=self._model_artifact_path,
            instance_type=self.config.sagemaker_instance_type,
            instance_count=self.config.sagemaker_transform_instance_count,
        )
        job = SageMakerBatchTransform(
            transform_config,
            sagemaker_client=self._sagemaker,
        )
        result = job.run(self._model_artifact_path)
        logger.info("SageMaker batch transform complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Stage 7 - Results loading
    # ------------------------------------------------------------------

    def run_results_loading(self) -> dict:
        from pipeline.results_loader import ResultsLoader, ResultsLoaderConfig

        model_version = self._model_artifact_path or "unknown"

        loader_config = ResultsLoaderConfig(
            s3_bucket=self.config.s3_bucket,
            sagemaker_output_prefix=self.config.sagemaker_output_prefix,
            features_prefix=self.config.features_prefix,
            enriched_prefix=self.config.enriched_prefix,
            results_prefix=self.config.results_prefix,
            fraud_threshold=self.config.fraud_threshold,
            quality_threshold=self.config.quality_threshold,
        )
        loader = ResultsLoader(loader_config, self._s3)
        summary = loader.run(model_version=model_version)
        logger.info("Results loading complete: %s", summary)
        return {
            "fraud_results_count": summary.fraud_results_count,
            "quality_results_count": summary.quality_results_count,
            "audit_entries_written": summary.audit_entries_written,
            "status": summary.status,
            "errors": summary.errors,
        }

    # ------------------------------------------------------------------
    # Full pipeline run
    # ------------------------------------------------------------------

    def run(self, start_stage: Stage = Stage.DATA_GENERATION) -> PipelineResult:
        """Execute the pipeline from *start_stage* onward."""
        result = PipelineResult()

        stages: list[tuple[Stage, str, callable]] = [
            (Stage.DATA_GENERATION, "data_generation", self.run_data_generation),
            (Stage.INGESTION, "ingestion", self.run_ingestion),
            (Stage.ENRICHMENT, "enrichment", self.run_enrichment),
            (Stage.FEATURE_ENGINEERING, "feature_engineering", self.run_feature_engineering),
            (Stage.SAGEMAKER_TRAINING, "training", self.run_sagemaker_training),
            (Stage.SAGEMAKER_BATCH_TRANSFORM, "transform", self.run_sagemaker_transform),
            (Stage.RESULTS_LOADING, "results", self.run_results_loading),
        ]

        for stage_enum, stage_name, stage_fn in stages:
            if stage_enum < start_stage:
                logger.info("Skipping stage: %s", stage_name)
                continue

            # Handle --skip-training
            if stage_enum == Stage.SAGEMAKER_TRAINING and self.skip_training:
                logger.info("Skipping training (--skip-training). Finding latest model...")
                self._model_artifact_path = self._find_latest_model()
                logger.info("Using model: %s", self._model_artifact_path)
                result.stages_completed.append(stage_name)
                continue

            logger.info("Running stage: %s", stage_name)
            try:
                output = stage_fn()
                setattr(result, stage_name, output)
                result.stages_completed.append(stage_name)
            except Exception as exc:
                msg = f"Stage '{stage_name}' failed: {exc}"
                logger.error(msg, exc_info=True)
                result.errors.append(msg)
                break  # Stop pipeline on failure

        return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the fraud detection & quality analysis pipeline."
    )
    parser.add_argument(
        "--skip-training", action="store_true",
        help="Skip SageMaker training; reuse the most recent model artifact.",
    )
    parser.add_argument(
        "--stage", choices=list(STAGE_NAMES.keys()), default="data_generation",
        help="Start pipeline from this stage (default: data_generation).",
    )
    parser.add_argument(
        "--bucket", default=None,
        help="Override S3 bucket name.",
    )
    parser.add_argument(
        "--role-arn", default=None,
        help="SageMaker IAM role ARN.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> PipelineResult:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    args = parse_args(argv)

    config_overrides: dict = {}
    if args.bucket:
        config_overrides["s3_bucket"] = args.bucket
    if args.role_arn:
        config_overrides["sagemaker_role_arn"] = args.role_arn

    config = PipelineConfig(**config_overrides) if config_overrides else PipelineConfig()

    orchestrator = PipelineOrchestrator(
        config,
        skip_training=args.skip_training,
    )

    start = STAGE_NAMES[args.stage]
    result = orchestrator.run(start_stage=start)

    if result.errors:
        logger.error("Pipeline completed with errors: %s", result.errors)
        sys.exit(1)

    logger.info("Pipeline completed successfully. Stages: %s", result.stages_completed)
    return result


if __name__ == "__main__":
    main()
