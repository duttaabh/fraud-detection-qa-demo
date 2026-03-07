"""AWS Glue feature engineering job for the anomaly detection pipeline.

Reads enriched claim Parquet data from S3, transforms it into numerical
feature vectors suitable for SageMaker Random Cut Forest, and writes the
feature matrix, scaler parameters, and feature metadata back to S3.

Replaces the old ``glue/jsonl_prep_job.py`` Bedrock JSONL preparation job.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class FeatureEngineeringConfig:
    """Configuration for the feature engineering Glue job."""

    s3_bucket: str
    enriched_prefix: str = "enriched/"
    ingested_prefix: str = "ingested/"
    features_prefix: str = "features/"
    feature_columns: list[str] = field(default_factory=lambda: [
        "claim_amount",
        "days_between_contract_start_and_claim",
    ])
    categorical_columns: list[str] = field(default_factory=lambda: [
        "claim_type",
        "product_category",
    ])


# ---------------------------------------------------------------------------
# Feature Engineering Job
# ---------------------------------------------------------------------------


class FeatureEngineeringJob:
    """Reads enriched claims from S3, engineers numerical features,
    applies standard scaling, and writes the feature matrix plus
    metadata to S3."""

    def __init__(self, config: FeatureEngineeringConfig, spark: SparkSession) -> None:
        self.config = config
        self.spark = spark

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self.config.s3_bucket}/{key}"

    # ------------------------------------------------------------------
    # Feature methods
    # ------------------------------------------------------------------

    def compute_manufacturer_frequency(self, claims_df: DataFrame) -> DataFrame:
        """Add ``manufacturer_claim_frequency`` column — the count of
        claims per manufacturer_id joined back onto each row.

        Uses broadcast join hint since the frequency table is small
        (one row per manufacturer).
        """
        freq = (
            claims_df.groupBy("manufacturer_id")
            .agg(F.count("*").alias("manufacturer_claim_frequency"))
        )
        return claims_df.join(F.broadcast(freq), on="manufacturer_id", how="left")

    def one_hot_encode(self, df: DataFrame, columns: list[str]) -> DataFrame:
        """One-hot encode the given categorical columns.

        For each column, known categories produce a ``<col>_<value>``
        column (0 or 1).  Null / missing values produce a 1 in the
        ``<col>_unknown`` column.
        """
        known_categories: dict[str, list[str]] = {
            "claim_type": ["repair", "replacement", "refund"],
            "product_category": ["electronics", "appliances", "automotive", "furniture"],
        }

        result = df
        for col_name in columns:
            categories = known_categories.get(col_name, [])
            for cat in categories:
                ohe_col = f"{col_name}_{cat}"
                result = result.withColumn(
                    ohe_col,
                    F.when(F.col(col_name) == cat, F.lit(1)).otherwise(F.lit(0)).cast(T.ByteType()),
                )
            # unknown column for nulls
            unknown_col = f"{col_name}_unknown"
            result = result.withColumn(
                unknown_col,
                F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0)).cast(T.ByteType()),
            )
        return result

    def compute_days_feature(self, df: DataFrame) -> DataFrame:
        """Compute ``days_between_contract_start_and_claim`` from
        ``claim_date`` and ``start_date`` columns."""
        result = df.withColumn(
            "days_between_contract_start_and_claim",
            F.datediff(
                F.to_date(F.col("claim_date")),
                F.to_date(F.col("start_date")),
            ).cast(T.DoubleType()),
        )
        return result

    def impute_missing(self, df: DataFrame) -> DataFrame:
        """Impute missing values.

        - Numerical nulls → column median
        - Categorical nulls → handled via the ``_unknown`` one-hot column
          (already set to 1 by ``one_hot_encode``).
        """
        numerical_cols = self.config.feature_columns + ["manufacturer_claim_frequency"]
        for col_name in numerical_cols:
            if col_name not in df.columns:
                continue
            median_val = df.approxQuantile(col_name, [0.5], 0.001)
            fill_value = median_val[0] if median_val and median_val[0] is not None else 0.0
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(fill_value)).otherwise(F.col(col_name)),
            )
        return df

    def standard_scale(
        self, df: DataFrame, columns: list[str]
    ) -> tuple[DataFrame, dict]:
        """Apply zero-mean, unit-variance scaling to the given columns.

        Computes mean and stddev for all columns in a single aggregation
        pass (instead of one pass per column) for efficiency at scale.

        Returns ``(scaled_df, scaler_params)`` where *scaler_params* maps
        each column name to ``{"mean": float, "std": float}``.
        """
        # Filter to columns that actually exist
        valid_cols = [c for c in columns if c in df.columns]
        if not valid_cols:
            return df, {}

        # Single-pass aggregation for all columns
        agg_exprs = []
        for col_name in valid_cols:
            agg_exprs.append(F.mean(F.col(col_name)).alias(f"{col_name}__mean"))
            agg_exprs.append(F.stddev(F.col(col_name)).alias(f"{col_name}__std"))

        stats_row = df.select(*agg_exprs).collect()[0]

        scaler_params: dict[str, dict[str, float]] = {}
        result = df

        for col_name in valid_cols:
            col_mean = float(stats_row[f"{col_name}__mean"]) if stats_row[f"{col_name}__mean"] is not None else 0.0
            col_std = float(stats_row[f"{col_name}__std"]) if stats_row[f"{col_name}__std"] is not None else 1.0
            if col_std == 0.0:
                col_std = 1.0

            scaler_params[col_name] = {"mean": col_mean, "std": col_std}

            result = result.withColumn(
                col_name,
                (F.col(col_name) - F.lit(col_mean)) / F.lit(col_std),
            )

        return result, scaler_params

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self) -> dict[str, str]:
        """Execute the full feature engineering flow.

        Returns a dict with S3 paths::

            {
                "feature_matrix": "s3://...",
                "scaler_params": "s3://...",
                "feature_metadata": "s3://...",
            }
        """
        import boto3

        enriched_path = self._s3_uri(f"{self.config.enriched_prefix}claims/")
        claims_df = self.spark.read.parquet(enriched_path)

        # Join with ingested contracts to get start_date
        contracts_path = self._s3_uri(f"{self.config.ingested_prefix}contracts/")
        contracts_df = self.spark.read.parquet(contracts_path).select("contract_id", "start_date")
        claims_df = claims_df.join(contracts_df, on="contract_id", how="left")

        # 1. Compute days feature
        df = self.compute_days_feature(claims_df)

        # 2. Compute manufacturer claim frequency
        df = self.compute_manufacturer_frequency(df)

        # 3. One-hot encode categorical columns
        df = self.one_hot_encode(df, self.config.categorical_columns)

        # 4. Impute missing values
        df = self.impute_missing(df)

        # 5. Standard-scale continuous features
        continuous_cols = self.config.feature_columns + ["manufacturer_claim_frequency"]
        df, scaler_params = self.standard_scale(df, continuous_cols)

        # 6. Select final feature columns
        one_hot_cols: list[str] = []
        known_categories: dict[str, list[str]] = {
            "claim_type": ["repair", "replacement", "refund"],
            "product_category": ["electronics", "appliances", "automotive", "furniture"],
        }
        for col_name in self.config.categorical_columns:
            categories = known_categories.get(col_name, [])
            for cat in categories:
                one_hot_cols.append(f"{col_name}_{cat}")
            one_hot_cols.append(f"{col_name}_unknown")

        all_feature_cols = continuous_cols + one_hot_cols
        output_cols = ["claim_id"] + all_feature_cols
        feature_df = df.select(*output_cols)

        # 7. Write feature matrix Parquet
        feature_path = self._s3_uri(f"{self.config.features_prefix}claim_features.parquet")
        feature_df.write.mode("overwrite").parquet(feature_path)

        # 8. Build feature metadata
        feature_types: dict[str, str] = {}
        for c in continuous_cols:
            feature_types[c] = "continuous"
        for c in one_hot_cols:
            feature_types[c] = "one_hot"

        scaler_params_s3 = f"s3://{self.config.s3_bucket}/{self.config.features_prefix}metadata/scaler_params.json"
        feature_metadata = {
            "feature_columns": all_feature_cols,
            "feature_types": feature_types,
            "feature_dim": len(all_feature_cols),
            "scaler_params_path": scaler_params_s3,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # 9. Write JSON files via boto3 (Spark saveAsTextFile fails on Glue 4.0)
        s3_client = boto3.client("s3")

        scaler_key = f"{self.config.features_prefix}metadata/scaler_params.json"
        s3_client.put_object(
            Bucket=self.config.s3_bucket,
            Key=scaler_key,
            Body=json.dumps(scaler_params, indent=2).encode("utf-8"),
        )

        metadata_key = f"{self.config.features_prefix}metadata/feature_metadata.json"
        s3_client.put_object(
            Bucket=self.config.s3_bucket,
            Key=metadata_key,
            Body=json.dumps(feature_metadata, indent=2).encode("utf-8"),
        )

        logger.info(
            "Feature engineering complete: %d rows, %d features",
            feature_df.count(),
            len(all_feature_cols),
        )

        return {
            "feature_matrix": feature_path,
            "scaler_params": scaler_params_s3,
            "feature_metadata": f"s3://{self.config.s3_bucket}/{metadata_key}",
        }


# ---------------------------------------------------------------------------
# Glue entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["S3_BUCKET"])
    spark = SparkSession.builder.appName("feature-engineering").getOrCreate()

    config = FeatureEngineeringConfig(s3_bucket=args["S3_BUCKET"])
    job = FeatureEngineeringJob(config, spark)
    result = job.run()

    logger.info("Feature engineering complete: %s", result)
    print(result)

    spark.stop()
