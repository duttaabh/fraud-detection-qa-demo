"""AWS Glue ingestion job for the fraud detection & quality analysis pipeline.

Reads raw CSV data (contracts, claims) from S3, optionally filters by a
high-watermark timestamp for incremental ingestion, writes partitioned
Parquet output back to S3, and produces an IngestionSummary.

This module is designed to run inside an AWS Glue PySpark environment but
can also be exercised locally with a regular SparkSession for testing.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration & result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class IngestionConfig:
    """Configuration for the ingestion Glue job."""

    s3_bucket: str
    raw_prefix: str = "raw/"
    ingested_prefix: str = "ingested/"
    high_watermark_timestamp: datetime | None = None  # None = full load
    max_retries: int = 3
    retry_backoff_base: float = 2.0


@dataclass
class IngestionSummary:
    """Summary produced after an ingestion run."""

    contracts_extracted: int = 0
    claims_extracted: int = 0
    run_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "success"  # "success" | "partial_failure" | "failure"
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Ingestion job
# ---------------------------------------------------------------------------


class IngestionJob:
    """Reads raw CSVs from S3, applies optional watermark filtering, and
    writes partitioned Parquet to the ingested prefix."""

    def __init__(self, config: IngestionConfig, spark: SparkSession) -> None:
        self.config = config
        self.spark = spark

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self.config.s3_bucket}/{key}"

    def _read_csv_with_retry(self, s3_path: str) -> DataFrame:
        """Read a CSV from S3 with retry + exponential backoff."""
        last_exc: Exception | None = None
        for attempt in range(self.config.max_retries):
            try:
                df = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(s3_path)
                )
                # Force evaluation so read errors surface here
                df.cache()
                df.count()
                return df
            except Exception as exc:
                last_exc = exc
                wait = self.config.retry_backoff_base ** attempt
                logger.warning(
                    "S3 read attempt %d/%d failed for %s: %s – retrying in %.1fs",
                    attempt + 1,
                    self.config.max_retries,
                    s3_path,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise RuntimeError(
            f"Failed to read {s3_path} after {self.config.max_retries} retries"
        ) from last_exc

    def _apply_watermark(self, df: DataFrame) -> DataFrame:
        """Filter rows whose ``updated_at`` >= the high-watermark timestamp."""
        if self.config.high_watermark_timestamp is None:
            return df

        watermark_str = self.config.high_watermark_timestamp.isoformat()
        return df.filter(F.col("updated_at") >= F.lit(watermark_str))

    # ------------------------------------------------------------------
    # Public extraction methods
    # ------------------------------------------------------------------

    def extract_contracts(self) -> DataFrame:
        """Read contracts CSV from S3 and apply watermark filter."""
        path = self._s3_uri(f"{self.config.raw_prefix}contracts.csv")
        df = self._read_csv_with_retry(path)
        return self._apply_watermark(df)

    def extract_claims(self) -> DataFrame:
        """Read claims CSV from S3 and apply watermark filter."""
        path = self._s3_uri(f"{self.config.raw_prefix}claims.csv")
        df = self._read_csv_with_retry(path)
        return self._apply_watermark(df)

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def _write_parquet(self, df: DataFrame, entity: str) -> int:
        """Write a DataFrame as Parquet partitioned by extraction date.

        Returns the number of rows written.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_path = self._s3_uri(
            f"{self.config.ingested_prefix}{entity}/dt={today}"
        )
        df.write.mode("overwrite").parquet(output_path)
        return df.count()

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self) -> IngestionSummary:
        """Execute the full ingestion flow and return a summary."""
        summary = IngestionSummary()
        contracts_ok = True
        claims_ok = True

        # --- Contracts ---
        try:
            contracts_df = self.extract_contracts()
            summary.contracts_extracted = self._write_parquet(contracts_df, "contracts")
        except Exception as exc:
            contracts_ok = False
            msg = f"Contract ingestion failed: {exc}"
            logger.error(msg)
            summary.errors.append(msg)

        # --- Claims ---
        try:
            claims_df = self.extract_claims()
            summary.claims_extracted = self._write_parquet(claims_df, "claims")
        except Exception as exc:
            claims_ok = False
            msg = f"Claim ingestion failed: {exc}"
            logger.error(msg)
            summary.errors.append(msg)

        # --- Status ---
        if contracts_ok and claims_ok:
            summary.status = "success"
        elif contracts_ok or claims_ok:
            summary.status = "partial_failure"
        else:
            summary.status = "failure"

        return summary


# ---------------------------------------------------------------------------
# Glue entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["S3_BUCKET"])
    spark = SparkSession.builder.appName("fraud-detection-ingestion").getOrCreate()

    config = IngestionConfig(s3_bucket=args["S3_BUCKET"])
    job = IngestionJob(config, spark)
    summary = job.run()

    logger.info("Ingestion complete: %s", summary)
    print(summary)

    spark.stop()
