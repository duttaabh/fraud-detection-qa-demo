"""AWS Glue enrichment job for the fraud detection & quality analysis pipeline.

Reads ingested claim data from S3, enriches each claim with manufacturer
details by calling the SKU microservice (API Gateway + Lambda) in batches,
and writes enriched Parquet output back to S3.

Claims whose SKU cannot be resolved are flagged as ``manufacturer_unknown``.
When the SKU microservice is entirely unavailable, unenriched claims are
queued to a retry S3 path for a subsequent run.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration & result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class EnrichmentConfig:
    """Configuration for the enrichment Glue job."""

    s3_bucket: str
    ingested_prefix: str = "ingested/"
    enriched_prefix: str = "enriched/"
    sku_api_url: str = "https://api.example.com"
    batch_size: int = 100
    retry_queue_prefix: str = "retry/unenriched_claims/"
    max_retries: int = 3
    retry_backoff_base: float = 2.0


@dataclass
class EnrichmentSummary:
    """Summary produced after an enrichment run."""

    claims_enriched: int = 0
    claims_unknown_manufacturer: int = 0
    claims_queued_for_retry: int = 0
    run_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "success"  # "success" | "partial_failure" | "failure"
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Enrichment job
# ---------------------------------------------------------------------------


class EnrichmentJob:
    """Reads ingested claims from S3, enriches with manufacturer data via the
    SKU microservice, and writes enriched Parquet to S3."""

    def __init__(
        self,
        config: EnrichmentConfig,
        spark: SparkSession,
        http_session: requests.Session | None = None,
    ) -> None:
        self.config = config
        self.spark = spark
        self._http = http_session or requests.Session()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self.config.s3_bucket}/{key}"

    # ------------------------------------------------------------------
    # SKU microservice interaction
    # ------------------------------------------------------------------

    def lookup_manufacturers(self, skus: list[str]) -> dict[str, dict[str, str]]:
        """Call the SKU microservice in batches and return a mapping of
        SKU -> manufacturer details.

        Raises ``requests.RequestException`` if the service is unavailable
        for all retry attempts.
        """
        all_results: dict[str, dict[str, str]] = {}

        for i in range(0, len(skus), self.config.batch_size):
            batch = skus[i : i + self.config.batch_size]
            batch_results = self._lookup_batch_with_retry(batch)
            all_results.update(batch_results)

        return all_results

    def _lookup_batch_with_retry(
        self, skus: list[str]
    ) -> dict[str, dict[str, str]]:
        """POST a batch of SKUs to the microservice with retry logic."""
        last_exc: Exception | None = None
        url = f"{self.config.sku_api_url}/sku/batch"

        for attempt in range(self.config.max_retries):
            try:
                resp = self._http.post(url, json={"skus": skus}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", {})
            except requests.RequestException as exc:
                last_exc = exc
                wait = self.config.retry_backoff_base ** attempt
                logger.warning(
                    "SKU batch lookup attempt %d/%d failed: %s – retrying in %.1fs",
                    attempt + 1,
                    self.config.max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise requests.RequestException(
            f"SKU batch lookup failed after {self.config.max_retries} retries"
        ) from last_exc

    # ------------------------------------------------------------------
    # Enrichment logic
    # ------------------------------------------------------------------

    def enrich_claims(self, claims_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Enrich claim records with manufacturer details.

        Returns a tuple of ``(enriched_df, retry_df)`` where *retry_df*
        contains claims that could not be enriched because the SKU
        microservice was unavailable.
        """
        # Collect unique SKUs from the claims
        unique_skus: list[str] = [
            row["sku"] for row in claims_df.select("sku").distinct().collect()
        ]

        # Attempt to look up manufacturer details
        try:
            manufacturer_map = self.lookup_manufacturers(unique_skus)
            service_available = True
        except requests.RequestException as exc:
            logger.error("SKU microservice unavailable: %s", exc)
            manufacturer_map = {}
            service_available = False

        if not service_available:
            # Queue ALL claims for retry – service is down
            enriched_df = self._add_empty_manufacturer_columns(
                self.spark.createDataFrame([], claims_df.schema)
            )
            return enriched_df, claims_df

        # Build a broadcast-friendly lookup from the map
        mfr_rows = []
        for sku, details in manufacturer_map.items():
            mfr_rows.append(
                (
                    sku,
                    details.get("manufacturer_id"),
                    details.get("manufacturer_name"),
                    details.get("product_category"),
                )
            )

        mfr_schema = StructType(
            [
                StructField("sku", StringType(), False),
                StructField("_manufacturer_id", StringType(), True),
                StructField("_manufacturer_name", StringType(), True),
                StructField("_product_category", StringType(), True),
            ]
        )

        if mfr_rows:
            mfr_df = self.spark.createDataFrame(mfr_rows, schema=mfr_schema)
        else:
            mfr_df = self.spark.createDataFrame([], schema=mfr_schema)

        # Left join claims with manufacturer data
        joined = claims_df.join(mfr_df, on="sku", how="left")

        # Derive enrichment columns
        enriched = joined.withColumn(
            "manufacturer_id", F.col("_manufacturer_id")
        ).withColumn(
            "manufacturer_name", F.col("_manufacturer_name")
        ).withColumn(
            "product_category", F.col("_product_category")
        ).withColumn(
            "manufacturer_unknown",
            F.when(F.col("_manufacturer_id").isNull(), F.lit(True)).otherwise(
                F.lit(False)
            ),
        ).drop(
            "_manufacturer_id", "_manufacturer_name", "_product_category"
        )

        # No retry claims when service is available (unknown SKUs are just flagged)
        empty_retry = self.spark.createDataFrame([], claims_df.schema)
        return enriched, empty_retry

    @staticmethod
    def _add_empty_manufacturer_columns(df: DataFrame) -> DataFrame:
        """Add manufacturer columns with null/false defaults to an empty DataFrame."""
        return (
            df.withColumn("manufacturer_id", F.lit(None).cast(StringType()))
            .withColumn("manufacturer_name", F.lit(None).cast(StringType()))
            .withColumn("product_category", F.lit(None).cast(StringType()))
            .withColumn("manufacturer_unknown", F.lit(True).cast(BooleanType()))
        )

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def _write_enriched(self, df: DataFrame) -> int:
        """Write enriched claims as Parquet partitioned by date. Returns row count."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_path = self._s3_uri(
            f"{self.config.enriched_prefix}claims/dt={today}"
        )
        df.write.mode("overwrite").parquet(output_path)
        return df.count()

    def _write_retry_queue(self, df: DataFrame) -> int:
        """Write unenriched claims to the retry S3 path. Returns row count."""
        if df.rdd.isEmpty():
            return 0
        output_path = self._s3_uri(self.config.retry_queue_prefix)
        df.write.mode("append").parquet(output_path)
        return df.count()

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self) -> EnrichmentSummary:
        """Execute the full enrichment flow and return a summary."""
        summary = EnrichmentSummary()

        # Read ingested claims from S3
        try:
            ingested_path = self._s3_uri(f"{self.config.ingested_prefix}claims/")
            claims_df = self.spark.read.parquet(ingested_path)
        except Exception as exc:
            msg = f"Failed to read ingested claims: {exc}"
            logger.error(msg)
            summary.errors.append(msg)
            summary.status = "failure"
            return summary

        # Enrich
        try:
            enriched_df, retry_df = self.enrich_claims(claims_df)
        except Exception as exc:
            msg = f"Enrichment failed: {exc}"
            logger.error(msg)
            summary.errors.append(msg)
            summary.status = "failure"
            return summary

        # Write enriched claims
        try:
            enriched_count = self._write_enriched(enriched_df)
            summary.claims_enriched = enriched_count
            summary.claims_unknown_manufacturer = (
                enriched_df.filter(F.col("manufacturer_unknown") == True).count()
            )
        except Exception as exc:
            msg = f"Failed to write enriched claims: {exc}"
            logger.error(msg)
            summary.errors.append(msg)

        # Write retry queue
        try:
            retry_count = self._write_retry_queue(retry_df)
            summary.claims_queued_for_retry = retry_count
        except Exception as exc:
            msg = f"Failed to write retry queue: {exc}"
            logger.error(msg)
            summary.errors.append(msg)

        # Determine status
        if summary.errors:
            summary.status = "partial_failure"
        elif summary.claims_queued_for_retry > 0:
            summary.status = "partial_failure"
        else:
            summary.status = "success"

        return summary


# ---------------------------------------------------------------------------
# Glue entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["S3_BUCKET", "SKU_API_URL"])
    spark = SparkSession.builder.appName("fraud-detection-enrichment").getOrCreate()

    config = EnrichmentConfig(
        s3_bucket=args["S3_BUCKET"],
        sku_api_url=args["SKU_API_URL"],
    )
    job = EnrichmentJob(config, spark)
    summary = job.run()

    logger.info("Enrichment complete: %s", summary)
    print(summary)

    spark.stop()
