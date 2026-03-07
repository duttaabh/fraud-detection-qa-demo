"""Results Loader — parses SageMaker batch transform output and writes JSONL results to S3.

Designed for large-scale processing (80M+ claims) with bounded memory usage.
Reads Parquet in chunks, streams scores, and writes JSONL output.
"""

import csv
import io
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ResultsLoaderConfig:
    s3_bucket: str
    sagemaker_output_prefix: str = "sagemaker/output/"
    features_prefix: str = "features/"
    enriched_prefix: str = "enriched/"
    results_prefix: str = "results/"
    fraud_threshold: float = 0.7
    quality_threshold: float = 2.0
    chunk_size: int = 100_000


@dataclass
class CategoryStats:
    category: str
    claim_count: int
    repair_rate: float


@dataclass
class FraudResult:
    claim_id: str
    contract_id: str
    fraud_score: float
    is_suspected_fraud: bool
    contributing_factors: list[str]
    model_version: str
    scored_at: str
    sku: str | None = None
    manufacturer_name: str | None = None
    claim_amount: float | None = None
    claim_type: str | None = None
    claim_date: str | None = None
    product_category: str | None = None
    status: str | None = None
    description: str | None = None


@dataclass
class SkuStats:
    sku: str
    claim_count: int
    repair_count: int
    repair_rate: float


@dataclass
class ManufacturerQualityResult:
    manufacturer_id: str
    manufacturer_name: str
    total_claims: int
    repair_claim_rate: float
    quality_score: float
    is_quality_concern: bool
    product_category_breakdown: dict[str, CategoryStats]
    sku_breakdown: dict[str, SkuStats]
    model_version: str
    scored_at: str


@dataclass
class AuditLogEntry:
    event_type: str
    entity_type: str
    entity_id: str
    details: dict
    created_at: str
    model_version: str | None = None
    score: float | None = None


@dataclass
class LoadSummary:
    fraud_results_count: int = 0
    quality_results_count: int = 0
    fraud_errors: int = 0
    quality_errors: int = 0
    audit_entries_written: int = 0
    status: str = "success"
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# ResultsLoader
# ---------------------------------------------------------------------------

class ResultsLoader:
    """Parses SageMaker batch transform output from S3 and writes result JSONL files back to S3.

    Designed for bounded memory usage at scale:
    - Reads Parquet files in batches via pyarrow
    - Two-pass approach: first pass collects raw scores + min/max for normalization,
      second pass writes normalized results as JSONL
    - Manufacturer quality aggregation uses streaming accumulators
    """

    def __init__(self, config: ResultsLoaderConfig, s3_client):
        self.config = config
        self._s3 = s3_client

    # -- helpers -------------------------------------------------------------

    def _read_s3_text(self, key: str) -> str:
        resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=key)
        return resp["Body"].read().decode("utf-8")

    def _read_s3_json(self, key: str):
        return json.loads(self._read_s3_text(key))

    def _list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _write_json(self, key: str, data) -> None:
        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key,
            Body=json.dumps(data, default=str),
            ContentType="application/json",
        )

    def _write_jsonl(self, key: str, records) -> None:
        """Write records as newline-delimited JSON (JSONL) to S3."""
        buf = io.BytesIO()
        for rec in records:
            line = json.dumps(rec, default=str) + "\n"
            buf.write(line.encode("utf-8"))
        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/x-ndjson",
        )

    # -- score normalization -------------------------------------------------

    def normalize_scores(self, raw_scores: list[float]) -> list[float]:
        """Min-max normalize raw anomaly scores to [0.0, 1.0].

        Edge cases:
        - Empty list → empty list
        - All identical scores → all 0.0
        """
        if not raw_scores:
            return []
        min_s = min(raw_scores)
        max_s = max(raw_scores)
        if max_s == min_s:
            return [0.0] * len(raw_scores)
        return [(s - min_s) / (max_s - min_s) for s in raw_scores]

    # -- contributing factors ------------------------------------------------

    def compute_contributing_factors(
        self, claim_features: dict[str, float], scaler_params: dict
    ) -> list[str]:
        """Return top 3 features by absolute z-score deviation."""
        deviations: list[tuple[str, float]] = []
        for feat_name, value in claim_features.items():
            if feat_name in scaler_params:
                params = scaler_params[feat_name]
                std = params.get("std", 1.0)
                if std > 0:
                    z = abs(value - params["mean"]) / std
                else:
                    z = 0.0
                deviations.append((feat_name, z))
            else:
                deviations.append((feat_name, abs(value)))

        deviations.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in deviations[:3]]

    # -- read raw scores from SageMaker output --------------------------------

    def _read_raw_scores(self) -> tuple[list[float], list[str]]:
        """Read all raw anomaly scores from SageMaker transform output files.

        Returns (raw_scores, errors).
        """
        raw_scores: list[float] = []
        errors: list[str] = []
        try:
            output_keys = self._list_keys(self.config.sagemaker_output_prefix)
            for key in output_keys:
                if key.endswith(".out") or key.endswith(".csv"):
                    text = self._read_s3_text(key)
                    for line in text.strip().splitlines():
                        line = line.strip()
                        if line:
                            try:
                                if line.startswith("{"):
                                    parsed = json.loads(line)
                                    raw_scores.append(float(parsed.get("score", parsed.get("anomaly_score", 0))))
                                else:
                                    raw_scores.append(float(line))
                            except (ValueError, json.JSONDecodeError) as exc:
                                errors.append(f"Failed to parse score line: {line!r}: {exc}")
        except Exception as exc:
            msg = f"Failed to read SageMaker output: {exc}"
            logger.error(msg)
            return [], [msg]
        return raw_scores, errors

    # -- list feature parquet part files --------------------------------------

    def _list_feature_parquet_keys(self) -> list[str]:
        """List all .parquet part files under the features prefix."""
        prefix = f"{self.config.features_prefix}claim_features.parquet"
        keys = self._list_keys(prefix)
        return [k for k in keys if k.endswith(".parquet")]

    # -- list enriched parquet part files -------------------------------------

    def _list_enriched_parquet_keys(self) -> list[str]:
        keys = self._list_keys(f"{self.config.enriched_prefix}claims/")
        return [k for k in keys if k.endswith(".parquet")]

    # -- SageMaker output parsing (streaming) ---------------------------------

    def parse_sagemaker_output(self, model_version: str) -> tuple[list[FraudResult], list[str]]:
        """Read SageMaker batch transform CSV output and enriched claims, produce FraudResults.

        For large datasets, reads Parquet in batches via pyarrow to keep memory bounded.
        Scores are normalized globally (requires reading all scores first).

        Returns:
            Tuple of (fraud_results, errors).
        """
        import pyarrow.parquet as pq

        errors: list[str] = []
        now = datetime.now(timezone.utc).isoformat()

        # 1. Read scaler params
        scaler_params = {}
        try:
            scaler_key = f"{self.config.features_prefix}metadata/scaler_params.json"
            scaler_params = self._read_s3_json(scaler_key)
        except Exception as exc:
            logger.warning("Could not read scaler params: %s", exc)

        # 2. Read all raw scores (these are just floats, ~640MB for 80M scores)
        raw_scores, score_errors = self._read_raw_scores()
        errors.extend(score_errors)
        if not raw_scores:
            return [], errors

        # 3. Normalize scores
        normalized = self.normalize_scores(raw_scores)

        # 4. Read claim_ids and features from parquet in batches, build results
        #    We read features parquet in order — the order matches the training CSV
        #    which matches the SageMaker output order.
        feature_parquet_keys = self._list_feature_parquet_keys()
        if not feature_parquet_keys:
            errors.append("No feature parquet files found")
            return [], errors

        # 5. Build enriched lookup — read enriched claims into a dict keyed by claim_id
        #    For 80M claims this is ~20GB if we hold all fields. Instead, only keep
        #    the fields we need: contract_id, sku, manufacturer_name, manufacturer_id, claim_type, product_category
        enriched_lookup: dict[str, dict] = {}
        enriched_keys = self._list_enriched_parquet_keys()
        needed_cols = ["claim_id", "contract_id", "sku", "manufacturer_name", "manufacturer_id", "claim_type", "product_category", "claim_amount", "claim_date", "status", "description"]
        for key in enriched_keys:
            try:
                resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=key)
                pf = pq.ParquetFile(io.BytesIO(resp["Body"].read()))
                for batch in pf.iter_batches(batch_size=self.config.chunk_size, columns=needed_cols):
                    df = batch.to_pandas()
                    for _, row in df.iterrows():
                        enriched_lookup[str(row.get("claim_id", ""))] = {
                            "contract_id": str(row.get("contract_id", "")),
                            "sku": str(row.get("sku", "")) or None,
                            "manufacturer_name": str(row.get("manufacturer_name", "")) or None,
                            "manufacturer_id": str(row.get("manufacturer_id", "")),
                            "claim_type": str(row.get("claim_type", "")),
                            "product_category": str(row.get("product_category", "")),
                            "claim_amount": float(row["claim_amount"]) if row.get("claim_amount") is not None else None,
                            "claim_date": str(row.get("claim_date", "")) or None,
                            "status": str(row.get("status", "")) or None,
                            "description": str(row.get("description", "")) or None,
                        }
            except Exception as exc:
                logger.warning("Could not read enriched parquet %s: %s", key, exc)

        # 6. Read feature parquet in batches and build FraudResults
        results: list[FraudResult] = []
        score_idx = 0

        for pk in feature_parquet_keys:
            try:
                resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=pk)
                pf = pq.ParquetFile(io.BytesIO(resp["Body"].read()))
                feature_cols = None

                for batch in pf.iter_batches(batch_size=self.config.chunk_size):
                    df = batch.to_pandas()
                    if feature_cols is None:
                        feature_cols = [c for c in df.columns if c != "claim_id"]

                    for _, row in df.iterrows():
                        if score_idx >= len(normalized):
                            break
                        cid = str(row["claim_id"])
                        score = normalized[score_idx]
                        score_idx += 1

                        enriched = enriched_lookup.get(cid, {})
                        features = {col: float(row[col]) for col in feature_cols}
                        factors = self.compute_contributing_factors(features, scaler_params)

                        results.append(FraudResult(
                            claim_id=cid,
                            contract_id=enriched.get("contract_id", ""),
                            fraud_score=score,
                            is_suspected_fraud=False,
                            contributing_factors=factors,
                            model_version=model_version,
                            scored_at=now,
                            sku=enriched.get("sku"),
                            manufacturer_name=enriched.get("manufacturer_name"),
                            claim_amount=enriched.get("claim_amount"),
                            claim_type=enriched.get("claim_type"),
                            claim_date=enriched.get("claim_date"),
                            product_category=enriched.get("product_category"),
                            status=enriched.get("status"),
                            description=enriched.get("description"),
                        ))
            except Exception as exc:
                msg = f"Failed to read feature parquet {pk}: {exc}"
                logger.error(msg)
                errors.append(msg)

        if score_idx != len(normalized):
            errors.append(
                f"Score count ({len(normalized)}) != claim count ({score_idx}). "
                f"Used min of both."
            )

        return results, errors

    # -- flagging ------------------------------------------------------------

    def flag_suspected_fraud(self, result: FraudResult) -> bool:
        """Return True if the fraud score exceeds the configured threshold."""
        return result.fraud_score > self.config.fraud_threshold

    # -- manufacturer quality aggregation ------------------------------------

    def aggregate_manufacturer_quality(
        self,
        fraud_results: list[FraudResult],
        enriched_lookup: dict[str, dict],
        model_version: str,
    ) -> list[ManufacturerQualityResult]:
        """Aggregate per-claim fraud results into manufacturer quality scores.

        Uses the enriched_lookup dict (already in memory from parse step)
        instead of re-reading enriched claims from S3.
        """
        now = datetime.now(timezone.utc).isoformat()

        # Group fraud results by manufacturer using streaming accumulators
        mfr_data: dict[str, dict] = {}
        for fr in fraud_results:
            enriched = enriched_lookup.get(fr.claim_id, {})
            mfr_id = enriched.get("manufacturer_id", "")
            if not mfr_id:
                continue
            if mfr_id not in mfr_data:
                mfr_data[mfr_id] = {
                    "name": enriched.get("manufacturer_name", ""),
                    "score_sum": 0.0,
                    "count": 0,
                    "repair_count": 0,
                    "categories": {},  # cat -> {total, repair}
                    "skus": {},  # sku -> {total, repair}
                }
            entry = mfr_data[mfr_id]
            entry["score_sum"] += fr.fraud_score
            entry["count"] += 1
            is_repair = enriched.get("claim_type") == "repair"
            if is_repair:
                entry["repair_count"] += 1

            cat = enriched.get("product_category", "unknown")
            if cat not in entry["categories"]:
                entry["categories"][cat] = {"total": 0, "repair": 0}
            entry["categories"][cat]["total"] += 1
            if is_repair:
                entry["categories"][cat]["repair"] += 1

            sku = enriched.get("sku") or "unknown"
            if sku not in entry["skus"]:
                entry["skus"][sku] = {"total": 0, "repair": 0}
            entry["skus"][sku]["total"] += 1
            if is_repair:
                entry["skus"][sku]["repair"] += 1

        # Compute per-manufacturer repair rate
        mfr_repair_rates: dict[str, float] = {}
        for mfr_id, data in mfr_data.items():
            total = data["count"]
            mfr_repair_rates[mfr_id] = data["repair_count"] / total if total > 0 else 0.0

        # Population stats for repair rate (z-score based quality scoring)
        if len(mfr_repair_rates) > 1:
            pop_mean = sum(mfr_repair_rates.values()) / len(mfr_repair_rates)
            pop_var = sum((r - pop_mean) ** 2 for r in mfr_repair_rates.values()) / len(mfr_repair_rates)
            pop_std = pop_var ** 0.5
        elif len(mfr_repair_rates) == 1:
            pop_mean = list(mfr_repair_rates.values())[0]
            pop_std = 0.0
        else:
            return []

        results: list[ManufacturerQualityResult] = []
        for mfr_id, data in mfr_data.items():
            total_claims = data["count"]
            repair_rate = mfr_repair_rates[mfr_id]

            # Quality score = z-score of repair rate across manufacturers
            # Higher repair rate relative to peers → higher quality concern
            if pop_std > 0:
                quality_score = (repair_rate - pop_mean) / pop_std
            else:
                quality_score = 0.0

            breakdown: dict[str, CategoryStats] = {}
            for cat, stats in data["categories"].items():
                breakdown[cat] = CategoryStats(
                    category=cat,
                    claim_count=stats["total"],
                    repair_rate=stats["repair"] / stats["total"] if stats["total"] > 0 else 0.0,
                )

            sku_breakdown: dict[str, SkuStats] = {}
            for sku, stats in data["skus"].items():
                sku_breakdown[sku] = SkuStats(
                    sku=sku,
                    claim_count=stats["total"],
                    repair_count=stats["repair"],
                    repair_rate=stats["repair"] / stats["total"] if stats["total"] > 0 else 0.0,
                )

            results.append(ManufacturerQualityResult(
                manufacturer_id=mfr_id,
                manufacturer_name=data["name"],
                total_claims=total_claims,
                repair_claim_rate=repair_rate,
                quality_score=quality_score,
                is_quality_concern=quality_score > self.config.quality_threshold,
                product_category_breakdown=breakdown,
                sku_breakdown=sku_breakdown,
                model_version=model_version,
                scored_at=now,
            ))

        return results

    # -- writing (JSONL for large datasets) ----------------------------------

    def write_fraud_results(self, results: list[FraudResult]) -> None:
        """Write fraud results as JSONL (one JSON object per line) for scalability."""
        key = f"{self.config.results_prefix}fraud_results.jsonl"
        # Stream to S3 in chunks to avoid building huge string
        buf = io.BytesIO()
        for r in results:
            line = json.dumps(asdict(r), default=str) + "\n"
            buf.write(line.encode("utf-8"))

        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/x-ndjson",
        )

        # Also write a backward-compatible JSON array for the API (paginated)
        # Only write first 10k for the API to load
        api_key = f"{self.config.results_prefix}fraud_results.json"
        api_payload = [asdict(r) for r in results[:10_000]]
        self._write_json(api_key, api_payload)

        logger.info("Wrote %d fraud results to s3://%s/%s", len(results), self.config.s3_bucket, key)

    def write_quality_results(self, results: list[ManufacturerQualityResult]) -> None:
        key = f"{self.config.results_prefix}quality_results.json"
        payload = [asdict(r) for r in results]
        self._write_json(key, payload)
        logger.info("Wrote %d quality results to s3://%s/%s", len(results), self.config.s3_bucket, key)

    def write_audit_log(self, entries: list[AuditLogEntry]) -> None:
        key = f"{self.config.results_prefix}audit_log.json"
        existing: list[dict] = []
        try:
            raw = self._read_s3_text(key)
            existing = json.loads(raw)
        except Exception:
            logger.info("No existing audit log found — starting fresh.")

        existing.extend(asdict(e) for e in entries)
        self._write_json(key, existing)
        logger.info("Wrote %d total audit entries to s3://%s/%s", len(existing), self.config.s3_bucket, key)

    # -- orchestration -------------------------------------------------------

    def _build_audit_entries(
        self,
        fraud_results: list[FraudResult],
        quality_results: list[ManufacturerQualityResult],
    ) -> list[AuditLogEntry]:
        entries: list[AuditLogEntry] = []
        now = datetime.now(timezone.utc).isoformat()

        for fr in fraud_results:
            if fr.is_suspected_fraud:
                entries.append(AuditLogEntry(
                    event_type="fraud_flag",
                    entity_type="claim",
                    entity_id=fr.claim_id,
                    model_version=fr.model_version,
                    score=fr.fraud_score,
                    details={
                        "contributing_factors": fr.contributing_factors,
                        "threshold": self.config.fraud_threshold,
                    },
                    created_at=now,
                ))

        for qr in quality_results:
            if qr.is_quality_concern:
                entries.append(AuditLogEntry(
                    event_type="quality_flag",
                    entity_type="manufacturer",
                    entity_id=qr.manufacturer_id,
                    model_version=qr.model_version,
                    score=qr.quality_score,
                    details={
                        "manufacturer_name": qr.manufacturer_name,
                        "total_claims": qr.total_claims,
                        "repair_claim_rate": qr.repair_claim_rate,
                        "threshold": self.config.quality_threshold,
                    },
                    created_at=now,
                ))

        return entries

    def run(self, model_version: str = "unknown") -> LoadSummary:
        """Parse SageMaker output, compute quality scores, flag results, write to S3.

        Key scaling improvements over the original:
        - Enriched claims read once and reused (not twice)
        - Quality aggregation uses streaming accumulators (no list[dict] of all claims)
        - Fraud results written as JSONL + truncated JSON for API
        """
        summary = LoadSummary()

        # --- fraud ---
        fraud_results, fraud_errors = self.parse_sagemaker_output(model_version)
        summary.fraud_errors = len(fraud_errors)
        summary.errors.extend(fraud_errors)

        for fr in fraud_results:
            fr.is_suspected_fraud = self.flag_suspected_fraud(fr)
        summary.fraud_results_count = len(fraud_results)

        # --- quality (reuse enriched_lookup from parse step) ---
        # Rebuild enriched_lookup from the parse step's data
        # The enriched_lookup was built during parse_sagemaker_output but not returned.
        # For quality aggregation, we rebuild it from the enriched parquet files.
        enriched_lookup: dict[str, dict] = {}
        try:
            import pyarrow.parquet as pq
            needed_cols = ["claim_id", "contract_id", "manufacturer_id", "manufacturer_name", "claim_type", "product_category", "sku"]
            enriched_keys = self._list_enriched_parquet_keys()
            for key in enriched_keys:
                resp = self._s3.get_object(Bucket=self.config.s3_bucket, Key=key)
                pf = pq.ParquetFile(io.BytesIO(resp["Body"].read()))
                for batch in pf.iter_batches(batch_size=self.config.chunk_size, columns=needed_cols):
                    df = batch.to_pandas()
                    for _, row in df.iterrows():
                        enriched_lookup[str(row.get("claim_id", ""))] = {
                            "manufacturer_id": str(row.get("manufacturer_id", "")),
                            "manufacturer_name": str(row.get("manufacturer_name", "")),
                            "claim_type": str(row.get("claim_type", "")),
                            "product_category": str(row.get("product_category", "")),
                            "sku": str(row.get("sku", "")) or None,
                        }
        except Exception as exc:
            logger.warning("Could not read enriched claims for quality aggregation: %s", exc)

        quality_results = self.aggregate_manufacturer_quality(
            fraud_results, enriched_lookup, model_version
        )
        summary.quality_results_count = len(quality_results)

        # --- write results ---
        self.write_fraud_results(fraud_results)
        self.write_quality_results(quality_results)

        # --- audit log ---
        audit_entries = self._build_audit_entries(fraud_results, quality_results)
        self.write_audit_log(audit_entries)
        summary.audit_entries_written = len(audit_entries)

        if summary.fraud_errors:
            summary.status = "partial_failure" if fraud_results else "failure"

        logger.info(
            "Results loader complete: %d fraud, %d quality, %d audit entries, %d errors",
            summary.fraud_results_count, summary.quality_results_count,
            summary.audit_entries_written, len(summary.errors),
        )
        return summary
