"""Synthetic data generator for the fraud detection & quality analysis demo pipeline.

Supports large-scale generation (100M+ rows) via chunked CSV streaming
with S3 multipart upload to avoid holding all data in memory.
"""

from __future__ import annotations

import io
import random
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import boto3
import pandas as pd

from config import PipelineConfig


@dataclass
class DataGeneratorConfig:
    """Configuration for synthetic data generation."""

    num_contracts: int = 10_000
    num_claims: int = 10_000
    num_skus: int = 500
    num_manufacturers: int = 50
    s3_bucket: str = "fraud-detection-demo"
    s3_prefix: str = "raw/"
    chunk_size: int = 500_000

    @classmethod
    def from_pipeline_config(cls, cfg: PipelineConfig) -> DataGeneratorConfig:
        return cls(
            num_contracts=cfg.num_contracts,
            num_claims=cfg.num_claims,
            num_skus=cfg.num_skus,
            num_manufacturers=cfg.num_manufacturers,
            s3_bucket=cfg.s3_bucket,
            s3_prefix=cfg.raw_prefix,
            chunk_size=getattr(cfg, "data_gen_chunk_size", 500_000),
        )


# ---------------------------------------------------------------------------
# Constants used for realistic-looking synthetic data
# ---------------------------------------------------------------------------

CONTRACT_TYPES = ["standard", "extended", "premium"]
CLAIM_TYPES = ["repair", "replacement", "refund"]
CLAIM_STATUSES = ["open", "closed", "pending"]
PRODUCT_CATEGORIES = ["electronics", "appliances", "automotive", "furniture"]

_PRODUCT_NAMES: dict[str, list[str]] = {
    "electronics": ["Laptop", "Smartphone", "Tablet", "Monitor", "Headphones", "Camera", "Speaker", "Router"],
    "appliances": ["Refrigerator", "Washer", "Dryer", "Dishwasher", "Microwave", "Oven", "Vacuum", "Air Purifier"],
    "automotive": ["Battery", "Brake Pad", "Tire", "Alternator", "Starter Motor", "Radiator", "Spark Plug", "Filter"],
    "furniture": ["Sofa", "Desk", "Chair", "Bookshelf", "Bed Frame", "Dresser", "Table", "Cabinet"],
}

_CLAIM_DESCRIPTIONS = [
    "Screen replacement needed",
    "Device not powering on",
    "Unusual noise during operation",
    "Component failure after normal use",
    "Physical damage reported",
    "Intermittent connectivity issues",
    "Overheating during use",
    "Software malfunction causing hardware issue",
    "Water damage claim",
    "Wear and tear beyond expected levels",
]


def _random_date(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between *start* and *end*."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


class _S3MultipartWriter:
    """Streams CSV chunks to S3 via multipart upload to avoid holding all data in memory."""

    # S3 multipart minimum part size is 5 MB
    _MIN_PART_SIZE = 5 * 1024 * 1024

    def __init__(self, s3_client, bucket: str, key: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._key = key
        self._buffer = io.BytesIO()
        self._parts: list[dict] = []
        self._part_number = 1
        self._upload_id: str | None = None

    def start(self) -> None:
        resp = self._s3.create_multipart_upload(Bucket=self._bucket, Key=self._key)
        self._upload_id = resp["UploadId"]

    def write(self, data: bytes) -> None:
        self._buffer.write(data)
        if self._buffer.tell() >= self._MIN_PART_SIZE:
            self._flush_part()

    def finish(self) -> None:
        # Flush remaining buffer
        if self._buffer.tell() > 0:
            self._flush_part()
        if not self._parts:
            # Nothing was written — abort and do a simple put
            if self._upload_id:
                self._s3.abort_multipart_upload(
                    Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
                )
            self._s3.put_object(Bucket=self._bucket, Key=self._key, Body=b"")
            return
        self._s3.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            MultipartUpload={"Parts": self._parts},
        )

    def _flush_part(self) -> None:
        body = self._buffer.getvalue()
        self._buffer = io.BytesIO()
        resp = self._s3.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            PartNumber=self._part_number,
            Body=body,
        )
        self._parts.append({"ETag": resp["ETag"], "PartNumber": self._part_number})
        self._part_number += 1


class DataGenerator:
    """Generates synthetic warranty data and uploads it to S3.

    For large datasets (millions of rows), generates in chunks and streams
    to S3 via multipart upload to keep memory usage bounded.
    """

    def __init__(self, config: DataGeneratorConfig, s3_client=None) -> None:
        self.config = config
        self._s3 = s3_client or boto3.client("s3")
        self._sku_df: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # SKU catalog
    # ------------------------------------------------------------------

    def generate_sku_catalog(self) -> pd.DataFrame:
        """Generate a SKU catalog mapping SKUs to manufacturers and products."""
        rows: list[dict] = []
        manufacturers = [
            (f"MFR-{i:05d}", f"Manufacturer_{i}")
            for i in range(1, self.config.num_manufacturers + 1)
        ]

        for sku_idx in range(1, self.config.num_skus + 1):
            mfr_id, mfr_name = random.choice(manufacturers)
            category = random.choice(PRODUCT_CATEGORIES)
            product_name = random.choice(_PRODUCT_NAMES[category])
            rows.append(
                {
                    "sku": f"SKU-{sku_idx:05d}",
                    "manufacturer_id": mfr_id,
                    "manufacturer_name": mfr_name,
                    "product_category": category,
                    "product_name": product_name,
                }
            )

        self._sku_df = pd.DataFrame(rows)
        return self._sku_df

    # ------------------------------------------------------------------
    # Chunked contract generation
    # ------------------------------------------------------------------

    def _generate_contracts_chunk(self, start_idx: int, count: int, skus: list[str]) -> pd.DataFrame:
        now = datetime.utcnow()
        start_range = datetime(2020, 1, 1)
        rows: list[dict] = []
        for i in range(start_idx, start_idx + count):
            start_date = _random_date(start_range, now)
            end_date = start_date + timedelta(days=random.randint(365, 1825))
            created_at = start_date - timedelta(days=random.randint(0, 30))
            updated_at = _random_date(created_at, now)
            rows.append(
                {
                    "contract_id": f"CONTRACT-{i:09d}",
                    "customer_id": f"CUST-{random.randint(1, self.config.num_contracts):09d}",
                    "sku": random.choice(skus),
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "contract_type": random.choice(CONTRACT_TYPES),
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                }
            )
        return pd.DataFrame(rows)

    def generate_contracts(self) -> pd.DataFrame:
        """Generate synthetic contract records as a DataFrame (small datasets only)."""
        if self._sku_df is None:
            self.generate_sku_catalog()
        skus = self._sku_df["sku"].tolist()
        return self._generate_contracts_chunk(1, self.config.num_contracts, skus)

    # ------------------------------------------------------------------
    # Chunked claims generation
    # ------------------------------------------------------------------

    def _generate_claims_chunk(self, start_idx: int, count: int, skus: list[str]) -> pd.DataFrame:
        now = datetime.utcnow()
        start_range = datetime(2020, 1, 1)
        rows: list[dict] = []

        # Pick a small set of "problem" manufacturers that will get extra
        # repair claims — this gives the quality scoring a clear signal.
        if not hasattr(self, "_problem_mfr_skus"):
            self._setup_anomaly_profiles(skus)

        for i in range(start_idx, start_idx + count):
            is_anomalous = random.random() < 0.5  # 50 % anomalous

            if is_anomalous:
                row = self._generate_anomalous_claim(i, now, start_range)
            else:
                row = self._generate_normal_claim(i, skus, now, start_range)

            rows.append(row)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Anomaly profile helpers
    # ------------------------------------------------------------------

    def _setup_anomaly_profiles(self, skus: list[str]) -> None:
        """Pre-compute SKU subsets for anomalous and good-quality claim generation.

        Manufacturer tiers:
        - ~10% "problem" manufacturers — receive anomalous claims (high amounts, repair bursts)
        - ~50% "good quality" manufacturers — very low repair rates, only normal claims
        - ~40% "neutral" manufacturers — normal mix of claims
        """
        if self._sku_df is not None:
            all_mfrs = self._sku_df["manufacturer_id"].unique().tolist()
            random.shuffle(all_mfrs)

            n_problem = max(2, len(all_mfrs) // 10)
            n_good = len(all_mfrs) // 2

            self._problem_mfrs = set(all_mfrs[:n_problem])
            self._good_mfrs = set(all_mfrs[n_problem : n_problem + n_good])
            # Remaining are neutral — they get normal random claims

            self._problem_mfr_skus = (
                self._sku_df[self._sku_df["manufacturer_id"].isin(self._problem_mfrs)]["sku"]
                .tolist()
            )
            self._good_mfr_skus = (
                self._sku_df[self._sku_df["manufacturer_id"].isin(self._good_mfrs)]["sku"]
                .tolist()
            )
        else:
            self._problem_mfrs = set()
            self._good_mfrs = set()
            self._problem_mfr_skus = skus[:10]
            self._good_mfr_skus = skus[10:30]

        # Fallback if the filter produced nothing
        if not self._problem_mfr_skus:
            self._problem_mfr_skus = skus[:10]
        if not self._good_mfr_skus:
            self._good_mfr_skus = skus[10:30]

    def _generate_normal_claim(
        self, idx: int, skus: list[str], now: datetime, start_range: datetime
    ) -> dict:
        """Generate a typical, non-anomalous claim.

        Normal claims have:
        - Moderate amounts ($50 – $2,000)
        - Evenly distributed claim types
        - Reasonable timing spread

        ~40% of normal claims are routed to "good quality" manufacturer SKUs
        with claim types biased away from repair (replacement/refund only).
        """
        # Decide if this normal claim goes to a good-quality manufacturer
        use_good_mfr = random.random() < 0.4 and self._good_mfr_skus

        if use_good_mfr:
            sku = random.choice(self._good_mfr_skus)
            # Good manufacturers rarely have repair claims — only ~5% chance
            if random.random() < 0.05:
                claim_type = "repair"
            else:
                claim_type = random.choice(["replacement", "refund"])
            # Good manufacturers have lower claim amounts on average
            amount = round(random.gauss(300.0, 150.0), 2)
        else:
            sku = random.choice(skus)
            claim_type = random.choice(CLAIM_TYPES)
            amount = round(random.gauss(500.0, 300.0), 2)

        claim_date = _random_date(start_range, now)
        created_at = claim_date - timedelta(days=random.randint(0, 5))
        updated_at = _random_date(created_at, now)
        return {
            "claim_id": f"CLAIM-{idx:09d}",
            "contract_id": f"CONTRACT-{random.randint(1, self.config.num_contracts):09d}",
            "sku": sku,
            "claim_date": claim_date.strftime("%Y-%m-%d"),
            "claim_type": claim_type,
            "claim_amount": round(max(10.0, amount), 2),
            "status": random.choice(CLAIM_STATUSES),
            "description": random.choice(_CLAIM_DESCRIPTIONS),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        }

    def _generate_anomalous_claim(
        self, idx: int, now: datetime, start_range: datetime
    ) -> dict:
        """Generate an anomalous claim with one or more unusual patterns.

        Anomalous claims exhibit combinations of:
        - Very high dollar amounts ($5k – $25k)
        - Heavily skewed toward repair claim type
        - Concentrated on "problem" manufacturer SKUs
        - Claims filed very soon after contract start
        """
        # Pick an anomaly pattern (can combine multiple)
        pattern = random.choice(["high_amount", "rapid_claim", "repair_burst", "combo"])

        # Default to problem-manufacturer SKU for stronger quality signal
        sku = random.choice(self._problem_mfr_skus)

        if pattern == "high_amount":
            amount = round(random.uniform(5_000.0, 25_000.0), 2)
            claim_type = random.choice(CLAIM_TYPES)
        elif pattern == "rapid_claim":
            amount = round(random.uniform(100.0, 5_000.0), 2)
            claim_type = random.choice(CLAIM_TYPES)
        elif pattern == "repair_burst":
            amount = round(random.uniform(200.0, 8_000.0), 2)
            claim_type = "repair"  # always repair — drives quality signal
        else:  # combo — high amount + repair + problem manufacturer
            amount = round(random.uniform(8_000.0, 25_000.0), 2)
            claim_type = "repair"

        # Anomalous claims tend to cluster in recent dates
        recent_start = now - timedelta(days=180)
        claim_date = _random_date(recent_start, now)
        created_at = claim_date - timedelta(days=random.randint(0, 2))
        updated_at = _random_date(created_at, now)

        return {
            "claim_id": f"CLAIM-{idx:09d}",
            "contract_id": f"CONTRACT-{random.randint(1, self.config.num_contracts):09d}",
            "sku": sku,
            "claim_date": claim_date.strftime("%Y-%m-%d"),
            "claim_type": claim_type,
            "claim_amount": round(max(10.0, amount), 2),
            "status": random.choice(CLAIM_STATUSES),
            "description": random.choice(_CLAIM_DESCRIPTIONS),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        }

    def generate_claims(self) -> pd.DataFrame:
        """Generate synthetic claim records as a DataFrame (small datasets only)."""
        if self._sku_df is None:
            self.generate_sku_catalog()
        skus = self._sku_df["sku"].tolist()
        return self._generate_claims_chunk(1, self.config.num_claims, skus)

    # ------------------------------------------------------------------
    # S3 upload — small datasets (backward compatible)
    # ------------------------------------------------------------------

    def _upload_df(self, df: pd.DataFrame, key: str) -> None:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        self._s3.put_object(Bucket=self.config.s3_bucket, Key=key, Body=buf.getvalue())

    def upload_to_s3(self, contracts: pd.DataFrame, claims: pd.DataFrame, sku_catalog: pd.DataFrame) -> None:
        prefix = self.config.s3_prefix
        self._upload_df(contracts, f"{prefix}contracts.csv")
        self._upload_df(claims, f"{prefix}claims.csv")
        self._upload_df(sku_catalog, f"{prefix}sku_catalog.csv")

    # ------------------------------------------------------------------
    # Streaming upload — large datasets
    # ------------------------------------------------------------------

    def _stream_upload_chunked(
        self,
        key: str,
        total: int,
        chunk_generator,
        skus: list[str],
    ) -> int:
        """Generate data in chunks and stream to S3 via multipart upload.

        Returns the total number of rows written.
        """
        writer = _S3MultipartWriter(self._s3, self.config.s3_bucket, key)
        writer.start()

        chunk_size = self.config.chunk_size
        header_written = False
        rows_written = 0

        for offset in range(0, total, chunk_size):
            count = min(chunk_size, total - offset)
            chunk_df = chunk_generator(offset + 1, count, skus)
            csv_str = chunk_df.to_csv(index=False, header=not header_written)
            if header_written:
                # Strip the header line that pandas always writes
                csv_str = csv_str.split("\n", 1)[1] if "\n" in csv_str else csv_str
            writer.write(csv_str.encode("utf-8"))
            header_written = True
            rows_written += len(chunk_df)

        writer.finish()
        return rows_written

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self) -> dict[str, int]:
        """Generate all synthetic data and upload to S3.

        Uses chunked streaming for large datasets (>chunk_size rows)
        and simple in-memory upload for small datasets.
        """
        sku_catalog = self.generate_sku_catalog()
        skus = sku_catalog["sku"].tolist()
        prefix = self.config.s3_prefix

        # SKU catalog is always small — simple upload
        self._upload_df(sku_catalog, f"{prefix}sku_catalog.csv")

        # Contracts
        if self.config.num_contracts > self.config.chunk_size:
            contracts_count = self._stream_upload_chunked(
                f"{prefix}contracts.csv",
                self.config.num_contracts,
                self._generate_contracts_chunk,
                skus,
            )
        else:
            contracts = self.generate_contracts()
            self._upload_df(contracts, f"{prefix}contracts.csv")
            contracts_count = len(contracts)

        # Claims
        if self.config.num_claims > self.config.chunk_size:
            claims_count = self._stream_upload_chunked(
                f"{prefix}claims.csv",
                self.config.num_claims,
                self._generate_claims_chunk,
                skus,
            )
        else:
            claims = self.generate_claims()
            self._upload_df(claims, f"{prefix}claims.csv")
            claims_count = len(claims)

        return {
            "contracts": contracts_count,
            "claims": claims_count,
            "skus": len(sku_catalog),
        }
