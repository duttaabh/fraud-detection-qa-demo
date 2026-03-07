"""S3DataStore — loads JSON result files from S3 into memory and provides query methods."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class PaginatedResult:
    items: list[dict]
    total: int
    page: int
    page_size: int
    total_pages: int


class S3DataStore:
    """Loads fraud_results.json, quality_results.json, audit_log.json from S3 into memory.

    Provides in-memory filtering, sorting, and pagination.
    """

    def __init__(self, s3_client, bucket: str, results_prefix: str = "results/"):
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = results_prefix
        self._fraud_results: list[dict] = []
        self._quality_results: list[dict] = []
        self._audit_log: list[dict] = []

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """Read result files from S3 into memory.

        Supports both JSON array and JSONL (newline-delimited JSON) formats.
        For fraud results, loads the truncated JSON file (max 10k records)
        to keep Lambda memory bounded.
        """
        self._fraud_results = self._load_json("fraud_results.json")
        self._quality_results = self._load_json("quality_results.json")
        self._audit_log = self._load_json("audit_log.json")

    def _load_json(self, filename: str) -> list[dict]:
        key = f"{self._prefix}{filename}"
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            raw = resp["Body"].read().decode("utf-8")
            # Try JSON array first
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    logger.info("Loaded %d records from s3://%s/%s", len(data), self._bucket, key)
                    return data
            except json.JSONDecodeError:
                pass
            # Fall back to JSONL
            data = []
            for line in raw.strip().splitlines():
                line = line.strip()
                if line:
                    data.append(json.loads(line))
            logger.info("Loaded %d records (JSONL) from s3://%s/%s", len(data), self._bucket, key)
            return data
        except self._s3.exceptions.NoSuchKey:
            logger.warning("File not found: s3://%s/%s — serving empty results", self._bucket, key)
            return []
        except Exception:
            logger.warning("Failed to load s3://%s/%s — serving empty results", self._bucket, key, exc_info=True)
            return []

    # -- filtering helpers ---------------------------------------------------

    @staticmethod
    def _matches_filters(item: dict, filters: dict) -> bool:
        """Return True if *item* satisfies all non-None filter predicates."""
        date_from = filters.get("date_from")
        date_to = filters.get("date_to")
        manufacturer = filters.get("manufacturer")
        category = filters.get("category")

        if date_from or date_to:
            scored_at = item.get("scored_at", "")
            try:
                item_dt = datetime.fromisoformat(scored_at.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                return False
            if date_from and item_dt < datetime.fromisoformat(date_from.replace("Z", "+00:00")):
                return False
            if date_to and item_dt > datetime.fromisoformat(date_to.replace("Z", "+00:00")):
                return False

        if manufacturer:
            item_mfr = item.get("manufacturer_name", "")
            if item_mfr != manufacturer:
                return False

        if category:
            # Fraud results: check product_category or contributing_factors context
            # Quality results: check product_category_breakdown keys
            breakdown = item.get("product_category_breakdown")
            if breakdown is not None:
                if category not in breakdown:
                    return False
            else:
                item_cat = item.get("product_category", "")
                if item_cat != category:
                    return False

        return True

    @staticmethod
    def _paginate(items: list[dict], page: int, page_size: int) -> PaginatedResult:
        total = len(items)
        total_pages = max(1, (total + page_size - 1) // page_size)
        start = (page - 1) * page_size
        end = start + page_size
        return PaginatedResult(
            items=items[start:end],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    # -- fraud queries -------------------------------------------------------

    def get_fraud_results(
        self,
        filters: dict | None = None,
        sort_by: str = "fraud_score",
        sort_desc: bool = True,
        page: int = 1,
        page_size: int = 50,
    ) -> PaginatedResult:
        filtered = [r for r in self._fraud_results if self._matches_filters(r, filters or {})]
        filtered.sort(key=lambda r: r.get(sort_by, 0), reverse=sort_desc)
        return self._paginate(filtered, page, page_size)

    def get_fraud_detail(self, claim_id: str) -> dict | None:
        for r in self._fraud_results:
            if r.get("claim_id") == claim_id:
                return r
        return None
    def get_fraud_score_bounds(self) -> dict:
        """Return min and max fraud_score across all loaded fraud results."""
        if not self._fraud_results:
            return {"min": 0.0, "max": 1.0}
        scores = [r.get("fraud_score", 0.0) for r in self._fraud_results]
        return {"min": round(min(scores), 2), "max": round(max(scores), 2)}

    # -- quality queries -----------------------------------------------------

    def get_quality_results(
        self,
        filters: dict | None = None,
        sort_by: str = "quality_score",
        sort_desc: bool = True,
        page: int = 1,
        page_size: int = 50,
    ) -> PaginatedResult:
        filtered = [r for r in self._quality_results if self._matches_filters(r, filters or {})]
        filtered.sort(key=lambda r: r.get(sort_by, 0), reverse=sort_desc)
        return self._paginate(filtered, page, page_size)

    def get_quality_detail(self, manufacturer_id: str) -> dict | None:
        for r in self._quality_results:
            if r.get("manufacturer_id") == manufacturer_id:
                return r
        return None
    def get_quality_score_bounds(self) -> dict:
        """Return min and max quality_score across all loaded quality results."""
        if not self._quality_results:
            return {"min": 0.0, "max": 1.0}
        scores = [r.get("quality_score", 0.0) for r in self._quality_results]
        return {"min": round(min(scores), 2), "max": round(max(scores), 2)}

    # -- audit queries -------------------------------------------------------

    def get_audit_logs(self, filters: dict | None = None) -> list[dict]:
        if not filters:
            return list(self._audit_log)

        results = []
        entity_type = filters.get("entity_type")
        entity_id = filters.get("entity_id")
        date_from = filters.get("date_from")
        date_to = filters.get("date_to")

        for entry in self._audit_log:
            if entity_type and entry.get("entity_type") != entity_type:
                continue
            if entity_id and entry.get("entity_id") != entity_id:
                continue
            if date_from or date_to:
                created = entry.get("created_at", "")
                try:
                    entry_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue
                if date_from and entry_dt < datetime.fromisoformat(date_from.replace("Z", "+00:00")):
                    continue
                if date_to and entry_dt > datetime.fromisoformat(date_to.replace("Z", "+00:00")):
                    continue
            results.append(entry)

        return results
