"""AWS Lambda handler for SKU manufacturer lookup.

Supports:
- GET /sku/{sku_id}  — single SKU lookup
- POST /sku/batch    — batch lookup (body: {"skus": [...]})

The SKU catalog is loaded from S3 on cold start and cached for the
lifetime of the Lambda execution environment.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Catalog loading (cold-start, cached across invocations)
# ---------------------------------------------------------------------------

_catalog: dict[str, dict[str, str]] | None = None


def _load_catalog() -> dict[str, dict[str, str]]:
    """Load SKU catalog CSV from S3 into a dict keyed by SKU."""
    global _catalog
    if _catalog is not None:
        return _catalog

    bucket = os.environ.get("S3_BUCKET", "fraud-detection-demo")
    key = os.environ.get("SKU_CATALOG_KEY", "raw/sku_catalog.csv")

    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(body))
    _catalog = {}
    for row in reader:
        _catalog[row["sku"]] = {
            "sku": row["sku"],
            "manufacturer_id": row["manufacturer_id"],
            "manufacturer_name": row["manufacturer_name"],
            "product_category": row["product_category"],
            "product_name": row["product_name"],
        }

    logger.info("Loaded %d SKUs from s3://%s/%s", len(_catalog), bucket, key)
    return _catalog


# ---------------------------------------------------------------------------
# Request routing helpers
# ---------------------------------------------------------------------------


def _single_lookup(sku_id: str) -> dict[str, Any]:
    """Return manufacturer details for a single SKU, or 404."""
    catalog = _load_catalog()
    item = catalog.get(sku_id)
    if item is None:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": f"SKU '{sku_id}' not found"}),
        }
    return {"statusCode": 200, "body": json.dumps(item)}


def _batch_lookup(body: dict[str, Any]) -> dict[str, Any]:
    """Return manufacturer details for a list of SKUs."""
    skus = body.get("skus")
    if not isinstance(skus, list):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "'skus' must be a list"}),
        }

    catalog = _load_catalog()
    results: dict[str, dict[str, str]] = {}
    not_found: list[str] = []

    for sku in skus:
        item = catalog.get(sku)
        if item is not None:
            results[sku] = item
        else:
            not_found.append(sku)

    return {
        "statusCode": 200,
        "body": json.dumps({"results": results, "not_found": not_found}),
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Route incoming API Gateway proxy events to the appropriate handler."""
    http_method = event.get("httpMethod", "")
    path = event.get("path", "")
    path_params = event.get("pathParameters") or {}

    # Common response headers
    headers = {"Content-Type": "application/json"}

    try:
        if http_method == "GET" and path.startswith("/sku/"):
            sku_id = path_params.get("sku_id", path.rsplit("/", 1)[-1])
            result = _single_lookup(sku_id)
        elif http_method == "POST" and path == "/sku/batch":
            body = json.loads(event.get("body", "{}") or "{}")
            result = _batch_lookup(body)
        else:
            result = {
                "statusCode": 405,
                "body": json.dumps({"error": "Method not allowed"}),
            }
    except Exception:
        logger.exception("Unhandled error in SKU lookup handler")
        result = {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
        }

    result["headers"] = headers
    return result
