"""Manufacturer quality analysis API routes."""

import csv
import io
import json
import logging
from typing import Optional

import boto3
from fastapi import APIRouter, Depends, Query, Response

from api.data_store import S3DataStore

router = APIRouter(prefix="/api/v1/quality", tags=["quality"])
logger = logging.getLogger(__name__)


def get_store() -> S3DataStore:
    """Overridden at app startup via dependency_overrides."""
    raise RuntimeError("S3DataStore not initialized")


def _build_filters(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    manufacturer: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    score_min: Optional[float] = Query(None),
    score_max: Optional[float] = Query(None),
    reason: Optional[str] = Query(None),
) -> dict:
    return {
        "date_from": date_from,
        "date_to": date_to,
        "manufacturer": manufacturer,
        "category": category,
        "score_min": score_min,
        "score_max": score_max,
        "reason": reason,
    }


@router.get("/flagged")
def list_flagged(
    filters: dict = Depends(_build_filters),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    store: S3DataStore = Depends(get_store),
):
    """List quality concern manufacturers sorted by quality_score descending."""
    result = store.get_quality_results(filters=filters, page=page, page_size=page_size)
    return {
        "items": result.items,
        "total": result.total,
        "page": result.page,
        "page_size": result.page_size,
        "total_pages": result.total_pages,
    }


@router.get("/score-bounds")
def score_bounds(store: S3DataStore = Depends(get_store)):
    """Return min/max quality_score across all data."""
    return store.get_quality_score_bounds()


@router.get("/manufacturers/{manufacturer_id}")
def manufacturer_detail(manufacturer_id: str, store: S3DataStore = Depends(get_store)):
    """Manufacturer detail with product category breakdown."""
    detail = store.get_quality_detail(manufacturer_id)
    if detail is None:
        return Response(status_code=404, content='{"detail":"Manufacturer not found"}', media_type="application/json")
    return detail


@router.get("/export")
def export_csv(
    filters: dict = Depends(_build_filters),
    store: S3DataStore = Depends(get_store),
):
    """CSV export of filtered quality results."""
    result = store.get_quality_results(filters=filters, page=1, page_size=100_000)
    items = result.items

    output = io.StringIO()
    if items:
        fieldnames = list(items[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            row = {k: (str(v) if isinstance(v, dict) else v) for k, v in item.items()}
            writer.writerow(row)

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quality_results.csv"},
    )


@router.get("/manufacturers/{manufacturer_id}/reasoning")
def manufacturer_reasoning(manufacturer_id: str, store: S3DataStore = Depends(get_store)):
    """Generate AI reasoning for a manufacturer quality concern using Bedrock Nova Pro."""
    detail = store.get_quality_detail(manufacturer_id)
    if detail is None:
        return Response(status_code=404, content='{"detail":"Manufacturer not found"}', media_type="application/json")

    system_msg, user_msg = _build_quality_reasoning_prompt(detail)

    try:
        bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
        response = bedrock.invoke_model(
            modelId="amazon.nova-pro-v1:0",
            contentType="application/json",
            accept="application/json",
            body=json.dumps({
                "system": [{"text": system_msg}],
                "messages": [{"role": "user", "content": [{"text": user_msg}]}],
                "inferenceConfig": {"maxTokens": 1024, "temperature": 0.3},
            }),
        )
        body = json.loads(response["body"].read())
        reasoning = body.get("output", {}).get("message", {}).get("content", [{}])[0].get("text", "")
    except bedrock.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "ValidationException" and "content filter" in str(exc).lower():
            logger.warning("Bedrock content filter triggered for manufacturer %s", manufacturer_id)
            reasoning = (
                "AI analysis could not be generated for this manufacturer due to content policy restrictions. "
                "Please review the repair claim rate and quality score above for assessment details."
            )
        else:
            logger.error("Bedrock reasoning failed for %s: %s", manufacturer_id, exc)
            reasoning = f"AI reasoning unavailable: {exc}"
    except Exception as exc:
        logger.error("Bedrock reasoning failed for %s: %s", manufacturer_id, exc)
        reasoning = f"AI reasoning unavailable: {exc}"

    return {"manufacturer_id": manufacturer_id, "reasoning": reasoning}


def _build_quality_reasoning_prompt(mfr: dict) -> tuple[str, str]:
    """Build a system + user prompt pair for Bedrock Nova Pro to reason about a manufacturer quality concern."""
    total_claims = mfr.get("total_claims", 0)
    repair_rate = mfr.get("repair_claim_rate", 0)
    repair_count = int(total_claims * repair_rate) if total_claims else 0
    non_repair_count = total_claims - repair_count

    breakdown = mfr.get("product_category_breakdown", {})
    category_lines = []
    for cat_key, stats in breakdown.items():
        if isinstance(stats, dict):
            cat_repair_rate = stats.get("repair_rate", 0)
            cat_claims = stats.get("claim_count", 0)
            cat_repairs = int(cat_claims * cat_repair_rate) if cat_claims else 0
            category_lines.append(
                f"  - {stats.get('category', cat_key)}: {cat_claims} total claims, "
                f"{cat_repairs} repair claims ({cat_repair_rate * 100:.1f}% repair rate)"
            )

    categories_text = "\n".join(category_lines) if category_lines else "  No breakdown available"

    sku_breakdown = mfr.get("sku_breakdown", {})
    sku_lines = []
    sku_items = []
    for sku_key, sku_stats in sku_breakdown.items():
        if isinstance(sku_stats, dict):
            sku_items.append(sku_stats)
    # Sort by repair count descending, show top 10
    sku_items.sort(key=lambda s: s.get("repair_count", 0), reverse=True)
    for s in sku_items[:10]:
        sku_lines.append(
            f"  - {s.get('sku', 'N/A')}: {s.get('claim_count', 0)} claims, "
            f"{s.get('repair_count', 0)} repairs ({s.get('repair_rate', 0) * 100:.1f}% repair rate)"
        )
    skus_text = "\n".join(sku_lines) if sku_lines else "  No SKU data available"

    system_msg = (
        "You are a product quality assurance analyst at a warranty services company. Your role is to "
        "review manufacturers whose products have statistically elevated repair rates compared to peers. "
        "You provide professional, data-driven analysis to help quality teams prioritize supplier reviews. "
        "This is a legitimate business analytics use case for product quality monitoring."
    )

    user_msg = f"""Review the following manufacturer that was flagged for elevated product repair rates. Provide an objective, data-driven analysis.

## Manufacturer Overview
- Manufacturer: {mfr.get('manufacturer_name', 'N/A')}
- Manufacturer ID: {mfr.get('manufacturer_id', 'N/A')}

## Repair Claim Analysis
- Total warranty claims: {total_claims}
- Repair claims: {repair_count} ({repair_rate * 100:.1f}% of all claims)
- Non-repair claims (replacement/refund): {non_repair_count}
- Quality Score (z-score): {mfr.get('quality_score', 'N/A')} — measures how far above average this manufacturer's repair rate is compared to all other manufacturers. A score above 2.0 means their repair rate is more than 2 standard deviations above the mean.
- Flagged as Quality Concern: {mfr.get('is_quality_concern', 'N/A')}

## Repair Claims by Product Category
{categories_text}

## Top SKUs by Repair Count
{skus_text}

## Context
- A repair claim means the product failed and needed to be fixed — this is a direct signal of product reliability
- This manufacturer has {repair_count} repair claims out of {total_claims} total claims ({repair_rate * 100:.1f}%)
- Their repair rate is significantly higher than peer manufacturers (z-score: {mfr.get('quality_score', 'N/A')})

Provide your analysis:
1. Quality Assessment: Critical / Moderate / Low concern — summarize the severity based on the repair claim volume
2. Repair Pattern Analysis: What the repair count and rate tell us about product reliability
3. Category Breakdown: Which product categories have the most repairs and what that suggests
4. SKU Analysis: Which specific SKUs have the highest repair counts and what that indicates about product-level issues
5. Root Cause Hypotheses: Possible reasons for elevated repairs (e.g. manufacturing process issues, material quality, design considerations, quality control gaps)
6. Recommended Actions: Concrete steps — supplier review, batch investigation, enhanced incoming inspection, etc.
7. Summary: Brief overall assessment focused on the repair claim data"""

    return system_msg, user_msg
