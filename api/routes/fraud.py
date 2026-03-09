"""Fraud detection API routes."""

import csv
import io
import json
import logging
from typing import Optional

import boto3
from fastapi import APIRouter, Depends, Query, Response

from api.data_store import S3DataStore

router = APIRouter(prefix="/api/v1/fraud", tags=["fraud"])
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
    """List suspected fraud claims sorted by fraud_score descending."""
    result = store.get_fraud_results(filters=filters, page=page, page_size=page_size)
    return {
        "items": result.items,
        "total": result.total,
        "page": result.page,
        "page_size": result.page_size,
        "total_pages": result.total_pages,
    }


@router.get("/score-bounds")
def score_bounds(store: S3DataStore = Depends(get_store)):
    """Return min/max fraud_score across all data."""
    return store.get_fraud_score_bounds()


@router.get("/claims/{claim_id}")
def claim_detail(claim_id: str, store: S3DataStore = Depends(get_store)):
    """Claim detail with contributing factors."""
    detail = store.get_fraud_detail(claim_id)
    if detail is None:
        return Response(status_code=404, content='{"detail":"Claim not found"}', media_type="application/json")
    return detail


@router.get("/export")
def export_csv(
    filters: dict = Depends(_build_filters),
    store: S3DataStore = Depends(get_store),
):
    """CSV export of filtered fraud results."""
    result = store.get_fraud_results(filters=filters, page=1, page_size=100_000)
    items = result.items

    output = io.StringIO()
    if items:
        fieldnames = list(items[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            row = {k: (str(v) if isinstance(v, list) else v) for k, v in item.items()}
            writer.writerow(row)

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=fraud_results.csv"},
    )


@router.get("/claims/{claim_id}/reasoning")
def claim_reasoning(claim_id: str, store: S3DataStore = Depends(get_store)):
    """Generate AI reasoning for a fraud claim using Bedrock Nova Pro."""
    detail = store.get_fraud_detail(claim_id)
    if detail is None:
        return Response(status_code=404, content='{"detail":"Claim not found"}', media_type="application/json")

    system_msg, user_msg = _build_fraud_reasoning_prompt(detail)

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
            logger.warning("Bedrock content filter triggered for claim %s", claim_id)
            reasoning = (
                "AI analysis could not be generated for this claim due to content policy restrictions. "
                "Please review the anomaly score and contributing factors above for assessment details."
            )
        else:
            logger.error("Bedrock reasoning failed for %s: %s", claim_id, exc)
            reasoning = f"AI reasoning unavailable: {exc}"
    except Exception as exc:
        logger.error("Bedrock reasoning failed for %s: %s", claim_id, exc)
        reasoning = f"AI reasoning unavailable: {exc}"

    return {"claim_id": claim_id, "reasoning": reasoning}


def _build_fraud_reasoning_prompt(claim: dict) -> tuple[str, str]:
    """Build a system + user prompt pair for Bedrock Nova Pro to reason about a warranty claim anomaly."""
    system_msg = (
        "You are a warranty claims analyst at an insurance company. Your role is to review "
        "warranty claims that have been statistically flagged as anomalous by a machine learning model "
        "(Random Cut Forest). You provide professional, objective analysis to help claims adjusters "
        "understand why a claim was flagged. This is a legitimate business analytics use case for "
        "warranty claim review and quality assurance."
    )

    user_msg = f"""Review the following warranty claim that was flagged as statistically anomalous by our anomaly detection model. Provide an objective analysis of why this claim deviates from normal patterns.

## Claim Information
- Claim ID: {claim.get('claim_id', 'N/A')}
- Claim Date: {claim.get('claim_date', 'N/A')}
- Claim Type: {claim.get('claim_type', 'N/A')}
- Claim Amount: ${claim.get('claim_amount', 'N/A')}
- Status: {claim.get('status', 'N/A')}
- Description: {claim.get('description', 'N/A')}
- Product Category: {claim.get('product_category', 'N/A')}

## Contract & Product
- Contract ID: {claim.get('contract_id', 'N/A')}
- SKU: {claim.get('sku', 'N/A')}
- Manufacturer: {claim.get('manufacturer_name', 'N/A')}

## Anomaly Detection Results
- Anomaly Score: {claim.get('fraud_score', 'N/A')} (0.0 = typical, 1.0 = highly unusual)
- Contributing Factors: {', '.join(claim.get('contributing_factors', []))}

Provide your analysis in this structure:
1. Risk Level: High / Medium / Low with a one-line summary
2. Key Observations: What makes this claim statistically unusual based on the contributing factors
3. Contextual Factors: Any factors that provide normal explanations for the anomaly
4. Recommended Next Steps: What a claims adjuster should investigate
5. Summary: A brief paragraph with the overall assessment"""

    return system_msg, user_msg
