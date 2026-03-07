"""Shared pytest fixtures and Hypothesis strategies for the fraud detection pipeline."""

import json
from datetime import datetime, timezone

import boto3
import pytest
from hypothesis import strategies as st
from moto import mock_aws

from config import PipelineConfig

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEST_BUCKET = "test-fraud-detection"


@pytest.fixture
def pipeline_config():
    return PipelineConfig(s3_bucket=TEST_BUCKET)


# ---------------------------------------------------------------------------
# AWS mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client


@pytest.fixture
def s3_with_sample_results(s3_client):
    """Populate the test bucket with sample fraud, quality, and audit JSON files."""
    fraud_results = [
        {
            "claim_id": "CLAIM-00001",
            "contract_id": "CONTRACT-00123",
            "sku": "SKU-456",
            "manufacturer_name": "Acme Corp",
            "fraud_score": 0.85,
            "is_suspected_fraud": True,
            "contributing_factors": ["high claim amount", "repeat claimant"],
            "model_version": "nova-pro-v1",
            "scored_at": "2024-01-15T10:30:00Z",
        },
        {
            "claim_id": "CLAIM-00002",
            "contract_id": "CONTRACT-00456",
            "sku": "SKU-789",
            "manufacturer_name": "Beta Inc",
            "fraud_score": 0.45,
            "is_suspected_fraud": False,
            "contributing_factors": [],
            "model_version": "nova-pro-v1",
            "scored_at": "2024-01-15T10:30:00Z",
        },
    ]
    quality_results = [
        {
            "manufacturer_id": "MFR-001",
            "manufacturer_name": "Acme Corp",
            "total_claims": 450,
            "repair_claim_rate": 0.267,
            "quality_score": 3.5,
            "is_quality_concern": True,
            "product_category_breakdown": {
                "electronics": {"category": "electronics", "claim_count": 300, "repair_rate": 0.30},
            },
            "model_version": "nova-pro-v1",
            "scored_at": "2024-01-15T10:30:00Z",
        },
    ]
    audit_log = [
        {
            "event_type": "fraud_flag",
            "entity_type": "claim",
            "entity_id": "CLAIM-00001",
            "model_version": "nova-pro-v1",
            "score": 0.85,
            "details": {"contributing_factors": ["high claim amount"], "threshold": 0.7},
            "created_at": "2024-01-15T10:30:00Z",
        },
    ]

    prefix = "results/"
    for name, data in [
        ("fraud_results.json", fraud_results),
        ("quality_results.json", quality_results),
        ("audit_log.json", audit_log),
    ]:
        s3_client.put_object(
            Bucket=TEST_BUCKET,
            Key=f"{prefix}{name}",
            Body=json.dumps(data),
        )
    return s3_client


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

ALPHANUMERIC = st.characters(whitelist_categories=("L", "N"))

claim_ids = st.text(min_size=1, max_size=20, alphabet=ALPHANUMERIC).map(lambda s: f"CLAIM-{s}")
contract_ids = st.text(min_size=1, max_size=20, alphabet=ALPHANUMERIC).map(lambda s: f"CONTRACT-{s}")
sku_ids = st.text(min_size=1, max_size=20, alphabet=ALPHANUMERIC).map(lambda s: f"SKU-{s}")
manufacturer_ids = st.text(min_size=1, max_size=20, alphabet=ALPHANUMERIC).map(lambda s: f"MFR-{s}")

claim_types = st.sampled_from(["repair", "replacement", "refund"])
claim_statuses = st.sampled_from(["open", "closed", "pending"])
contract_types = st.sampled_from(["standard", "extended", "premium"])
product_categories = st.sampled_from(["electronics", "appliances", "automotive", "furniture"])

fraud_scores = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
quality_scores = st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)
thresholds = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)

timestamps = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2026, 1, 1),
)

claims_strategy = st.fixed_dictionaries({
    "claim_id": claim_ids,
    "contract_id": contract_ids,
    "sku": sku_ids,
    "claim_date": timestamps.map(lambda d: d.isoformat()),
    "claim_type": claim_types,
    "claim_amount": st.floats(min_value=0, max_value=100_000, allow_nan=False, allow_infinity=False),
    "status": claim_statuses,
    "description": st.text(min_size=0, max_size=200),
    "created_at": timestamps.map(lambda d: d.isoformat()),
    "updated_at": timestamps.map(lambda d: d.isoformat()),
})

manufacturers_strategy = st.fixed_dictionaries({
    "manufacturer_id": manufacturer_ids,
    "manufacturer_name": st.text(min_size=1, max_size=100),
    "product_category": product_categories,
})

bedrock_jsonl_records = st.fixed_dictionaries({
    "recordId": st.text(
        min_size=1, max_size=30,
        alphabet=st.characters(whitelist_categories=("L", "N", "Pd")),
    ),
    "modelInput": st.fixed_dictionaries({
        "messages": st.just([{
            "role": "user",
            "content": [{"type": "text", "text": "test prompt"}],
        }]),
        "inferenceConfig": st.fixed_dictionaries({
            "maxTokens": st.integers(min_value=1, max_value=4096),
        }),
    }),
})
