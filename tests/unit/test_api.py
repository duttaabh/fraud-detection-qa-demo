"""Tests for the FastAPI API layer and S3DataStore."""

import json

import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from api.data_store import S3DataStore
from tests.conftest import TEST_BUCKET


# ---------------------------------------------------------------------------
# S3DataStore tests
# ---------------------------------------------------------------------------


class TestS3DataStore:
    def test_load_from_s3(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_fraud_results()
        assert result.total == 2

    def test_load_missing_files_returns_empty(self, s3_client):
        store = S3DataStore(s3_client, bucket=TEST_BUCKET)
        store.load()
        assert store.get_fraud_results().total == 0
        assert store.get_quality_results().total == 0
        assert store.get_audit_logs() == []

    def test_fraud_results_sorted_desc(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_fraud_results()
        scores = [i["fraud_score"] for i in result.items]
        assert scores == sorted(scores, reverse=True)

    def test_fraud_detail_found(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        detail = store.get_fraud_detail("CLAIM-00001")
        assert detail is not None
        assert detail["fraud_score"] == 0.85

    def test_fraud_detail_not_found(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        assert store.get_fraud_detail("NONEXISTENT") is None

    def test_quality_results_sorted_desc(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_quality_results()
        assert result.total == 1
        assert result.items[0]["manufacturer_id"] == "MFR-001"

    def test_quality_detail_found(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        detail = store.get_quality_detail("MFR-001")
        assert detail is not None
        assert detail["quality_score"] == 3.5

    def test_quality_detail_not_found(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        assert store.get_quality_detail("NONEXISTENT") is None

    def test_filter_by_manufacturer(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_fraud_results(filters={"manufacturer": "Acme Corp"})
        assert result.total == 1
        assert result.items[0]["manufacturer_name"] == "Acme Corp"

    def test_filter_by_date_range(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_fraud_results(filters={
            "date_from": "2024-01-01T00:00:00Z",
            "date_to": "2024-12-31T23:59:59Z",
        })
        assert result.total == 2

    def test_pagination(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        result = store.get_fraud_results(page=1, page_size=1)
        assert len(result.items) == 1
        assert result.total == 2
        assert result.total_pages == 2

    def test_audit_logs_unfiltered(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        logs = store.get_audit_logs()
        assert len(logs) == 1

    def test_audit_logs_filter_entity_type(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        logs = store.get_audit_logs(filters={"entity_type": "claim"})
        assert len(logs) == 1
        logs = store.get_audit_logs(filters={"entity_type": "manufacturer"})
        assert len(logs) == 0

    def test_audit_logs_filter_entity_id(self, s3_with_sample_results):
        store = S3DataStore(s3_with_sample_results, bucket=TEST_BUCKET)
        store.load()
        logs = store.get_audit_logs(filters={"entity_id": "CLAIM-00001"})
        assert len(logs) == 1
        logs = store.get_audit_logs(filters={"entity_id": "NONEXISTENT"})
        assert len(logs) == 0


# ---------------------------------------------------------------------------
# FastAPI endpoint tests
# ---------------------------------------------------------------------------


def _make_test_client(s3_client) -> TestClient:
    """Build a TestClient with a pre-loaded S3DataStore injected."""
    from api.routes import audit, fraud, quality
    from api.main import app

    store = S3DataStore(s3_client, bucket=TEST_BUCKET)
    store.load()

    app.dependency_overrides[fraud.get_store] = lambda: store
    app.dependency_overrides[quality.get_store] = lambda: store
    app.dependency_overrides[audit.get_store] = lambda: store

    return TestClient(app, raise_server_exceptions=False)


class TestFraudEndpoints:
    def test_list_flagged(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/fraud/flagged")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        # first item should have highest score
        assert body["items"][0]["fraud_score"] >= body["items"][-1]["fraud_score"]

    def test_list_flagged_with_manufacturer_filter(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/fraud/flagged", params={"manufacturer": "Beta Inc"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["items"][0]["manufacturer_name"] == "Beta Inc"

    def test_claim_detail_found(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/fraud/claims/CLAIM-00001")
        assert resp.status_code == 200
        body = resp.json()
        assert body["claim_id"] == "CLAIM-00001"
        assert "contributing_factors" in body

    def test_claim_detail_not_found(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/fraud/claims/NONEXISTENT")
        assert resp.status_code == 404

    def test_export_csv(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/fraud/export")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "CLAIM-00001" in resp.text


class TestQualityEndpoints:
    def test_list_flagged(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/quality/flagged")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1

    def test_manufacturer_detail_found(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/quality/manufacturers/MFR-001")
        assert resp.status_code == 200
        body = resp.json()
        assert body["manufacturer_name"] == "Acme Corp"
        assert "product_category_breakdown" in body

    def test_manufacturer_detail_not_found(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/quality/manufacturers/NONEXISTENT")
        assert resp.status_code == 404

    def test_export_csv(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/quality/export")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "Acme Corp" in resp.text


class TestAuditEndpoints:
    def test_list_audit_logs(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/audit/logs")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1

    def test_filter_by_entity_type(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/audit/logs", params={"entity_type": "claim"})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_filter_by_entity_type_no_match(self, s3_with_sample_results):
        client = _make_test_client(s3_with_sample_results)
        resp = client.get("/api/v1/audit/logs", params={"entity_type": "manufacturer"})
        assert resp.status_code == 200
        assert len(resp.json()) == 0
