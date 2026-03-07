"""Audit log API routes."""

from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.data_store import S3DataStore

router = APIRouter(prefix="/api/v1/audit", tags=["audit"])


def get_store() -> S3DataStore:
    """Overridden at app startup via dependency_overrides."""
    raise RuntimeError("S3DataStore not initialized")


@router.get("/logs")
def list_audit_logs(
    entity_type: Optional[str] = Query(None),
    entity_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    store: S3DataStore = Depends(get_store),
):
    """Audit log entries with filters by entity type, entity ID, date range."""
    filters = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "date_from": date_from,
        "date_to": date_to,
    }
    return store.get_audit_logs(filters=filters)
