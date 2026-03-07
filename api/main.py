"""FastAPI application entry point for the Fraud Detection & Quality Analysis API."""

import logging
from contextlib import asynccontextmanager

import boto3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.data_store import S3DataStore
from api.routes import audit, fraud, quality
from config import PipelineConfig

logger = logging.getLogger(__name__)

_store: S3DataStore | None = None


def _get_store() -> S3DataStore:
    assert _store is not None, "S3DataStore not loaded"
    return _store


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load S3 data on startup."""
    global _store
    cfg = PipelineConfig()
    s3_client = boto3.client("s3")
    _store = S3DataStore(s3_client, bucket=cfg.s3_bucket, results_prefix=cfg.results_prefix)
    _store.load()
    logger.info("S3DataStore loaded for bucket=%s prefix=%s", cfg.s3_bucket, cfg.results_prefix)
    yield


app = FastAPI(title="Fraud Detection & Quality Analysis API", lifespan=lifespan)

# CORS — allow dashboard origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Wire dependency overrides so route modules resolve the shared store
app.dependency_overrides[fraud.get_store] = _get_store
app.dependency_overrides[quality.get_store] = _get_store
app.dependency_overrides[audit.get_store] = _get_store

# Include routers
app.include_router(fraud.router)
app.include_router(quality.router)
app.include_router(audit.router)

# Mangum adapter for AWS Lambda deployment via API Gateway
from mangum import Mangum

handler = Mangum(app)
