# Implementation Plan: Fraud Detection & Quality Analysis (AWS)

## Overview

This plan implements a demo batch-processing pipeline on AWS that generates synthetic warranty data, stores it in S3, processes it with AWS Glue, enriches it via API Gateway + Lambda, runs AI analysis through Amazon Bedrock batch inference, writes results as JSON files to S3, and exposes them via a FastAPI backend (reading from S3) and React dashboard.

## Tasks

- [x] 1. Set up project structure, dependencies, and shared configuration
  - [x] 1.1 Create project directory structure and configuration files
    - Create top-level directories: `scripts/`, `glue/`, `lambda/`, `pipeline/`, `api/`, `dashboard/`, `tests/`
    - Create `pyproject.toml` with dependencies: boto3, fastapi, uvicorn, hypothesis, pytest, moto, httpx, pandas
    - Create `dashboard/package.json` with dependencies: react, react-dom, typescript, vite, axios
    - Create shared config module (`config.py`) with dataclass-based configuration for S3 bucket, results prefix, Bedrock model ID, API Gateway URL, thresholds, etc.
    - _Requirements: All_

  - [x] 1.2 Create shared test fixtures and Hypothesis strategies
    - Create `tests/conftest.py` with shared pytest fixtures (moto S3 mock, sample JSON data in S3)
    - Define Hypothesis strategies for claims, fraud scores, thresholds, manufacturer data, and Bedrock JSONL records as specified in design
    - Set up moto decorators for S3 and Lambda mocking
    - _Requirements: All_

- [-] 2. Implement Synthetic Data Generation
  - [x] 2.1 Implement `scripts/generate_data.py` with `DataGenerator`
    - Ensure `requirements.txt` and `requirements-dev.txt` include all needed dependencies and install them into the `.venv` virtual environment via `pip install -r requirements-dev.txt`
    - Implement `DataGeneratorConfig` dataclass with num_contracts=10000, num_claims=10000, num_skus=500, num_manufacturers=50, s3_bucket, s3_prefix
    - Implement `generate_contracts()` producing 10,000 contract records as CSV with fields: contract_id, customer_id, sku, start_date, end_date, contract_type, created_at, updated_at
    - Implement `generate_claims()` producing 10,000 claim records as CSV with fields: claim_id, contract_id, sku, claim_date, claim_type, claim_amount, status, description, created_at, updated_at
    - Implement `generate_sku_catalog()` producing SKU catalog CSV with fields: sku, manufacturer_id, manufacturer_name, product_category, product_name
    - Implement `upload_to_s3()` to upload all CSVs to the configured S3 bucket
    - _Requirements: 1.1, 1.2_

  - [ ]* 2.2 Write unit tests for synthetic data generation
    - Test that generated contracts CSV has exactly 10,000 rows with valid schema
    - Test that generated claims CSV has exactly 10,000 rows with valid schema
    - Test that SKU catalog has correct manufacturer-to-SKU relationships
    - Test S3 upload using moto mock
    - _Requirements: 1.1, 1.2_

- [-] 3. Implement SKU Microservice (API Gateway + Lambda)
  - [x] 3.1 Implement Lambda function (`lambda/sku_lookup/handler.py`)
    - Implement handler for `GET /sku/{sku_id}` returning single SKU manufacturer details
    - Implement handler for `POST /sku/batch` accepting `{"skus": [...]}` and returning batch manufacturer details
    - Load SKU catalog from S3 (or environment-configured source)
    - Return 404 for unknown SKUs; include `not_found` list in batch responses
    - _Requirements: 2.1, 2.3_

  - [ ]* 3.2 Write unit tests for Lambda SKU lookup
    - Test single SKU lookup returns correct manufacturer details
    - Test batch SKU lookup returns results for known SKUs and not_found for unknown
    - Test handling of empty batch request
    - _Requirements: 2.1, 2.3_

- [x] 4. Implement Data Ingestion Glue Job
  - [x] 4.1 Implement `glue/ingestion_job.py` with `IngestionJob`
    - Implement `IngestionConfig` and `IngestionSummary` dataclasses
    - Implement `extract_contracts()` and `extract_claims()` reading CSVs from S3 using GlueContext/SparkSession
    - Implement incremental ingestion using high-watermark timestamp filtering on `updated_at`
    - Implement retry logic: up to 3 retries with exponential backoff on S3 read failure
    - Implement `run()` method that orchestrates extraction, writes Parquet output partitioned by date to S3, and produces `IngestionSummary`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ]* 4.2 Write property test: Incremental ingestion only extracts new or updated records
    - **Property 1: Incremental ingestion only extracts new or updated records**
    - **Validates: Requirements 1.3**

  - [ ]* 4.3 Write property test: Ingestion summary accuracy
    - **Property 2: Ingestion summary accuracy**
    - **Validates: Requirements 1.1, 1.2, 1.5**

  - [ ]* 4.4 Write unit tests for ingestion retry logic and error handling
    - Test 3-retry exponential backoff on S3 read failure
    - Test ingestion summary status values ("success", "partial_failure", "failure")
    - Use moto to mock S3 interactions
    - _Requirements: 1.4, 1.5_

- [-] 5. Implement Enrichment Glue Job
  - [x] 5.1 Implement `glue/enrichment_job.py` with `EnrichmentJob`
    - Implement `EnrichmentConfig` and `EnrichmentSummary` dataclasses
    - Implement `lookup_manufacturers()` to call API Gateway SKU microservice in configurable batches (up to 100 SKUs per request)
    - Implement `enrich_claims()` to join manufacturer details with claim records
    - Flag claims as `manufacturer_unknown` when SKU microservice returns no data
    - Queue unenriched claims to retry S3 path when SKU microservice is unavailable
    - Implement `run()` method that orchestrates enrichment and writes enriched Parquet to S3
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ]* 5.2 Write property test: Enrichment preserves claim data and adds manufacturer fields
    - **Property 3: Enrichment preserves claim data and adds manufacturer fields**
    - **Validates: Requirements 2.1, 2.2**

  - [ ]* 5.3 Write property test: Missing manufacturer data flags claim as unknown
    - **Property 4: Missing manufacturer data flags claim as unknown**
    - **Validates: Requirements 2.3**

  - [ ]* 5.4 Write unit tests for SKU microservice unavailability and retry queuing
    - Test that unenriched claims are queued to S3 retry path when SKU service is unavailable
    - Test that processing continues for other claims when one SKU lookup fails
    - _Requirements: 2.3, 2.4_

- [x] 6. Checkpoint - Ensure pipeline tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [-] 7. Implement JSONL Preparation Glue Job
  - [x] 7.1 Implement `glue/jsonl_prep_job.py` with `JSONLPrepJob`
    - Implement `JSONLPrepConfig` dataclass with prompt templates, maxTokens, S3 paths
    - Implement `format_fraud_record()` to build Bedrock Converse API format JSONL record for a single claim (recordId + modelInput with messages and inferenceConfig)
    - Implement `format_quality_record()` to build Bedrock Converse API format JSONL record for manufacturer aggregate data
    - Implement `build_fraud_jsonl()` to read enriched claims and write fraud.jsonl to S3
    - Implement `build_quality_jsonl()` to aggregate claims by manufacturer and write quality.jsonl to S3
    - Implement `run()` returning S3 paths for both JSONL files
    - _Requirements: 6.5_

  - [ ]* 7.2 Write property test: Bedrock JSONL input conforms to Converse API schema
    - **Property 13: Bedrock JSONL input conforms to Converse API schema**
    - **Validates: Requirements 6.5**

  - [ ]* 7.3 Write unit tests for JSONL preparation
    - Test fraud JSONL record has correct structure (recordId, modelInput.messages, modelInput.inferenceConfig)
    - Test quality JSONL record has correct structure
    - Test prompt templates include all required claim/manufacturer fields
    - _Requirements: 6.5_

- [x] 8. Implement Bedrock Batch Inference Orchestrator
  - [x] 8.1 Implement `pipeline/bedrock_batch.py` with `BedrockBatchOrchestrator`
    - Implement `BedrockBatchConfig` dataclass with model_id="amazon.nova-pro-v1:0", role_arn, S3 paths, thresholds
    - Implement `create_fraud_batch_job()` using `boto3.client("bedrock").create_model_invocation_job()` with correct inputDataConfig and outputDataConfig
    - Implement `create_quality_batch_job()` similarly for quality analysis
    - Implement `wait_for_job()` to poll `get_model_invocation_job()` until completion
    - _Requirements: 3.1, 4.1_

  - [ ]* 8.2 Write unit tests for Bedrock batch job creation and polling
    - Test batch job creation with correct parameters (model ID, role ARN, S3 URIs)
    - Test job polling logic (in-progress → completed, in-progress → failed)
    - Mock Bedrock client responses
    - _Requirements: 3.1, 4.1_

- [-] 9. Implement Results Loader (writes JSON to S3)
  - [x] 9.1 Implement `pipeline/results_loader.py` with `ResultsLoader`
    - Implement `ResultsLoaderConfig`, `FraudResult`, `ManufacturerQualityResult`, `CategoryStats`, `AuditLogEntry` dataclasses
    - Implement `parse_fraud_output()` to read Bedrock output JSONL from S3, extract fraud_score and contributing_factors from model response text
    - Implement `parse_quality_output()` to read Bedrock output JSONL from S3, extract quality_score and manufacturer analysis
    - Implement `flag_suspected_fraud()` using configurable threshold comparison
    - Implement `write_fraud_results()` to serialize fraud results as JSON and write `fraud_results.json` to S3
    - Implement `write_quality_results()` to serialize quality results as JSON and write `quality_results.json` to S3
    - Implement `write_audit_log()` to read existing `audit_log.json` from S3 (if any), append new entries, and write back
    - Implement `run()` orchestrating parse → flag → write JSON to S3 → write audit log
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 4.1, 4.2, 4.3, 4.4, 4.5, 6.3, 6.4, 7.1, 7.2_

  - [ ]* 9.2 Write property test: Fraud score is bounded between 0 and 1
    - **Property 5: Fraud score is bounded between 0 and 1**
    - **Validates: Requirements 3.1**

  - [ ]* 9.3 Write property test: Threshold-based flagging is consistent (fraud)
    - **Property 6: Threshold-based flagging is consistent**
    - **Validates: Requirements 3.2**

  - [ ]* 9.4 Write property test: Batch processing skips errors and continues
    - **Property 7: Batch processing skips errors and continues**
    - **Validates: Requirements 3.5, 6.3, 6.4**

  - [ ]* 9.5 Write property test: Repair claim rate calculation correctness
    - **Property 8: Repair claim rate calculation correctness**
    - **Validates: Requirements 4.1, 4.2**

  - [ ]* 9.6 Write property test: Category grouping covers all categories in data
    - **Property 9: Category grouping covers all categories in data**
    - **Validates: Requirements 4.5**

  - [ ]* 9.7 Write property test: Threshold-based flagging is consistent (quality)
    - **Property 6: Threshold-based flagging is consistent**
    - **Validates: Requirements 4.3**

  - [ ]* 9.8 Write property test: Audit log completeness for flagged items
    - **Property 14: Audit log completeness for flagged items**
    - **Validates: Requirements 7.1, 7.2, 7.4, 3.3, 4.4**

  - [ ]* 9.9 Write unit tests for results loading and error handling
    - Test parsing valid Bedrock fraud output
    - Test parsing valid Bedrock quality output
    - Test skipping malformed Bedrock output records with proper logging
    - Test score out-of-range rejection
    - Test JSON write to S3 produces valid JSON files
    - Test audit log append reads existing entries and adds new ones
    - _Requirements: 3.5, 6.3, 6.4_

- [x] 10. Checkpoint - Ensure pipeline and results loader tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Implement FastAPI Backend (reads from S3)
  - [x] 11.1 Implement S3DataStore (`api/data_store.py`)
    - Implement `S3DataStore` class that loads `fraud_results.json`, `quality_results.json`, `audit_log.json` from S3 into memory
    - Implement in-memory filtering by date range, manufacturer, product category
    - Implement in-memory sorting (default: score descending)
    - Implement in-memory pagination with configurable page size
    - Implement `get_fraud_detail()` and `get_quality_detail()` for single-item lookup
    - Implement `get_audit_logs()` with entity type/ID filtering
    - Handle missing S3 files gracefully (serve empty results, log warning)
    - _Requirements: 5.1, 5.2, 5.5_

  - [x] 11.2 Implement fraud API routes (`api/routes/fraud.py`)
    - `GET /api/v1/fraud/flagged` — list suspected fraud claims sorted by fraud_score descending, with pagination and filters (date range, manufacturer, product category)
    - `GET /api/v1/fraud/claims/{claim_id}` — claim detail with contributing factors
    - `GET /api/v1/fraud/export` — CSV export of filtered fraud results
    - _Requirements: 5.1, 5.3, 5.5, 5.6_

  - [x] 11.3 Implement quality API routes (`api/routes/quality.py`)
    - `GET /api/v1/quality/flagged` — list quality concern manufacturers sorted by quality_score descending, with pagination and filters
    - `GET /api/v1/quality/manufacturers/{id}` — manufacturer detail with product category breakdown
    - `GET /api/v1/quality/export` — CSV export of filtered quality results
    - _Requirements: 5.2, 5.4, 5.5, 5.6_

  - [x] 11.4 Implement audit API routes (`api/routes/audit.py`)
    - `GET /api/v1/audit/logs` — audit log entries with filters by entity type, entity ID, date range
    - _Requirements: 7.3_

  - [x] 11.5 Create FastAPI app entry point (`api/main.py`)
    - Wire all route modules together
    - Configure CORS for dashboard origin
    - Initialize S3DataStore on startup (load JSON files from S3)
    - Inject S3DataStore as dependency into route handlers
    - _Requirements: 5.1, 5.2_

  - [ ]* 11.6 Write property test: Flagged results are sorted by score descending
    - **Property 10: Flagged results are sorted by score descending**
    - **Validates: Requirements 5.1, 5.2**

  - [ ]* 11.7 Write property test: Filter results match filter criteria
    - **Property 11: Filter results match filter criteria**
    - **Validates: Requirements 5.5**

  - [ ]* 11.8 Write property test: CSV export round trip
    - **Property 12: CSV export round trip**
    - **Validates: Requirements 5.6**

  - [ ]* 11.9 Write unit tests for API endpoints and S3DataStore
    - Test S3DataStore loads JSON from moto-mocked S3
    - Test S3DataStore handles missing S3 files gracefully
    - Test claim detail response structure
    - Test manufacturer detail response structure with category breakdown
    - Test audit log API access and filtering
    - Test invalid filter parameter validation (400 response)
    - _Requirements: 5.3, 5.4, 7.3_

- [x] 12. Checkpoint - Ensure API tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Implement React Dashboard
  - [x] 13.1 Set up dashboard project with Vite, React, and TypeScript
    - Initialize Vite project in `dashboard/`
    - Install npm dependencies (react, react-dom, typescript, vite, axios, react-router-dom) via `npm install` in `dashboard/`
    - Create API client module (`dashboard/src/api/client.ts`) with typed functions for all backend endpoints
    - Define TypeScript interfaces for fraud results, quality results, and audit log entries
    - _Requirements: 5.1, 5.2_

  - [x] 13.2 Implement Fraud Overview page
    - Create table component displaying suspected fraud claims sorted by fraud_score descending
    - Implement click-through to claim detail view showing contributing fraud factors
    - _Requirements: 5.1, 5.3_

  - [x] 13.3 Implement Quality Overview page
    - Create table component displaying flagged manufacturers sorted by quality_score descending
    - Implement click-through to manufacturer detail view with product category breakdown
    - _Requirements: 5.2, 5.4_

  - [x] 13.4 Implement shared filter and export components
    - Create date range picker, manufacturer dropdown, and product category dropdown filter components
    - Wire filters to API query parameters on both Fraud and Quality pages
    - Create CSV export button component that triggers download from backend export endpoints
    - _Requirements: 5.5, 5.6_

  - [x] 13.5 Implement Audit Trail component
    - Create expandable audit log section on claim detail and manufacturer detail views
    - Fetch and display audit log entries for the selected entity
    - _Requirements: 7.3_

  - [x] 13.6 Wire dashboard routing and layout
    - Set up React Router with routes: `/fraud`, `/fraud/:claimId`, `/quality`, `/quality/:manufacturerId`
    - Create navigation layout with links to Fraud Overview and Quality Overview
    - _Requirements: 5.1, 5.2_

- [x] 14. Integration and end-to-end wiring
  - [x] 14.1 Create pipeline orchestration script
    - Create `run_pipeline.py` that executes the full batch flow: data generation → S3 upload → ingestion Glue job → enrichment Glue job → JSONL prep → Bedrock batch inference → results loading (writes JSON to S3)
    - Wire boto3 client creation and configuration loading
    - Ensure jobs are idempotent and can be re-run safely
    - _Requirements: 1.1, 1.2, 2.1, 3.1, 4.1_

  - [x] 14.2 Create mock Bedrock responses for local testing
    - Create sample Bedrock output JSONL files with deterministic fraud and quality scores
    - Create mock boto3 Bedrock client that returns pre-built responses
    - _Requirements: 3.1, 4.1_

  - [ ]* 14.3 Write integration tests for full pipeline flow
    - Test data generation → ingestion → enrichment → JSONL prep → results loading end-to-end with moto-mocked S3 and mock Bedrock output
    - Verify results JSON files are written correctly to S3
    - Verify audit_log.json entries are created for all flagged items and ingestion runs
    - Verify FastAPI S3DataStore can load the generated JSON files and serve correct results
    - _Requirements: 1.1, 1.2, 2.1, 2.2, 3.1, 3.2, 4.1, 4.3, 7.1, 7.2, 7.4_

- [x] 15. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Set up AWS CDK project and shared infrastructure
  - [x] 16.1 Initialize CDK project in `infra/` directory
    - Create `infra/` directory with `app.py`, `stack.py`, `__init__.py`
    - Create `infra/cdk.json` pointing to `infra/app.py` as the CDK app entry point
    - Create `infra/requirements.txt` with `aws-cdk-lib` and `constructs` dependencies
    - _Requirements: 8.8_

  - [x] 16.2 Define S3 data bucket in CDK stack
    - Create the primary S3 bucket for all pipeline data (raw, enriched, Bedrock I/O, results)
    - Set `RemovalPolicy.DESTROY` and `auto_delete_objects=True` for demo teardown
    - Export bucket name as a CloudFormation output
    - _Requirements: 8.1_

  - [x] 16.3 Define SKU lookup Lambda and REST API Gateway
    - Define Lambda function from `lambda/sku_lookup` source with Python 3.12 runtime
    - Set `S3_BUCKET` environment variable on the Lambda
    - Grant the Lambda read access to `raw/sku_catalog.csv` in the data bucket
    - Define REST API Gateway with `GET /sku/{sku_id}` and `POST /sku/batch` routes integrated with the Lambda
    - Export SKU API URL as a CloudFormation output
    - _Requirements: 8.2, 8.3_

  - [x] 16.4 Define Glue jobs for ingestion, enrichment, and JSONL preparation
    - Define a Glue IAM role with `AWSGlueServiceRole` managed policy and read/write access to the data bucket
    - Define three `CfnJob` resources (ingestion, enrichment, JSONL prep) with Glue 4.0, Python 3, G.1X workers
    - Pass `--S3_BUCKET` and `--SKU_API_URL` as default arguments to each job
    - Set script locations to `s3://<bucket>/glue-scripts/<job>.py`
    - _Requirements: 8.4_

  - [x] 16.5 Define Bedrock batch inference IAM role
    - Create IAM role with `bedrock.amazonaws.com` as trusted principal
    - Grant read/write on `bedrock/*` in the data bucket (covers both `bedrock/input/*` and `bedrock/output/*`)
    - Add policy statement allowing `bedrock:InvokeModel` on the Nova Pro foundation model ARN
    - Export role ARN as a CloudFormation output
    - **Note**: Using `bedrock/*` instead of separate `bedrock/input/*` and `bedrock/output/*` grants avoids S3 permission issues with Bedrock batch inference jobs
    - _Requirements: 8.5_

- [x] 17. Define FastAPI Lambda deployment and Dashboard hosting
  - [x] 17.1 Add Mangum adapter to FastAPI app
    - Add `mangum` to `requirements.txt`
    - Add `from mangum import Mangum` and `handler = Mangum(app)` to `api/main.py`
    - Ensure the existing FastAPI app works unchanged locally and via Lambda
    - _Requirements: 8.6_

  - [x] 17.2 Define FastAPI backend Lambda and HTTP API Gateway in CDK
    - Define Lambda function packaging the project root (excluding dashboard, infra, .venv, tests)
    - Set `api.main.handler` as the Lambda handler (Mangum entry point)
    - Set `S3_BUCKET` and `RESULTS_PREFIX` environment variables
    - Grant the Lambda read access to `results/*` in the data bucket
    - Define HTTP API Gateway with `/{proxy+}` catch-all route integrated with the Lambda
    - Configure CORS preflight to allow all origins, methods, and headers
    - Export HTTP API URL as a CloudFormation output
    - _Requirements: 8.6_

  - [x] 17.3 Define dashboard S3 bucket and CloudFront distribution in CDK
    - Create a separate S3 bucket for dashboard static files with `BlockPublicAccess.BLOCK_ALL`
    - Define CloudFront distribution with S3 origin access control, HTTPS redirect, and SPA error response (404 → /index.html)
    - Add `BucketDeployment` to upload `dashboard/dist/` to the dashboard bucket and invalidate CloudFront cache
    - Export CloudFront distribution URL as a CloudFormation output
    - _Requirements: 8.7_

  - [ ]* 17.4 Write CDK snapshot/assertion test for stack resources
    - Use `aws_cdk.assertions.Template` to verify the synthesized template contains all expected resources
    - Assert presence of: 2 S3 buckets, 2 Lambda functions, 1 REST API, 1 HTTP API, 3 Glue jobs, 2 IAM roles (Glue + Bedrock), 1 CloudFront distribution
    - Assert SKU Lambda has `S3_BUCKET` environment variable
    - Assert API Lambda has `S3_BUCKET` and `RESULTS_PREFIX` environment variables
    - **Property 15: CDK stack synthesizes all required resources**
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [x] 18. Deployment checkpoint
  - Verify `cdk synth` produces a valid CloudFormation template
  - Verify `cdk deploy` provisions all resources (manual verification by user)
  - Verify `cdk destroy` tears down all resources (manual verification by user)
  - _Requirements: 8.8, 8.9, 8.10_

## Deployment & Local Development

### 1. Deploy infrastructure to AWS
```bash
cdk deploy --app "PYTHONPATH=. .venv/bin/python infra/app.py" --require-approval never
```
This deploys: S3 bucket, SKU Lambda + REST API, 3 Glue jobs, Bedrock IAM role, FastAPI Lambda + HTTP API, Dashboard S3 + CloudFront.
Glue scripts (`glue/*.py`) are automatically uploaded to `s3://<bucket>/glue-scripts/` via CDK BucketDeployment.

**Important**: Each Glue script must have an `if __name__ == "__main__"` entrypoint that parses job arguments via `getResolvedOptions` and calls `run()`. Without this, Glue will execute the script but no work will be performed. Redeploy after any Glue script changes so updated files are uploaded to S3.

### 2. Run the data pipeline (against deployed AWS resources)
```bash
bash run_local.sh
```
This script:
1. Fetches stack outputs (`S3_BUCKET`, `BedrockRoleArn`, `HttpApiUrl`, `SkuApiUrl`)
2. Generates synthetic data and uploads to S3
3. Runs 3 Glue jobs sequentially (ingestion → enrichment → JSONL prep), polling until each completes
4. Runs Bedrock batch inference (fraud + quality) using real Bedrock API
5. Parses Bedrock output and writes results JSON to S3

### 3. Start the local dashboard (proxying to deployed API Gateway)
```bash
cd dashboard && VITE_API_URL="<HttpApiUrl from stack outputs>" npm run dev
```
Dashboard runs on `http://localhost:5173`, proxying API calls to the deployed HTTP API Gateway.

### 4. Tear down
```bash
cdk destroy --app "PYTHONPATH=. .venv/bin/python infra/app.py"
```

### Troubleshooting

- **Glue jobs succeed but produce no output**: Ensure each Glue script has an `if __name__ == "__main__"` entrypoint that calls `getResolvedOptions` and `run()`. Without it, Glue loads the script without executing anything.
- **Bedrock batch job fails with "No read permissions"**: The Bedrock IAM role needs `s3:GetObject` on the input prefix. Use `grant_read(role, "bedrock/*")` in CDK rather than narrow `bedrock/input/*` to avoid pattern mismatch issues.
- **JSONL prep fails with `ClassNotFoundException: DirectOutputCommitter`**: Don't use Spark RDD `saveAsTextFile` in Glue 4.0. Use `boto3.client("s3").put_object()` instead to write JSONL files directly.
- **CDK CLI not found**: The CDK CLI is a Node.js tool (`npm install -g aws-cdk`), not a Python module. Don't run it via `python -m aws_cdk`.
- **SKU Lambda returns 500 / `NoSuchBucket`**: The Lambda reads the bucket name from `S3_BUCKET` env var (set by CDK). Ensure the handler code uses `os.environ.get("S3_BUCKET", ...)` — not a different env var name like `SKU_CATALOG_BUCKET`.
- **Bedrock JSONL `Unrecognized field "type"`**: Bedrock batch Converse API content blocks use `{"text": "..."}` not `{"type": "text", "text": "..."}`. The `type` field is only used in the streaming/real-time Converse API, not batch.
- **Bedrock batch job fails with "less records than minimum of 100"**: Bedrock batch inference requires at least 100 records per JSONL file. Ensure `num_manufacturers >= 100` in `config.py` so the quality JSONL meets this minimum.
- **Bedrock `content_filtered` responses**: Some records get blocked by Bedrock's safety guardrails when prompts mention "fraud". Add a `system` prompt in the JSONL `modelInput` to establish legitimate business context (e.g., "You are a warranty claims analyst..."). This significantly reduces content filtering.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- AWS services (S3, Lambda, Bedrock) are mocked using moto and custom mocks in tests
- Python is used for pipeline, Glue jobs, Lambda, and API code; TypeScript is used for the React dashboard
- Amazon Bedrock batch inference uses Amazon Nova Pro (amazon.nova-pro-v1:0) with Converse API format
- **No database dependencies** — all results are stored as JSON files in S3 and loaded into memory by FastAPI
- Dependencies: boto3, fastapi, uvicorn, hypothesis, pytest, moto, httpx, pandas (no psycopg2-binary, no sqlalchemy)
- CDK dependencies (in `infra/requirements.txt`): aws-cdk-lib, constructs
- Mangum is added to `requirements.txt` for Lambda deployment of FastAPI
- Tasks 16–18 cover AWS CDK infrastructure provisioning and deployment verification
