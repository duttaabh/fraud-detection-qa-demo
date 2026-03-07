# Implementation Plan: Unsupervised Anomaly Detection

## Overview

Replace the Bedrock LLM-based fraud detection pipeline with SageMaker Random Cut Forest unsupervised anomaly detection. Implementation proceeds bottom-up: config changes, then new modules (feature engineering, training, batch transform), then updated results loader, then pipeline orchestration, then infrastructure, and finally wiring and cleanup.

## Tasks

- [x] 1. Update configuration and remove Bedrock settings
  - [x] 1.1 Update `config.py` to add SageMaker configuration and remove Bedrock configuration
    - Add fields: `features_prefix`, `models_prefix`, `sagemaker_output_prefix`, `sagemaker_role_arn`, `sagemaker_instance_type`, `sagemaker_training_instance_count`, `sagemaker_transform_instance_count`, `num_trees`, `num_samples_per_tree`
    - Remove fields: `bedrock_input_prefix`, `bedrock_output_prefix`, `bedrock_model_id`, `bedrock_role_arn`, `bedrock_max_tokens`, `fraud_prompt_template`, `quality_prompt_template`
    - _Requirements: 2.2, 3.2, 7.5_

- [x] 2. Implement Feature Engineering Glue Job
  - [x] 2.1 Create `glue/feature_engineering_job.py` with `FeatureEngineeringConfig` and `FeatureEngineeringJob` class
    - Implement `compute_manufacturer_frequency`, `one_hot_encode`, `compute_days_feature`, `impute_missing`, `standard_scale`, and `run` methods
    - Read enriched Parquet from `s3://{bucket}/enriched/claims/`, produce feature matrix Parquet at `s3://{bucket}/features/claim_features.parquet`
    - Write scaler parameters JSON to `s3://{bucket}/features/metadata/scaler_params.json`
    - Write feature metadata JSON to `s3://{bucket}/features/metadata/feature_metadata.json`
    - Handle missing values: numerical nulls → column median, categorical nulls → "unknown" one-hot column
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [ ]* 2.2 Write property test for feature engineering completeness
    - **Property 1: Feature engineering produces complete feature vectors**
    - Use Hypothesis to generate random enriched claims DataFrames, verify output has one row per input and all expected feature columns
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 2.3 Write property test for standard scaling round-trip
    - **Property 2: Standard scaling round-trip via persisted parameters**
    - Generate random claims, run feature engineering, verify inverse transform recovers original values within floating-point tolerance
    - **Validates: Requirements 1.3, 1.4**

  - [ ]* 2.4 Write property test for missing value imputation
    - **Property 3: Missing value imputation produces no nulls**
    - Generate DataFrames with random nulls, verify output has zero nulls, numerical nulls replaced with median, categorical nulls produce "unknown" one-hot column
    - **Validates: Requirements 1.5**

  - [ ]* 2.5 Write property test for feature metadata consistency
    - **Property 4: Feature metadata consistency**
    - Run feature engineering, verify metadata column names match actual output columns, feature_dim matches column count, feature types are correct
    - **Validates: Requirements 1.6**

- [x] 3. Checkpoint - Feature engineering tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement SageMaker Training Module
  - [x] 4.1 Create `pipeline/sagemaker_training.py` with `SageMakerTrainingConfig` and `SageMakerTrainingJob` class
    - Implement `get_feature_dim`, `create_training_job`, `wait_for_training`, `write_training_manifest`, and `run` methods
    - Use SageMaker built-in Random Cut Forest algorithm
    - Accept configurable hyperparameters: `num_trees` (default 100), `num_samples_per_tree` (default 256), `feature_dim`
    - Save model artifact to versioned S3 path under `models/` prefix
    - Write training manifest JSON with job_name, model_artifact_s3_path, training_duration_seconds, record_count, hyperparameters, created_at
    - Handle training failures: raise RuntimeError with SageMaker error message
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ]* 4.2 Write property test for training manifest fields
    - **Property 5: Training manifest contains all required fields**
    - Generate random hyperparameters and record counts, verify manifest contains all required fields and hyperparameters match configured values
    - **Validates: Requirements 2.2, 2.4**

- [x] 5. Implement SageMaker Batch Transform Module
  - [x] 5.1 Create `pipeline/sagemaker_batch_transform.py` with `SageMakerBatchTransformConfig` and `SageMakerBatchTransform` class
    - Implement `create_model`, `create_transform_job`, `wait_for_transform`, and `run` methods
    - Accept configurable instance_type (default ml.m5.xlarge) and instance_count (default 2)
    - Produce per-claim CSV output with claim_id and anomaly_score at `s3://{bucket}/sagemaker/output/`
    - Handle transform failures: raise RuntimeError with SageMaker error message
    - _Requirements: 3.1, 3.2, 3.3, 3.5_

- [x] 6. Update Results Loader for SageMaker Output
  - [x] 6.1 Update `pipeline/results_loader.py` to read SageMaker output instead of Bedrock output
    - Update `ResultsLoaderConfig`: replace `bedrock_output_prefix` with `sagemaker_output_prefix`, add `features_prefix`, `enriched_prefix`
    - Add `normalize_scores(raw_scores: list[float]) -> list[float]` using min-max scaling; handle edge cases (all identical scores → 0.0, empty list → empty)
    - Add `compute_contributing_factors(claim_features: dict, scaler_params: dict) -> list[str]` returning top 3 features by absolute z-score deviation
    - Add `aggregate_manufacturer_quality(fraud_results, enriched_claims) -> list[ManufacturerQualityResult]` grouping by manufacturer, computing mean anomaly score, std-dev quality_score, repair_claim_rate, per-category breakdown
    - Replace `parse_fraud_output` with `parse_sagemaker_output` to read CSV, join with enriched claims, normalize scores, compute contributing factors
    - Remove `parse_quality_output` (replaced by `aggregate_manufacturer_quality`)
    - Preserve output schemas: FraudResult, ManufacturerQualityResult, AuditLogEntry unchanged
    - Generate audit log entries for flagged claims (event_type "fraud_flag") and flagged manufacturers (event_type "quality_flag")
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 5.1, 5.2, 5.3, 5.4, 6.1, 6.2, 6.3_

  - [ ]* 6.2 Write property test for min-max normalization bounds
    - **Property 6: Min-max normalization bounds**
    - Generate lists of positive floats with at least two distinct values, verify all normalized scores in [0.0, 1.0], min maps to 0.0, max maps to 1.0
    - **Validates: Requirements 4.1**

  - [ ]* 6.3 Write property test for fraud threshold flagging
    - **Property 7: Fraud threshold flagging**
    - Generate random normalized scores and thresholds in (0, 1), verify claim flagged iff score > threshold
    - **Validates: Requirements 4.2**

  - [ ]* 6.4 Write property test for contributing factors top-3
    - **Property 8: Contributing factors are top-3 by deviation**
    - Generate random feature vectors and scaler params, verify contributing_factors contains exactly 3 features with highest absolute z-score, ordered by descending deviation
    - **Validates: Requirements 4.3**

  - [ ]* 6.5 Write property test for fraud result schema completeness
    - **Property 9: Fraud result schema completeness**
    - Generate FraudResult lists, verify each serialized JSON has all required fields, fraud_score in [0.0, 1.0], model_version matches artifact path
    - **Validates: Requirements 4.4, 4.5**

  - [ ]* 6.6 Write property test for manufacturer quality aggregation
    - **Property 10: Manufacturer quality aggregation correctness**
    - Generate per-claim fraud results grouped by manufacturer, verify total_claims count, repair_claim_rate fraction, per-category breakdown sums to total
    - **Validates: Requirements 5.1**

  - [ ]* 6.7 Write property test for quality z-score
    - **Property 11: Quality score is z-score of manufacturer mean**
    - Generate manufacturer mean anomaly scores, verify quality_score equals (manufacturer_mean - population_mean) / population_std
    - **Validates: Requirements 5.2**

  - [ ]* 6.8 Write property test for quality threshold flagging
    - **Property 12: Quality threshold flagging**
    - Generate quality scores and thresholds, verify manufacturer flagged iff quality_score > threshold
    - **Validates: Requirements 5.3**

  - [ ]* 6.9 Write property test for audit entries matching flagged entities
    - **Property 13: Audit entries match flagged entities**
    - Generate fraud and quality results with various scores/thresholds, verify audit log has exactly one entry per flagged entity and none for unflagged
    - **Validates: Requirements 6.1, 6.2**

- [x] 7. Checkpoint - Results loader tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Update Pipeline Orchestrator
  - [x] 8.1 Update `run_pipeline.py` with new stages and CLI flags
    - Update `Stage` enum: replace JSONL_PREP and BEDROCK_BATCH with FEATURE_ENGINEERING, SAGEMAKER_TRAINING, SAGEMAKER_BATCH_TRANSFORM
    - Add `--skip-training` flag: reuse most recent model artifact from `models/` prefix by listing `models/*/manifest.json` and sorting by timestamp
    - Update `--stage` flag choices to: `data_generation`, `ingestion`, `enrichment`, `feature_engineering`, `training`, `transform`, `results`
    - Remove `--mock` and `--skip-bedrock` flags
    - Wire new stages: feature engineering → SageMaker training → SageMaker batch transform → results loading
    - Handle `--skip-training` with no existing model: raise FileNotFoundError
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 8.2 Write property test for pipeline stage ordering
    - **Property 14: Pipeline stage ordering with --stage flag**
    - Generate valid stage names, verify only stages from that point onward execute in defined order
    - **Validates: Requirements 7.3**

  - [ ]* 8.3 Write property test for pipeline halt on failure
    - **Property 15: Pipeline halts on stage failure**
    - Simulate stage failures, verify subsequent stages don't execute, error recorded in result, stages_completed excludes failed and later stages
    - **Validates: Requirements 7.4**

- [x] 9. Update CDK Infrastructure
  - [x] 9.1 Update `infra/stack.py` to add SageMaker resources and remove Bedrock resources
    - Add SageMaker IAM role with S3 read/write on data bucket and SageMaker permissions (CreateTrainingJob, CreateTransformJob, CreateModel, DescribeTrainingJob, DescribeTransformJob)
    - Add Feature Engineering Glue job resource with script deployed to S3
    - Remove `self.bedrock_role` (Bedrock batch inference IAM role)
    - Remove `self.jsonl_prep_job` (JSONL preparation Glue job)
    - Retain existing: S3 data bucket, ingestion Glue job, enrichment Glue job, API Lambda, HTTP API Gateway, dashboard S3 bucket, CloudFront
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [ ]* 9.2 Write unit tests for CDK stack changes
    - Synth the stack and verify: SageMaker role exists with correct permissions, Bedrock role removed, feature engineering Glue job exists, JSONL prep Glue job removed
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 10. Cleanup and wiring
  - [x] 10.1 Delete `pipeline/bedrock_batch.py` and `glue/jsonl_prep_job.py`
    - Remove old Bedrock batch inference module
    - Remove old JSONL preparation Glue job
    - _Requirements: 7.5, 8.2_

  - [x] 10.2 Update `launch_app.sh` to use new pipeline stages
    - Replace references to JSONL prep and Bedrock stages with feature engineering, training, and transform stages
    - Update CLI flag usage to match new `--skip-training` and `--stage` flags
    - _Requirements: 7.1_

  - [x] 10.3 Remove `tests/mock_bedrock.py` and update test imports
    - Delete the Bedrock mock helper
    - Update any test files that import from removed modules
    - _Requirements: 7.5_

- [x] 11. Final checkpoint - Full pipeline tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Scale pipeline for 160M contracts / 80M claims
  - [x] 12.1 Update `config.py` with production-scale defaults
    - Bump `sagemaker_transform_instance_count` from 2 to 5
    - Add `data_gen_chunk_size` (500,000) and `results_chunk_size` (100,000) fields
  - [x] 12.2 Rewrite `scripts/generate_data.py` for chunked streaming generation
    - Add `_S3MultipartWriter` class for streaming CSV to S3 via multipart upload
    - Generate contracts and claims in configurable chunks (default 500k rows) instead of building all rows in memory
    - Backward-compatible: small datasets still use simple in-memory upload
  - [x] 12.3 Rewrite `pipeline/sagemaker_training.py` `_prepare_training_csv()` for chunked Parquet→CSV
    - Read Parquet part files in 100k-row batches via pyarrow instead of loading entire DataFrame into pandas
    - Stream CSV output to S3 via multipart upload
    - Handle Spark-style partitioned Parquet directories (multiple part files)
  - [x] 12.4 Rewrite `pipeline/results_loader.py` for bounded-memory processing
    - Read enriched claims via pyarrow batches with only needed columns (not full DataFrame)
    - Quality aggregation uses streaming accumulators (sum/count per manufacturer) instead of storing all claim dicts
    - Write fraud results as JSONL + truncated 10k-record JSON for the API Lambda
    - Add `chunk_size` config field for batch processing control
  - [x] 12.5 Update `api/data_store.py` to support JSONL format
    - Load both JSON array and JSONL (newline-delimited JSON) formats
    - Loads truncated JSON file (max 10k records) to keep Lambda memory bounded
  - [x] 12.6 Optimize `glue/feature_engineering_job.py` for large-scale Spark
    - `standard_scale()` computes mean/stddev for all columns in a single aggregation pass (was one pass per column)
    - `compute_manufacturer_frequency()` uses `F.broadcast()` join hint for the small frequency table
  - [x] 12.7 Scale `infra/stack.py` Glue and Lambda resources
    - Glue ingestion and enrichment: 2×G.1X → 20×G.2X
    - Glue feature engineering: 2×G.1X → 30×G.2X
    - API Lambda memory: 512MB → 1024MB

- [x] 13. Add per-step CLI flags to `launch_app.sh`
  - [x] 13.1 Add `--all`, `--datagen`, `--glue`, `--training`, `--transform`, `--results` flags
    - Default behavior (no flags) runs all steps, same as `--all`
    - Flags can be combined freely (e.g. `--training --transform`)
  - [x] 13.2 Add `--model-artifact PATH` flag for running `--transform` without `--training`
    - Falls back to `/tmp/model_artifact_path.txt` from a prior training run if not provided
  - [x] 13.3 Add `-h` / `--help` flag with usage documentation
  - [x] 13.4 Add `--local-ui` flag to start dashboard Vite dev server locally
    - Fetches API Gateway URL from stack outputs and passes as `VITE_API_URL`
    - Can be combined with other flags (e.g. `--results --local-ui`)

- [x] 14. Update quality scoring to use repair claim rate
  - [x] 14.1 Change `aggregate_manufacturer_quality()` in `pipeline/results_loader.py`
    - Quality score is now the z-score of each manufacturer's repair claim rate across all manufacturers (was z-score of mean anomaly score)
    - Manufacturers with disproportionately high repair rates relative to peers are flagged as quality concerns
    - `is_quality_concern` triggers when repair rate z-score exceeds `quality_threshold` (default 2.0)

- [x] 15. Fix API Lambda missing dependencies
  - [x] 15.1 Create `api/requirements.txt` with Lambda-only deps (`fastapi`, `mangum`)
  - [x] 15.2 Add `ILocalBundling` to `infra/stack.py` for Docker-free Lambda packaging
    - Runs `pip install` with `--platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:` to get Linux-compatible wheels on macOS
    - Copies `api/` and `config.py` into the asset output
    - Falls back to Docker bundling if local bundling fails
  - [x] 15.3 Update CORS `allow_origins` to `["*"]` in `api/main.py`
    - Was restricted to `localhost` origins, blocking the CloudFront-hosted dashboard

- [x] 16. Enhance dashboard with claim details and score filters
  - [x] 16.1 Add claim detail fields to `FraudResult` in `pipeline/results_loader.py`
    - Added `claim_amount`, `claim_type`, `claim_date`, `product_category`, `status`, `description` to `FraudResult` dataclass
    - Updated enriched lookup in `parse_sagemaker_output` to read and populate the new fields
  - [x] 16.2 Update `dashboard/src/types.ts` with new `FraudResult` fields and score filter support
    - Added `claim_amount`, `claim_type`, `claim_date`, `product_category`, `status`, `description` to `FraudResult` interface
    - Added `score_min`, `score_max` to `Filters` interface
  - [x] 16.3 Add score range filter to `dashboard/src/components/Filters.tsx`
    - New optional `showScoreFilter` and `scoreLabel` props
    - Renders min/max number inputs when enabled
  - [x] 16.4 Update `dashboard/src/pages/FraudOverview.tsx`
    - Added Claim Type and Amount columns to the table
    - Enabled fraud score min/max filter
    - Client-side filtering on `score_min` / `score_max`
  - [x] 16.5 Update `dashboard/src/pages/FraudDetail.tsx` with full claim and contract info
    - Claim Details section: claim date, type, amount, status, description, product category
    - Contract & Product section: contract ID, SKU, manufacturer
    - Fraud Analysis section: score, suspected fraud flag, model version, scored at
  - [x] 16.6 Update `dashboard/src/pages/QualityOverview.tsx` with quality score filter
    - Enabled quality score min/max filter
  - [x] 16.7 Re-ran results loader and redeployed CDK stack
    - `bash launch_app.sh --results` to regenerate JSON with new fields
    - `cdk deploy` to push updated Lambda and dashboard

- [x] 17. Add Bedrock Nova Pro AI reasoning to detail pages
  - [x] 17.1 Add fraud reasoning API endpoint in `api/routes/fraud.py`
    - `GET /api/v1/fraud/claims/{claim_id}/reasoning` calls Bedrock Nova Pro (`amazon.nova-pro-v1:0`)
    - Passes claim details, contract info, anomaly score, and contributing factors to the model
    - Returns structured fraud analysis: risk assessment, red flags, mitigating factors, recommended actions
  - [x] 17.2 Add quality reasoning API endpoint in `api/routes/quality.py`
    - `GET /api/v1/quality/manufacturers/{manufacturer_id}/reasoning` calls Bedrock Nova Pro
    - Prompt emphasizes repair claim count as the primary quality signal (higher repairs = worse quality)
    - Includes per-category repair breakdown in the prompt context
    - Returns structured quality analysis: severity, repair pattern analysis, root cause hypotheses, recommended actions
  - [x] 17.3 Add `getFraudReasoning()` and `getQualityReasoning()` to `dashboard/src/api/client.ts`
  - [x] 17.4 Update `dashboard/src/pages/FraudDetail.tsx` with AI reasoning section
    - Auto-fetches reasoning on page load, shows "AI Reasoning (Amazon Nova Pro)" section
  - [x] 17.5 Update `dashboard/src/pages/QualityDetail.tsx` with AI reasoning section
    - Auto-fetches reasoning on page load, shows repair claim count alongside total claims
  - [x] 17.6 Update `dashboard/src/pages/QualityOverview.tsx` to show repair claims column
    - Table now shows "Repair Claims" (computed as `total_claims × repair_rate`) instead of "Total Claims"
  - [x] 17.7 Add Bedrock IAM permission to API Lambda in `infra/stack.py`
    - `bedrock:InvokeModel` scoped to `amazon.nova-pro-v1:0` foundation model ARN
  - [x] 17.8 Update README and tasks.md
    - Added Bedrock Nova Pro to architecture diagram, dashboard description, API endpoints table, and cost estimate

- [x] 18. Fix Bedrock content filter blocking AI reasoning
  - [x] 18.1 Add system prompts to establish business context for Bedrock Nova Pro
    - Fraud reasoning: system prompt identifies the model as a warranty claims analyst at an insurance company
    - Quality reasoning: system prompt identifies the model as a product quality assurance analyst
    - System messages passed via the `"system"` field in the Bedrock request (separate from user message)
  - [x] 18.2 Soften prompt language to avoid content filter triggers
    - Replaced "fraud" with "anomaly", "red flags" with "key observations", "suspicious" with "statistically unusual"
    - Reframed analysis sections with neutral, business-appropriate terminology
  - [x] 18.3 Add specific content filter exception handling
    - Catches `ValidationException` with "content filter" in the error message
    - Returns a graceful fallback message instead of a raw error

- [x] 19. Improve synthetic data with 50/50 normal vs anomalous claims
  - [x] 19.1 Update `scripts/generate_data.py` with mixed claim generation
    - 50% normal claims: moderate amounts ($50–$2k gaussian), evenly distributed types, full date range
    - 50% anomalous claims with four patterns: `high_amount` ($5k–$25k), `rapid_claim` (last 6 months), `repair_burst` (always repair type), `combo` (high amount + repair)
    - ~10% of manufacturers designated as "problem" manufacturers; anomalous claims concentrated on their SKUs
    - Gives RCF model clear separation and quality scoring a strong repair-rate signal

- [x] 20. Rename `run_local.sh` to `launch_app.sh`
  - [x] 20.1 Rename script file and update all references in README, tasks.md, and script help text

- [x] 21. Add smart skip for existing SageMaker artifacts in `launch_app.sh`
  - [x] 21.1 When running `--all`, check S3 for existing model and transform output before executing
    - Checks `models/*/manifest.json` for latest trained model — skips training if found
    - Checks `sagemaker/output/` for transform results — skips batch transform if found
    - Explicit `--training` or `--transform` flags always run regardless (skip only applies to `--all`)

- [x] 22. Add SKU data to dashboard and Bedrock reasoning prompts
  - [x] 22.1 Add SKU column to `dashboard/src/pages/FraudOverview.tsx`
    - SKU column between Manufacturer and Claim Type
  - [x] 22.2 Add SKU breakdown to quality results
    - `SkuStats` dataclass in `pipeline/results_loader.py` with sku, claim_count, repair_count, repair_rate
    - `sku_breakdown` field added to `ManufacturerQualityResult` and aggregated in `aggregate_manufacturer_quality()`
    - `SkuStats` and `sku_breakdown` added to `dashboard/src/types.ts` `QualityResult` interface
  - [x] 22.3 Add SKU breakdown table to `dashboard/src/pages/QualityDetail.tsx`
    - Shows per-SKU claim count, repair count, and repair rate sorted by repair count descending
  - [x] 22.4 Add top SKUs column to `dashboard/src/pages/QualityOverview.tsx`
    - Shows top 3 SKUs by repair count for each manufacturer
  - [x] 22.5 Add repair count column to QualityDetail category breakdown table
    - Computed as `Math.round(claim_count * repair_rate)`
  - [x] 22.6 Add SKU breakdown to quality Bedrock reasoning prompt in `api/routes/quality.py`
    - Top 10 SKUs by repair count included in prompt context
    - Added "SKU Analysis" step to the analysis instructions

- [x] 23. Add good-quality manufacturer tier to synthetic data generation
  - [x] 23.1 Update `scripts/generate_data.py` with three manufacturer tiers
    - ~10% "problem" manufacturers — receive anomalous claims (high amounts, repair bursts)
    - ~50% "good quality" manufacturers — very low repair rates (~5%), lower claim amounts, mostly replacement/refund
    - ~40% "neutral" manufacturers — normal mix of claims
    - ~40% of normal claims routed to good-quality manufacturer SKUs with repair-averse claim types
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

- [x] 24. Suppress pytest warnings
  - [x] 24.1 Add `filterwarnings` to `pyproject.toml` to suppress mangum DeprecationWarning
    - Filters `DeprecationWarning` from `mangum` about `asyncio.get_event_loop()`

- [x] 25. Fix SKU showing "unknown" in Quality Overview page
  - [x] 25.1 Add `sku` to enriched lookup columns in `results_loader.py` `run()` method
    - The `run()` method rebuilds `enriched_lookup` for quality aggregation but was missing `sku` in `needed_cols` and in the dict construction
    - Added `"sku"` to the parquet column read list and `"sku": str(row.get("sku", "")) or None` to the dict
    - Root cause: the quality aggregation enriched lookup was added before SKU breakdown was implemented, and wasn't updated when SKU support was added

- [x] 26. Add Cognito authentication to protect the dashboard
  - [x] 26.1 Add Cognito User Pool, App Client, Hosted UI domain, and demo user to `infra/stack.py`
    - User Pool with email sign-in, no self-sign-up, password policy (8+ chars, upper/lower/digits)
    - App Client configured for OAuth2 authorization code grant (PKCE, no client secret)
    - Hosted UI domain prefix: `fraud-detection-dashboard`
    - Demo user: `demo@example.com` (temporary password sent via email, or set manually via CLI)
    - Stack outputs: `CognitoUserPoolId`, `CognitoAppClientId`, `CognitoDomain`, `DemoUserNote`
  - [x] 26.2 Add `amazon-cognito-identity-js` to dashboard dependencies
  - [x] 26.3 Create `dashboard/src/auth.ts` with Cognito Hosted UI OAuth2 PKCE flow
    - Login/logout URL builders, code exchange, token storage in sessionStorage, JWT expiry check
    - Configured via `VITE_COGNITO_USER_POOL_ID`, `VITE_COGNITO_APP_CLIENT_ID`, `VITE_COGNITO_DOMAIN` env vars
    - Auth is optional — if env vars are not set, dashboard works without auth
  - [x] 26.4 Create `dashboard/src/components/AuthGate.tsx` wrapper component
    - Shows login page with "Sign in" button redirecting to Cognito Hosted UI
    - Handles OAuth callback code exchange
    - Shows user email and sign-out button when authenticated
    - Passes through to children when auth is not configured (backward compatible)
  - [x] 26.5 Wrap App with AuthGate in `dashboard/src/main.tsx`
  - [x] 26.6 Add auth styles to `dashboard/src/index.css`
  - [x] 26.7 Add Vite env type declarations for Cognito vars in `dashboard/src/vite-env.d.ts`
  - [x] 26.8 Add `--create-user PASSWORD` flag to `launch_app.sh`
    - Creates/resets demo Cognito user (`demo@example.com`) with password provided as CLI argument
    - Password is required — script exits with error if not provided
    - Checks if Cognito is deployed, creates user with `admin-create-user` (SUPPRESS message action), sets permanent password
    - Prints credentials to console
  - [x] 26.9 Pass Cognito env vars to Vite in `--local-ui` mode
    - When Cognito is deployed, passes `VITE_COGNITO_USER_POOL_ID`, `VITE_COGNITO_APP_CLIENT_ID`, `VITE_COGNITO_DOMAIN` to the dev server

## Troubleshooting

- **Glue feature engineering fails with `Column 'start_date' does not exist`**: The enriched claims don't have `start_date` — it lives on the contracts table. Fixed by joining enriched claims with ingested contracts in `feature_engineering_job.py`.

- **SageMaker RCF training fails with `Rows 1-1000 have different fields than the expected size N+1`**: SageMaker built-in algorithms assume the first CSV column is a label by default. For unsupervised RCF (no label), set `ContentType` to `text/csv;label_size=0` in the training job's `InputDataConfig`.

- **API Lambda fails with `Runtime.ImportModuleError: No module named 'fastapi'`**: CDK's `Code.from_asset(".")` zips the project directory but does NOT install pip dependencies. Fixed by adding `ILocalBundling` to `infra/stack.py` that runs `pip install -r api/requirements.txt` with `--platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:` into the asset output, then copies `api/` and `config.py`. A separate `api/requirements.txt` lists only Lambda-needed deps (`fastapi`, `mangum`). The local bundling approach avoids requiring Docker on the build machine. Also updated CORS `allow_origins` to `["*"]` so the CloudFront-hosted dashboard can reach the API.

- **Bedrock Nova Pro returns "content filter" error**: The model's content filters block prompts or responses containing certain terms. Fixed by adding a system prompt that establishes the business context (warranty claims analyst / quality assurance analyst), softening prompt language (e.g. "anomaly" instead of "fraud"), and adding graceful fallback handling for content filter exceptions.

- **SKU shows "unknown" in Quality Overview page**: The `run()` method in `results_loader.py` rebuilds `enriched_lookup` for quality aggregation but was missing `sku` in the column read list and dict construction. The quality aggregation code (`aggregate_manufacturer_quality`) falls back to `"unknown"` when `enriched.get("sku")` returns `None`. Fixed by adding `"sku"` to `needed_cols` and storing it in the enriched dict. Requires re-running `bash launch_app.sh --results` to regenerate quality JSON in S3.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property tests validate the 15 correctness properties from the design document
- Checkpoints ensure incremental validation at key integration points
- The API and dashboard require zero changes (Requirement 9)
