# Requirements Document

## Introduction

Replace the existing LLM-based (Bedrock batch inference) fraud detection and quality analysis pipeline with an unsupervised anomaly detection approach using Amazon SageMaker. The current pipeline uses Nova Pro to score individual claims for fraud and aggregate manufacturer data for quality concerns, but this approach is too slow and expensive at production scale (~160 million contracts, ~88 million claims). The new system uses statistical/ML-based unsupervised anomaly detection (e.g., Isolation Forest, Random Cut Forest) trained and run via SageMaker, eliminating the LLM dependency while preserving the existing ingestion, enrichment, API, and dashboard layers.

## Glossary

- **Pipeline**: The end-to-end batch processing system that ingests, enriches, scores, and surfaces warranty claim data
- **Anomaly_Detection_Model**: The unsupervised ML model (e.g., Isolation Forest or Random Cut Forest) trained on claim features to identify statistical outliers without labeled fraud data
- **Feature_Engineering_Job**: The Glue ETL job that transforms enriched claim data into numerical feature vectors suitable for the Anomaly_Detection_Model
- **SageMaker_Training_Job**: The SageMaker job that trains the Anomaly_Detection_Model on the engineered feature set
- **SageMaker_Batch_Transform**: The SageMaker batch transform job that runs inference on claim data using a trained Anomaly_Detection_Model
- **Anomaly_Score**: A numerical score (0.0 to 1.0) assigned to each claim indicating how anomalous the claim is relative to the overall population; higher values indicate greater anomaly
- **Quality_Aggregator**: The component that aggregates per-claim anomaly scores to the manufacturer level to produce manufacturer quality assessments
- **Results_Loader**: The component that reads SageMaker batch transform output, applies thresholds, and writes structured JSON results to S3
- **Fraud_Threshold**: A configurable score boundary (default 0.7) above which a claim is flagged as suspected fraud
- **Quality_Threshold**: A configurable score boundary (default 2.0 standard deviations) above which a manufacturer is flagged as a quality concern
- **Dashboard**: The React frontend that displays fraud and quality results to analysts
- **API**: The FastAPI backend that serves fraud, quality, and audit data to the Dashboard

## Requirements

### Requirement 1: Feature Engineering from Enriched Claims

**User Story:** As a data engineer, I want enriched claim data transformed into numerical feature vectors, so that the unsupervised anomaly detection model can process the data.

#### Acceptance Criteria

1. WHEN enriched claim Parquet data is available in S3, THE Feature_Engineering_Job SHALL read the enriched claims and produce a feature matrix as Parquet output in S3
2. THE Feature_Engineering_Job SHALL extract the following numerical and encoded features from each claim: claim_amount, days_between_contract_start_and_claim, claim_type (one-hot encoded), product_category (one-hot encoded), and manufacturer claim frequency
3. THE Feature_Engineering_Job SHALL normalize all continuous features to zero mean and unit variance using standard scaling
4. THE Feature_Engineering_Job SHALL persist the scaler parameters (mean and standard deviation per feature) to S3 so that the same transformation can be applied during inference
5. IF a claim record contains null or missing values in required feature columns, THEN THE Feature_Engineering_Job SHALL impute missing numerical values with the column median and missing categorical values with a dedicated "unknown" category
6. THE Feature_Engineering_Job SHALL write a feature metadata file to S3 containing the feature column names, their types, and the scaling parameters used

### Requirement 2: SageMaker Model Training

**User Story:** As a data scientist, I want to train an unsupervised anomaly detection model on the engineered features, so that the system can identify anomalous claims without labeled fraud data.

#### Acceptance Criteria

1. WHEN the Feature_Engineering_Job has completed and feature data is available in S3, THE SageMaker_Training_Job SHALL train an Anomaly_Detection_Model using the SageMaker built-in Random Cut Forest algorithm
2. THE SageMaker_Training_Job SHALL accept configurable hyperparameters including num_trees (default 100), num_samples_per_tree (default 256), and feature_dim matching the engineered feature count
3. WHEN training completes, THE SageMaker_Training_Job SHALL save the trained model artifact to a versioned S3 path under a models/ prefix
4. THE SageMaker_Training_Job SHALL log training metadata (training duration, record count, hyperparameters, model artifact S3 path) to a training manifest JSON file in S3
5. IF the SageMaker_Training_Job fails, THEN THE Pipeline SHALL log the failure details and report the error without proceeding to batch inference

### Requirement 3: SageMaker Batch Transform for Scoring

**User Story:** As a data engineer, I want to score all claims in batch using the trained model, so that every claim receives an anomaly score efficiently at production scale.

#### Acceptance Criteria

1. WHEN a trained Anomaly_Detection_Model artifact exists in S3, THE SageMaker_Batch_Transform SHALL run batch inference on the full feature dataset and produce per-claim anomaly scores
2. THE SageMaker_Batch_Transform SHALL accept configurable instance type (default ml.m5.xlarge) and instance count (default 2) parameters to scale for production data volumes
3. THE SageMaker_Batch_Transform SHALL produce output containing the original claim_id and the raw anomaly score for each claim
4. THE SageMaker_Batch_Transform SHALL complete scoring of 88 million claims within 4 hours using the configured instance fleet
5. IF the SageMaker_Batch_Transform encounters records that fail inference, THEN THE SageMaker_Batch_Transform SHALL log the failed record identifiers and continue processing remaining records

### Requirement 4: Anomaly Score Normalization and Fraud Flagging

**User Story:** As an analyst, I want anomaly scores normalized to a 0-1 range and claims flagged against a configurable threshold, so that I can interpret results consistently and focus on the highest-risk claims.

#### Acceptance Criteria

1. WHEN raw anomaly scores are produced by the SageMaker_Batch_Transform, THE Results_Loader SHALL normalize the scores to a 0.0–1.0 range using min-max scaling across the batch
2. THE Results_Loader SHALL flag each claim where the normalized Anomaly_Score exceeds the configured Fraud_Threshold as suspected fraud
3. THE Results_Loader SHALL produce a contributing_factors list for each flagged claim identifying which features contributed most to the anomaly score (top 3 features by deviation from population mean)
4. THE Results_Loader SHALL write fraud results as JSON to S3 in the same schema as the existing fraud_results.json (fields: claim_id, contract_id, fraud_score, is_suspected_fraud, contributing_factors, model_version, scored_at)
5. THE Results_Loader SHALL include the SageMaker model artifact version as the model_version field in each result record

### Requirement 5: Manufacturer Quality Aggregation

**User Story:** As an analyst, I want manufacturer-level quality assessments derived from per-claim anomaly scores, so that I can identify manufacturers with systemic quality issues.

#### Acceptance Criteria

1. WHEN per-claim fraud results are available, THE Quality_Aggregator SHALL group claims by manufacturer_id and compute aggregate statistics: mean anomaly score, claim count, repair claim rate, and per-category breakdown
2. THE Quality_Aggregator SHALL compute a quality_score for each manufacturer as the number of standard deviations the manufacturer's mean anomaly score is above the population mean
3. THE Quality_Aggregator SHALL flag each manufacturer where the quality_score exceeds the configured Quality_Threshold as a quality concern
4. THE Quality_Aggregator SHALL write quality results as JSON to S3 in the same schema as the existing quality_results.json (fields: manufacturer_id, manufacturer_name, total_claims, repair_claim_rate, quality_score, is_quality_concern, product_category_breakdown, sku_breakdown, model_version, scored_at)

### Requirement 6: Audit Trail

**User Story:** As a compliance officer, I want an audit trail of all fraud flags and quality flags, so that I can review the basis for each automated decision.

#### Acceptance Criteria

1. WHEN the Results_Loader flags a claim as suspected fraud, THE Results_Loader SHALL append an audit log entry with event_type "fraud_flag", the claim_id, the Anomaly_Score, the Fraud_Threshold, and the contributing_factors
2. WHEN the Quality_Aggregator flags a manufacturer as a quality concern, THE Quality_Aggregator SHALL append an audit log entry with event_type "quality_flag", the manufacturer_id, the quality_score, and the Quality_Threshold
3. THE Results_Loader SHALL write audit entries as JSON to S3 in the same schema as the existing audit_log.json (fields: event_type, entity_type, entity_id, model_version, score, details, created_at)

### Requirement 7: Pipeline Orchestration

**User Story:** As a data engineer, I want the pipeline stages to execute in the correct order with proper error handling, so that the end-to-end flow runs reliably.

#### Acceptance Criteria

1. THE Pipeline SHALL execute stages in order: data generation, ingestion, enrichment, feature engineering, SageMaker training, SageMaker batch transform, results loading
2. THE Pipeline SHALL support a --skip-training flag that reuses the most recent trained model artifact from S3 instead of retraining
3. THE Pipeline SHALL support a --stage flag to start execution from any named stage onward
4. IF any stage fails, THEN THE Pipeline SHALL log the failure, record the error in the pipeline result, and stop execution
5. THE Pipeline SHALL remove the JSONL preparation and Bedrock batch inference stages from the execution flow

### Requirement 8: Infrastructure as Code

**User Story:** As a DevOps engineer, I want the SageMaker resources and updated pipeline infrastructure defined in CDK, so that the environment is reproducible and version-controlled.

#### Acceptance Criteria

1. THE CDK Stack SHALL provision an IAM role for SageMaker with permissions to read/write the data S3 bucket and to create training jobs and batch transform jobs
2. THE CDK Stack SHALL remove the Bedrock batch inference IAM role and the JSONL preparation Glue job resources
3. THE CDK Stack SHALL retain the existing S3 data bucket, ingestion Glue job, enrichment Glue job, API Lambda, HTTP API Gateway, dashboard S3 bucket, and CloudFront distribution
4. THE CDK Stack SHALL add a new Glue job resource for the Feature_Engineering_Job with the Glue script deployed to S3

### Requirement 9: API and Dashboard Compatibility

**User Story:** As a frontend developer, I want the API response schema to remain unchanged, so that the existing React dashboard works without modification.

#### Acceptance Criteria

1. THE API SHALL continue to serve fraud results at the existing endpoint with the same response schema (FraudResult type: claim_id, contract_id, sku, fraud_score, is_suspected_fraud, contributing_factors, model_version, scored_at)
2. THE API SHALL continue to serve quality results at the existing endpoint with the same response schema (QualityResult type: manufacturer_id, manufacturer_name, total_claims, repair_claim_rate, quality_score, is_quality_concern, product_category_breakdown, sku_breakdown, model_version, scored_at)
3. THE API SHALL continue to serve audit log entries at the existing endpoint with the same response schema (AuditLogEntry type: event_type, entity_type, entity_id, model_version, score, details, created_at)
4. THE Dashboard SHALL require zero code changes to display results from the new anomaly detection pipeline

### Requirement 10: Synthetic Data Quality Tiers

**User Story:** As a data scientist, I want synthetic data with distinct manufacturer quality tiers, so that the anomaly detection model has clear signal separation and quality scoring produces meaningful results.

#### Acceptance Criteria

1. THE Data Generator SHALL designate approximately 10% of manufacturers as "problem" manufacturers that receive anomalous claims
2. THE Data Generator SHALL designate approximately 50% of manufacturers as "good quality" manufacturers with very low repair rates (~5%) and lower claim amounts
3. THE Data Generator SHALL treat the remaining ~40% of manufacturers as "neutral" with a normal mix of claim types and amounts
4. THE Data Generator SHALL produce 50% normal claims and 50% anomalous claims, with anomalous claims concentrated on problem manufacturer SKUs
5. THE Data Generator SHALL route approximately 40% of normal claims to good-quality manufacturer SKUs with claim types biased away from repair (replacement/refund only)

### Requirement 11: AI Reasoning with SKU Context

**User Story:** As an analyst, I want the AI reasoning to include SKU-level data for quality concerns, so that I can identify which specific products are driving repair issues.

#### Acceptance Criteria

1. THE Quality Reasoning Prompt SHALL include the top 10 SKUs by repair count for the manufacturer being analyzed
2. THE Quality Reasoning Prompt SHALL request SKU-level analysis as part of the structured response
3. THE Fraud Reasoning Prompt SHALL include the claim's SKU and manufacturer in the context provided to the model

### Requirement 12: Dashboard Authentication

**User Story:** As a security administrator, I want the dashboard protected by Cognito authentication, so that only authorized users can access fraud and quality data.

#### Acceptance Criteria

1. THE CDK Stack SHALL provision a Cognito User Pool with email sign-in, no self-sign-up, and a password policy requiring 8+ characters with uppercase, lowercase, and digits
2. THE CDK Stack SHALL provision a Cognito App Client configured for OAuth2 authorization code grant with PKCE
3. THE Dashboard SHALL redirect unauthenticated users to the Cognito Hosted UI for login
4. THE Dashboard SHALL support optional authentication — if Cognito environment variables are not set, the dashboard SHALL work without authentication
5. THE `launch_app.sh` script SHALL support a `--create-user PASSWORD` flag that creates or resets the demo user (`demo@example.com`) with the provided password using the Cognito admin API
