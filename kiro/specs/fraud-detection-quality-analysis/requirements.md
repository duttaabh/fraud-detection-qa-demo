# Requirements Document

## Introduction

This feature integrates warranty data (~160 million Contracts, ~88 million Claims) and the SKU Microservice (manufacturer details) to power an AI-driven analysis pipeline. The pipeline serves two goals: detecting fraudulent activity in warranty claims and identifying manufacturers with abnormally high repair claim rates that signal product quality issues. This is a demo build intended to validate the approach before broader rollout.

## Glossary

- **Contract**: A warranty agreement record. The system holds approximately 160 million contracts.
- **Claim**: A warranty repair or replacement request record. The system holds approximately 88 million claims.
- **SKU_Microservice**: An existing microservice that provides manufacturer and product details for a given SKU.
- **Fraud_Detection_Engine**: The AI model component responsible for scoring claims for fraudulent activity.
- **Quality_Analysis_Engine**: The AI model component responsible for aggregating and analyzing repair claim rates per manufacturer to identify quality issues.
- **Data_Ingestion_Pipeline**: The component responsible for extracting, joining, and preparing data from the source systems and the SKU_Microservice for analysis.
- **Fraud_Score**: A numeric value between 0 and 1 assigned to a claim indicating the likelihood of fraud, where 1 represents the highest likelihood.
- **Quality_Score**: A numeric value assigned to a manufacturer representing the deviation of its repair claim rate from the expected baseline.
- **Dashboard**: The user-facing interface that displays fraud detection results and manufacturer quality analysis.
- **CDK_Stack**: The AWS Cloud Development Kit (Python) stack that defines all AWS infrastructure resources as code for this application.

## Requirements

### Requirement 1: Data Ingestion

**User Story:** As a data analyst, I want the system to ingest contract and claims data, so that the AI models have the raw data needed for fraud and quality analysis.

#### Acceptance Criteria

1. WHEN a data ingestion job is triggered, THE Data_Ingestion_Pipeline SHALL extract contract records from the source system.
2. WHEN a data ingestion job is triggered, THE Data_Ingestion_Pipeline SHALL extract claim records from the source system.
3. THE Data_Ingestion_Pipeline SHALL support incremental ingestion so that only new or updated records since the last ingestion run are extracted.
4. IF the connection to the source system fails during ingestion, THEN THE Data_Ingestion_Pipeline SHALL retry the connection up to 3 times with exponential backoff and log the failure if all retries are exhausted.
5. WHEN ingestion completes, THE Data_Ingestion_Pipeline SHALL produce an ingestion summary report containing the count of contracts extracted, the count of claims extracted, and the timestamp of the run.

### Requirement 2: Manufacturer Data Enrichment via SKU Microservice

**User Story:** As a data analyst, I want each claim to be enriched with manufacturer details from the SKU Microservice, so that claims can be grouped and analyzed by manufacturer.

#### Acceptance Criteria

1. WHEN a claim record is processed, THE Data_Ingestion_Pipeline SHALL call the SKU_Microservice to retrieve manufacturer details for the SKU associated with the claim.
2. THE Data_Ingestion_Pipeline SHALL join manufacturer details with the corresponding claim record and store the enriched record.
3. IF the SKU_Microservice returns no manufacturer data for a given SKU, THEN THE Data_Ingestion_Pipeline SHALL flag the claim as "manufacturer_unknown" and continue processing remaining claims.
4. IF the SKU_Microservice is unavailable, THEN THE Data_Ingestion_Pipeline SHALL queue the unenriched claims for retry and continue processing other claims.

### Requirement 3: Fraud Detection

**User Story:** As a fraud analyst, I want the system to score warranty claims for fraudulent activity using an AI model, so that I can investigate and act on suspicious claims.

#### Acceptance Criteria

1. WHEN enriched claim data is available, THE Fraud_Detection_Engine SHALL analyze each claim and assign a Fraud_Score between 0 and 1.
2. THE Fraud_Detection_Engine SHALL flag claims with a Fraud_Score above a configurable threshold as "suspected_fraud".
3. WHEN a claim is flagged as "suspected_fraud", THE Fraud_Detection_Engine SHALL record the contributing factors that led to the elevated Fraud_Score.
4. THE Fraud_Detection_Engine SHALL process the full set of enriched claims within 24 hours for a batch run.
5. IF the Fraud_Detection_Engine encounters a claim with missing or malformed data, THEN THE Fraud_Detection_Engine SHALL skip the claim, log the error with the claim identifier, and continue processing.

### Requirement 4: Manufacturer Quality Analysis

**User Story:** As a quality analyst, I want the system to identify manufacturers with abnormally high repair claim rates, so that I can flag potential product quality issues.

#### Acceptance Criteria

1. WHEN enriched claim data is available, THE Quality_Analysis_Engine SHALL calculate the repair claim rate for each manufacturer.
2. THE Quality_Analysis_Engine SHALL compute a Quality_Score for each manufacturer representing the deviation from the expected baseline repair claim rate.
3. THE Quality_Analysis_Engine SHALL flag manufacturers whose Quality_Score exceeds a configurable threshold as "quality_concern".
4. WHEN a manufacturer is flagged as "quality_concern", THE Quality_Analysis_Engine SHALL generate a summary containing the manufacturer name, total claims, repair claim rate, and Quality_Score.
5. THE Quality_Analysis_Engine SHALL group analysis results by product category in addition to manufacturer.

### Requirement 5: Dashboard and Reporting

**User Story:** As a fraud analyst or quality analyst, I want a dashboard that displays fraud detection results and manufacturer quality analysis, so that I can review findings and take action.

#### Acceptance Criteria

1. THE Dashboard SHALL display a list of claims flagged as "suspected_fraud", sorted by Fraud_Score in descending order.
2. THE Dashboard SHALL display a list of manufacturers flagged as "quality_concern", sorted by Quality_Score in descending order.
3. WHEN a user selects a flagged claim, THE Dashboard SHALL display the claim details including contributing fraud factors.
4. WHEN a user selects a flagged manufacturer, THE Dashboard SHALL display the manufacturer quality summary and a breakdown of claims by product category.
5. THE Dashboard SHALL allow users to filter results by date range, manufacturer, and product category.
6. THE Dashboard SHALL allow users to export the displayed results to CSV format.

### Requirement 6: Model Integration

**User Story:** As a developer, I want the system to integrate with the AI model, so that fraud detection and quality analysis use the agreed-upon model interface.

#### Acceptance Criteria

1. THE Fraud_Detection_Engine SHALL consume the AI model via a versioned API endpoint.
2. THE Quality_Analysis_Engine SHALL consume the AI model via a versioned API endpoint.
3. WHEN the AI model API returns an error, THE Fraud_Detection_Engine SHALL log the error, skip the affected claim, and continue processing.
4. WHEN the AI model API returns an error, THE Quality_Analysis_Engine SHALL log the error, skip the affected manufacturer batch, and continue processing.
5. THE Data_Ingestion_Pipeline SHALL format input data according to the specified schema before sending data to the AI model API.

### Requirement 7: Audit and Traceability

**User Story:** As a compliance officer, I want all fraud flags and quality flags to be auditable, so that I can trace how and when each flag was generated.

#### Acceptance Criteria

1. WHEN a claim is flagged as "suspected_fraud", THE Fraud_Detection_Engine SHALL record the timestamp, model version, Fraud_Score, and contributing factors in an audit log.
2. WHEN a manufacturer is flagged as "quality_concern", THE Quality_Analysis_Engine SHALL record the timestamp, model version, Quality_Score, and supporting data in an audit log.
3. THE Dashboard SHALL provide access to the audit log for each flagged item.
4. THE Data_Ingestion_Pipeline SHALL log the source, timestamp, and record count for each ingestion run in the audit log.

### Requirement 8: AWS Infrastructure Provisioning via CDK

**User Story:** As a developer, I want the entire AWS infrastructure for this application to be defined as code using AWS CDK (Python), so that the environment can be reproducibly deployed, updated, and torn down.

#### Acceptance Criteria

1. THE CDK_Stack SHALL define an S3 bucket for all pipeline data storage including raw CSVs, enriched data, Bedrock I/O, and results JSON files.
2. THE CDK_Stack SHALL define a Lambda function for the SKU lookup microservice, configured with the S3 bucket name as an environment variable and granted read access to the SKU catalog object in S3.
3. THE CDK_Stack SHALL define an API Gateway REST API with routes `GET /sku/{sku_id}` and `POST /sku/batch` integrated with the SKU lookup Lambda function.
4. THE CDK_Stack SHALL define AWS Glue jobs for ingestion, enrichment, and JSONL preparation, each configured with the S3 bucket name and API Gateway URL as job parameters.
5. THE CDK_Stack SHALL define an IAM role for Amazon Bedrock batch inference with permissions to read from and write to the designated S3 prefixes and to invoke the specified Bedrock model.
6. THE CDK_Stack SHALL define a Lambda function for the FastAPI backend using the Mangum adapter, with an API Gateway HTTP API routing all requests to the Lambda function.
7. THE CDK_Stack SHALL define an S3 bucket configured for static website hosting and a CloudFront distribution to serve the React dashboard from that bucket.
8. WHEN `cdk deploy` is executed, THE CDK_Stack SHALL provision all defined resources in a single CloudFormation stack.
9. WHEN `cdk destroy` is executed, THE CDK_Stack SHALL remove all provisioned resources.
10. IF a resource creation fails during deployment, THEN THE CDK_Stack SHALL roll back all changes made in the current deployment.
