#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Usage / help
# ---------------------------------------------------------------------------
usage() {
  cat <<EOF
Usage: bash launch_app.sh [OPTIONS]

Run the fraud detection pipeline end-to-end or individual steps.

Options:
  --all                Run all steps including CDK deploy (default if no step flags given)
                       Automatically skips training/transform if artifacts already exist in S3
  --cdk-deploy         Build dashboard and deploy CDK stack
  --datagen            Step 1: Generate synthetic data and upload to S3
  --glue               Step 2: Run all three Glue jobs (ingestion, enrichment, feature engineering)
  --training           Step 3: Run SageMaker RCF training (always runs when explicitly requested)
  --transform          Step 4: Run SageMaker batch transform (always runs when explicitly requested)
  --results            Step 5: Load results to S3
  --local-ui           Start the dashboard dev server locally
  --no-auth            Skip Cognito auth when using --local-ui
  --create-user PASS   Create/reset the demo Cognito user (demo@example.com) with given password
  --model-artifact PATH  Provide model artifact path (required for --transform without --training)
  -h, --help           Show this help message

Examples:
  bash launch_app.sh                     # runs all steps (incl. CDK deploy)
  bash launch_app.sh --all               # runs all steps (explicit)
  bash launch_app.sh --cdk-deploy        # only build dashboard + deploy CDK stack
  bash launch_app.sh --training          # only run SageMaker training
  bash launch_app.sh --transform --model-artifact s3://bucket/models/.../output/model.tar.gz
  bash launch_app.sh --datagen --glue    # run data gen + glue jobs only
  bash launch_app.sh --local-ui          # start dashboard dev server locally
  bash launch_app.sh --local-ui --no-auth # start dashboard without Cognito auth
  bash launch_app.sh --create-user MyP@ss1 # create/reset demo Cognito user
EOF
  exit 0
}

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
RUN_DATAGEN=false
RUN_CDK_DEPLOY=false
RUN_GLUE=false
RUN_TRAINING=false
RUN_TRANSFORM=false
RUN_RESULTS=false
RUN_ALL=false
RUN_LOCAL_UI=false
RUN_CREATE_USER=false
NO_AUTH=false
DEMO_PASSWORD=""
MODEL_ARTIFACT_OVERRIDE=""

if [ $# -eq 0 ]; then
  RUN_ALL=true
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --all)          RUN_ALL=true ;;
    --cdk-deploy)   RUN_CDK_DEPLOY=true ;;
    --datagen)      RUN_DATAGEN=true ;;
    --glue)         RUN_GLUE=true ;;
    --training)     RUN_TRAINING=true ;;
    --transform)    RUN_TRANSFORM=true ;;
    --results)      RUN_RESULTS=true ;;
    --local-ui)     RUN_LOCAL_UI=true ;;
    --no-auth)      NO_AUTH=true ;;
    --create-user)
      RUN_CREATE_USER=true
      shift
      DEMO_PASSWORD="${1:-}"
      if [ -z "$DEMO_PASSWORD" ]; then
        echo "ERROR: --create-user requires a password argument"
        exit 1
      fi
      ;;
    --model-artifact)
      shift
      MODEL_ARTIFACT_OVERRIDE="${1:-}"
      if [ -z "$MODEL_ARTIFACT_OVERRIDE" ]; then
        echo "ERROR: --model-artifact requires a value"
        exit 1
      fi
      ;;
    -h|--help)      usage ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
  shift
done

if $RUN_ALL; then
  RUN_CDK_DEPLOY=true
  RUN_DATAGEN=true
  RUN_GLUE=true
  RUN_TRAINING=true
  RUN_TRANSFORM=true
  RUN_RESULTS=true
fi

# ---------------------------------------------------------------------------
# Smart skip: check if model / transform output already exist in S3
# ---------------------------------------------------------------------------
check_existing_artifacts() {
  # Only applies when running --all (not individual steps)
  if ! $RUN_ALL; then
    return
  fi

  # Need S3_BUCKET resolved first — defer to after stack output fetch
  :
}

_check_skip_training() {
  # Check if a trained model already exists under models/
  local latest_manifest
  latest_manifest=$(aws s3api list-objects-v2 \
    --bucket "$S3_BUCKET" \
    --prefix "models/" \
    --query "Contents[?ends_with(Key, 'manifest.json')]|sort_by(@, &LastModified)[-1].Key" \
    --output text 2>/dev/null || echo "None")

  if [ "$latest_manifest" != "None" ] && [ -n "$latest_manifest" ]; then
    # Extract model artifact path from manifest
    local artifact_path
    artifact_path=$(aws s3 cp "s3://$S3_BUCKET/$latest_manifest" - 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('model_artifact_s3_path',''))" 2>/dev/null || echo "")

    if [ -n "$artifact_path" ]; then
      echo "    [skip] Trained model already exists: $artifact_path"
      echo "           (from manifest: $latest_manifest)"
      RUN_TRAINING=false
      MODEL_ARTIFACT_PATH="$artifact_path"
      echo "$artifact_path" > /tmp/model_artifact_path.txt
    fi
  fi
}

_check_skip_transform() {
  # Check if batch transform output already exists
  local transform_output
  transform_output=$(aws s3api list-objects-v2 \
    --bucket "$S3_BUCKET" \
    --prefix "sagemaker/output/" \
    --query "Contents[0].Key" \
    --output text 2>/dev/null || echo "None")

  if [ "$transform_output" != "None" ] && [ -n "$transform_output" ]; then
    echo "    [skip] Batch transform output already exists at s3://$S3_BUCKET/sagemaker/output/"
    RUN_TRANSFORM=false
  fi
}

# ---------------------------------------------------------------------------
# Step 0: Build dashboard and deploy CDK stack
# ---------------------------------------------------------------------------
STACK_NAME="${STACK_NAME:-FraudDetectionStack}"

if $RUN_CDK_DEPLOY; then
  echo ""
  echo "==> Step 0: Building dashboard and deploying CDK stack..."
  echo "    Building dashboard (initial)..."
  npm run build --prefix dashboard
  echo "    Deploying CDK stack: $STACK_NAME"
  cdk deploy --app "PYTHONPATH=. .venv/bin/python infra/app.py" --require-approval never
  echo "    CDK deploy: done"

  # Rebuild dashboard with Cognito env vars now that stack outputs are available
  _COGNITO_POOL_ID=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='CognitoUserPoolId'].OutputValue" \
    --output text 2>/dev/null || echo "None")
  _COGNITO_CLIENT_ID=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='CognitoAppClientId'].OutputValue" \
    --output text 2>/dev/null || echo "None")
  _COGNITO_DOMAIN_URL=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='CognitoDomain'].OutputValue" \
    --output text 2>/dev/null || echo "None")

  if [ "$_COGNITO_POOL_ID" != "None" ] && [ -n "$_COGNITO_POOL_ID" ]; then
    echo "    Rebuilding dashboard with Cognito auth..."
    VITE_COGNITO_USER_POOL_ID="$_COGNITO_POOL_ID" \
    VITE_COGNITO_APP_CLIENT_ID="$_COGNITO_CLIENT_ID" \
    VITE_COGNITO_DOMAIN="$_COGNITO_DOMAIN_URL" \
    npm run build --prefix dashboard

    # Re-upload built dashboard to S3
    _DASHBOARD_BUCKET=$(aws cloudformation describe-stacks \
      --stack-name "$STACK_NAME" \
      --query "Stacks[0].Outputs[?OutputKey=='DashboardUrl'].OutputValue" \
      --output text 2>/dev/null || echo "")
    _DASHBOARD_BUCKET_NAME=$(aws cloudformation describe-stack-resources \
      --stack-name "$STACK_NAME" \
      --query "StackResources[?ResourceType=='AWS::S3::Bucket' && starts_with(LogicalResourceId, 'DashboardBucket')].PhysicalResourceId | [0]" \
      --output text 2>/dev/null || echo "")

    if [ -n "$_DASHBOARD_BUCKET_NAME" ]; then
      echo "    Uploading auth-enabled dashboard to S3..."
      aws s3 sync dashboard/dist "s3://$_DASHBOARD_BUCKET_NAME" --delete
      # Invalidate CloudFront cache
      _CF_DIST_ID=$(aws cloudformation describe-stack-resources \
        --stack-name "$STACK_NAME" \
        --query "StackResources[?ResourceType=='AWS::CloudFront::Distribution'].PhysicalResourceId | [0]" \
        --output text 2>/dev/null || echo "")
      if [ -n "$_CF_DIST_ID" ]; then
        echo "    Invalidating CloudFront cache..."
        aws cloudfront create-invalidation --distribution-id "$_CF_DIST_ID" --paths "/*" > /dev/null
      fi
    fi
    echo "    Dashboard rebuilt with Cognito auth: done"
  fi
fi

# ---------------------------------------------------------------------------
# Fetch stack outputs
# ---------------------------------------------------------------------------
echo "==> Fetching stack outputs from: $STACK_NAME"
S3_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text)

if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "None" ]; then
  echo "ERROR: Could not resolve DataBucketName from stack $STACK_NAME"
  exit 1
fi

HTTP_API_URL=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='HttpApiUrl'].OutputValue" \
  --output text)

SKU_API_URL=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='SkuApiUrl'].OutputValue" \
  --output text)

SAGEMAKER_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='SageMakerRoleArn'].OutputValue" \
  --output text)

export S3_BUCKET
export SAGEMAKER_ROLE_ARN
echo "    S3_BUCKET=$S3_BUCKET"
echo "    SKU_API_URL=$SKU_API_URL"
echo "    HTTP_API_URL=$HTTP_API_URL"
echo "    SAGEMAKER_ROLE_ARN=$SAGEMAKER_ROLE_ARN"

# Cognito outputs (optional — only present after Cognito is deployed)
COGNITO_USER_POOL_ID=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoUserPoolId'].OutputValue" \
  --output text 2>/dev/null || echo "None")

COGNITO_APP_CLIENT_ID=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoAppClientId'].OutputValue" \
  --output text 2>/dev/null || echo "None")

COGNITO_DOMAIN=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoDomain'].OutputValue" \
  --output text 2>/dev/null || echo "None")

if [ "$COGNITO_USER_POOL_ID" != "None" ] && [ -n "$COGNITO_USER_POOL_ID" ]; then
  echo "    COGNITO_USER_POOL_ID=$COGNITO_USER_POOL_ID"
  echo "    COGNITO_APP_CLIENT_ID=$COGNITO_APP_CLIENT_ID"
  echo "    COGNITO_DOMAIN=$COGNITO_DOMAIN"
fi

# ---------------------------------------------------------------------------
# Check for existing artifacts (skip expensive steps when --all)
# ---------------------------------------------------------------------------
if $RUN_ALL; then
  echo ""
  echo "==> Checking for existing artifacts..."
  _check_skip_training
  _check_skip_transform
fi

# ---------------------------------------------------------------------------
# Helper: run a Glue job and wait for it to finish
# ---------------------------------------------------------------------------
run_glue_job() {
  local job_name="$1"
  echo ""
  echo "==> Starting Glue job: $job_name"
  local run_id
  run_id=$(aws glue start-job-run --job-name "$job_name" \
    --arguments "{\"--S3_BUCKET\":\"$S3_BUCKET\",\"--SKU_API_URL\":\"$SKU_API_URL\"}" \
    --query "JobRunId" --output text)
  echo "    JobRunId: $run_id"

  local status="RUNNING"
  while [ "$status" = "RUNNING" ] || [ "$status" = "STARTING" ] || [ "$status" = "STOPPING" ] || [ "$status" = "WAITING" ]; do
    sleep 15
    status=$(aws glue get-job-run --job-name "$job_name" --run-id "$run_id" \
      --query "JobRun.JobRunState" --output text)
    echo "    $job_name: $status"
  done

  if [ "$status" != "SUCCEEDED" ]; then
    echo "ERROR: Glue job $job_name finished with status: $status"
    aws glue get-job-run --job-name "$job_name" --run-id "$run_id" \
      --query "JobRun.ErrorMessage" --output text
    exit 1
  fi
  echo "    $job_name: done"
}

# ---------------------------------------------------------------------------
# Step 1: Generate synthetic data → S3
# ---------------------------------------------------------------------------
if $RUN_DATAGEN; then
  echo ""
  echo "==> Step 1: Generating synthetic data and uploading to S3..."
  .venv/bin/python -c "
import boto3
from config import PipelineConfig
from scripts.generate_data import DataGenerator, DataGeneratorConfig

cfg = PipelineConfig(s3_bucket='$S3_BUCKET')
gen_cfg = DataGeneratorConfig.from_pipeline_config(cfg)
gen = DataGenerator(gen_cfg, s3_client=boto3.client('s3'))
print(gen.run())
"
fi

# ---------------------------------------------------------------------------
# Step 2: Run Glue jobs sequentially (scripts deployed by CDK)
# ---------------------------------------------------------------------------
if $RUN_GLUE; then
  echo ""
  echo "==> Step 2: Running Glue jobs..."
  run_glue_job "fraud-detection-ingestion"
  run_glue_job "fraud-detection-enrichment"
  run_glue_job "fraud-detection-feature-engineering"
fi

# ---------------------------------------------------------------------------
# Step 3: Run SageMaker training
# ---------------------------------------------------------------------------
if $RUN_TRAINING; then
  echo ""
  echo "==> Step 3: Running SageMaker RCF training..."
  .venv/bin/python -c "
import logging, boto3
from config import PipelineConfig
from pipeline.sagemaker_training import SageMakerTrainingConfig, SageMakerTrainingJob

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
cfg = PipelineConfig(s3_bucket='$S3_BUCKET', sagemaker_role_arn='$SAGEMAKER_ROLE_ARN')
train_cfg = SageMakerTrainingConfig(
    s3_bucket=cfg.s3_bucket,
    features_prefix=cfg.features_prefix,
    models_prefix=cfg.models_prefix,
    sagemaker_role_arn=cfg.sagemaker_role_arn,
    instance_type=cfg.sagemaker_instance_type,
    instance_count=cfg.sagemaker_training_instance_count,
    num_trees=cfg.num_trees,
    num_samples_per_tree=cfg.num_samples_per_tree,
)
job = SageMakerTrainingJob(train_cfg)
result = job.run()
print(result)

# Save model artifact path for next step
with open('/tmp/model_artifact_path.txt', 'w') as f:
    f.write(result['model_artifact_path'])
"
  MODEL_ARTIFACT_PATH=$(cat /tmp/model_artifact_path.txt)
  echo "    Model artifact: $MODEL_ARTIFACT_PATH"
fi

# ---------------------------------------------------------------------------
# Step 4: Run SageMaker batch transform
# ---------------------------------------------------------------------------
if $RUN_TRANSFORM; then
  # Resolve model artifact: override > training output > error
  if [ -n "$MODEL_ARTIFACT_OVERRIDE" ]; then
    MODEL_ARTIFACT_PATH="$MODEL_ARTIFACT_OVERRIDE"
  elif [ -z "${MODEL_ARTIFACT_PATH:-}" ]; then
    if [ -f /tmp/model_artifact_path.txt ]; then
      MODEL_ARTIFACT_PATH=$(cat /tmp/model_artifact_path.txt)
    else
      echo "ERROR: No model artifact available. Run --training first or provide --model-artifact."
      exit 1
    fi
  fi

  echo ""
  echo "==> Step 4: Running SageMaker batch transform..."
  echo "    Model artifact: $MODEL_ARTIFACT_PATH"
  .venv/bin/python -c "
import logging, os
from pipeline.sagemaker_batch_transform import SageMakerBatchTransformConfig, SageMakerBatchTransform

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
os.environ['SAGEMAKER_ROLE_ARN'] = '$SAGEMAKER_ROLE_ARN'
transform_cfg = SageMakerBatchTransformConfig(
    s3_bucket='$S3_BUCKET',
    model_artifact_path='$MODEL_ARTIFACT_PATH',
)
job = SageMakerBatchTransform(transform_cfg)
result = job.run('$MODEL_ARTIFACT_PATH')
print(result)
"
fi

# ---------------------------------------------------------------------------
# Step 5: Parse output and write results JSON → S3
# ---------------------------------------------------------------------------
if $RUN_RESULTS; then
  echo ""
  echo "==> Step 5: Loading results to S3..."
  .venv/bin/python -c "
import boto3
from config import PipelineConfig
from pipeline.results_loader import ResultsLoader, ResultsLoaderConfig

cfg = PipelineConfig(s3_bucket='$S3_BUCKET')
loader_cfg = ResultsLoaderConfig(
    s3_bucket='$S3_BUCKET',
    sagemaker_output_prefix=cfg.sagemaker_output_prefix,
    features_prefix=cfg.features_prefix,
    enriched_prefix=cfg.enriched_prefix,
    results_prefix=cfg.results_prefix,
    fraud_threshold=cfg.fraud_threshold,
    quality_threshold=cfg.quality_threshold,
)
loader = ResultsLoader(loader_cfg, boto3.client('s3'))
summary = loader.run(model_version='${MODEL_ARTIFACT_PATH:-unknown}')
print(f'Fraud: {summary.fraud_results_count}, Quality: {summary.quality_results_count}, Audit: {summary.audit_entries_written}')
"
fi

# ---------------------------------------------------------------------------
# Create/reset Cognito demo user
# ---------------------------------------------------------------------------
if $RUN_CREATE_USER; then
  echo ""
  echo "==> Creating/resetting Cognito demo user..."

  if [ "$COGNITO_USER_POOL_ID" = "None" ] || [ -z "$COGNITO_USER_POOL_ID" ]; then
    echo "ERROR: Cognito User Pool not found in stack outputs. Deploy the stack with Cognito first."
    exit 1
  fi

  DEMO_EMAIL="demo@example.com"

  # Check if user already exists
  USER_EXISTS=$(aws cognito-idp admin-get-user \
    --user-pool-id "$COGNITO_USER_POOL_ID" \
    --username "$DEMO_EMAIL" \
    --query "Username" --output text 2>/dev/null || echo "NOT_FOUND")

  if [ "$USER_EXISTS" = "NOT_FOUND" ]; then
    echo "    Creating user: $DEMO_EMAIL"
    aws cognito-idp admin-create-user \
      --user-pool-id "$COGNITO_USER_POOL_ID" \
      --username "$DEMO_EMAIL" \
      --user-attributes Name=email,Value="$DEMO_EMAIL" Name=email_verified,Value=true \
      --message-action SUPPRESS \
      --output text > /dev/null
  else
    echo "    User already exists: $DEMO_EMAIL"
  fi

  # Set permanent password (bypasses FORCE_CHANGE_PASSWORD state)
  echo "    Setting permanent password..."
  aws cognito-idp admin-set-user-password \
    --user-pool-id "$COGNITO_USER_POOL_ID" \
    --username "$DEMO_EMAIL" \
    --password "$DEMO_PASSWORD" \
    --permanent

  echo ""
  echo "    ✓ Demo user ready"
  echo "    Email:    $DEMO_EMAIL"
  echo "    Password: $DEMO_PASSWORD"
  echo ""
  echo "    Login URL: $COGNITO_DOMAIN/login?client_id=$COGNITO_APP_CLIENT_ID&response_type=code&scope=openid+email+profile&redirect_uri=https://$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].Outputs[?OutputKey=='DashboardUrl'].OutputValue" --output text | sed 's|https://||')"
fi

# ---------------------------------------------------------------------------
# Local UI: start dashboard dev server
# ---------------------------------------------------------------------------
if $RUN_LOCAL_UI; then
  echo ""
  echo "==> Starting dashboard dev server..."
  echo "    API proxy: $HTTP_API_URL"
  echo "    Dashboard: http://localhost:5173"
  VITE_ARGS="VITE_API_URL=$HTTP_API_URL"
  if ! $NO_AUTH && [ "$COGNITO_USER_POOL_ID" != "None" ] && [ -n "$COGNITO_USER_POOL_ID" ]; then
    VITE_ARGS="$VITE_ARGS VITE_COGNITO_USER_POOL_ID=$COGNITO_USER_POOL_ID VITE_COGNITO_APP_CLIENT_ID=$COGNITO_APP_CLIENT_ID VITE_COGNITO_DOMAIN=$COGNITO_DOMAIN"
    echo "    Cognito auth: enabled"
  elif $NO_AUTH; then
    echo "    Cognito auth: skipped (--no-auth)"
  fi
  env $VITE_ARGS npm run dev --prefix dashboard
else
  echo ""
  echo "==> Pipeline complete. To start the dashboard:"
  echo ""
  echo "    bash launch_app.sh --local-ui"
  echo ""
  echo "    Or manually:"
  echo "    cd dashboard && VITE_API_URL=\"$HTTP_API_URL\" npm run dev"
  echo ""
  echo "    Dashboard: http://localhost:5173"
  echo "    API proxy: $HTTP_API_URL"
fi
