#!/usr/bin/env bash
# ============================================================
# submit_emr_job.sh
#
# Creates an EMR cluster (or reuses an existing one) and submits
# the pipeline as a Spark step.
#
# Usage:
#   ./scripts/submit_emr_job.sh [--env prod] [--cluster-id j-XXXXXXXXX]
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ---- defaults ----
ENV="${PIPELINE_ENV:-dev}"
CLUSTER_ID=""
S3_BUCKET="${S3_BUCKET:-my-data-lake}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# ---- parse args ----
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)        ENV="$2";        shift 2 ;;
        --cluster-id) CLUSTER_ID="$2"; shift 2 ;;
        *)            echo "Unknown arg: $1"; exit 1 ;;
    esac
done

S3_CODE_PATH="s3://${S3_BUCKET}/code/etl-pipeline"
S3_LOGS_PATH="s3://${S3_BUCKET}/emr-logs/"

echo "╔══════════════════════════════════════════════╗"
echo "║  ETL Pipeline — EMR Submit                   ║"
echo "║  env=${ENV}                                   ║"
echo "╚══════════════════════════════════════════════╝"

# ---- Step 1: upload code to S3 ----
echo "[1/3] Uploading pipeline code to ${S3_CODE_PATH} …"
cd "$PROJECT_ROOT"
zip -r /tmp/etl_pipeline.zip \
    main.py src/ config/ requirements.txt \
    -x "*.pyc" "__pycache__/*" ".git/*" "tests/*"

aws s3 cp /tmp/etl_pipeline.zip "${S3_CODE_PATH}/etl_pipeline.zip" --region "$AWS_REGION"
aws s3 cp config/pipeline_config.yaml "${S3_CODE_PATH}/config/pipeline_config.yaml" --region "$AWS_REGION"
aws s3 cp "config/env/${ENV}.yaml" "${S3_CODE_PATH}/config/env/${ENV}.yaml" --region "$AWS_REGION" 2>/dev/null || true

# ---- Step 2: create cluster if needed ----
if [[ -z "$CLUSTER_ID" ]]; then
    echo "[2/3] Creating EMR cluster …"

    CLUSTER_ID=$(aws emr create-cluster \
        --name "etl-pipeline-${ENV}" \
        --release-label emr-7.1.0 \
        --region "$AWS_REGION" \
        --applications Name=Spark Name=Hadoop \
        --instance-groups '[
            {"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge"},
            {"InstanceCount":3,"InstanceGroupType":"CORE","InstanceType":"m5.2xlarge"}
        ]' \
        --log-uri "$S3_LOGS_PATH" \
        --service-role EMR_DefaultRole \
        --ec2-attributes "InstanceProfile=EMR_EC2_DefaultRole${EMR_SUBNET_ID:+,SubnetId=$EMR_SUBNET_ID}${EMR_KEY_PAIR:+,KeyName=$EMR_KEY_PAIR}" \
        --configurations '[
            {
                "Classification":"spark-defaults",
                "Properties":{
                    "spark.jars.packages":"net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1,org.postgresql:postgresql:42.7.3",
                    "spark.sql.adaptive.enabled":"true"
                }
            }
        ]' \
        --auto-terminate \
        --query 'ClusterId' \
        --output text)

    echo "  Cluster created: ${CLUSTER_ID}"
    echo "  Waiting for cluster to be RUNNING …"
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID" --region "$AWS_REGION"
else
    echo "[2/3] Using existing cluster: ${CLUSTER_ID}"
fi

# ---- Step 3: submit spark step ----
echo "[3/3] Submitting Spark step …"

STEP_ID=$(aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --region "$AWS_REGION" \
    --steps "[{
        \"Type\":\"Spark\",
        \"Name\":\"etl-pipeline-${ENV}\",
        \"ActionOnFailure\":\"CONTINUE\",
        \"Args\":[
            \"--deploy-mode\",\"cluster\",
            \"--py-files\",\"${S3_CODE_PATH}/etl_pipeline.zip\",
            \"${S3_CODE_PATH}/etl_pipeline.zip\",
            \"--env\",\"${ENV}\",
            \"--config\",\"config/pipeline_config.yaml\"
        ]
    }]" \
    --query 'StepIds[0]' \
    --output text)

echo "  Step submitted: ${STEP_ID}"
echo "  Monitor: aws emr describe-step --cluster-id ${CLUSTER_ID} --step-id ${STEP_ID} --region ${AWS_REGION}"
echo ""
echo "Done."
