#!/usr/bin/env bash
set -euo pipefail

# Configuration (env-overridable with sane defaults)
AWS_PROFILE="${AWS_PROFILE:-prod}"
AWS_REGION="${AWS_REGION:-us-east-2}"
ECR_REPOSITORY="${ECR_REPOSITORY:-trackgraph-api}"
# Prefer git short SHA if available, otherwise 'latest'
DOCKER_IMAGE_TAG="${DOCKER_IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || echo latest)}"
S3_BUCKET="${S3_BUCKET:-trackgraph-frontend-bucket}"
CLOUDFRONT_DISTRIBUTION_ID="${CLOUDFRONT_DISTRIBUTION_ID:-E2TECYH5833PN9}"

# Optional: set DRY_RUN=1 to preview S3 sync
DRY_RUN_FLAG=""
if [[ "${DRY_RUN:-}" =~ ^(1|true|yes)$ ]]; then
  DRY_RUN_FLAG="--dry-run"
fi

log()  { printf "\033[1;34m[deploy]\033[0m %s\n" "$*"; }
err()  { printf "\033[1;31m[error]\033[0m %s\n" "$*" 1>&2; }

# Basic preflight checks
command -v aws >/dev/null 2>&1 || { err "aws CLI is required"; exit 1; }
command -v docker >/dev/null 2>&1 || { err "docker is required"; exit 1; }
command -v npm >/dev/null 2>&1 || { err "npm is required"; exit 1; }

log "Using profile=${AWS_PROFILE}, region=${AWS_REGION}"
log "ECR repo=${ECR_REPOSITORY}, tag=${DOCKER_IMAGE_TAG}"
log "S3 bucket=${S3_BUCKET} ${DRY_RUN_FLAG:+(dry-run)}"

# Validate AWS credentials for the selected profile and get account ID.
log "Validating AWS STS identity..."
ACCOUNT_ID=$(aws sts get-caller-identity \
  --profile "${AWS_PROFILE}" \
  --query 'Account' --output text)
if [[ -z "${ACCOUNT_ID}" || "${ACCOUNT_ID}" == "None" ]]; then
  err "Failed to resolve AWS account ID for profile '${AWS_PROFILE}'."
  exit 1
fi
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_URI="${ECR_REGISTRY}/${ECR_REPOSITORY}"
log "AWS account=${ACCOUNT_ID} registry=${ECR_REGISTRY}"

# Log in to ECR.
log "Logging in to ECR..."
aws ecr get-login-password --region "${AWS_REGION}" --profile "${AWS_PROFILE}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

# Build, tag, and push the backend image.
log "Building backend Docker image..."
pushd backend >/dev/null
docker build --platform linux/amd64 -t "${ECR_REPOSITORY}:${DOCKER_IMAGE_TAG}" .
docker tag "${ECR_REPOSITORY}:${DOCKER_IMAGE_TAG}" "${ECR_URI}:${DOCKER_IMAGE_TAG}"
log "Pushing image to ${ECR_URI}:${DOCKER_IMAGE_TAG}"
docker push "${ECR_URI}:${DOCKER_IMAGE_TAG}"
popd >/dev/null

# Build the frontend assets.
log "Building frontend (React) assets..."
pushd spotify-analytics-dashboard >/dev/null
if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install --no-audit --no-fund
fi
npm run build
popd >/dev/null

# Sync the frontend bundle to S3.
log "Syncing build/ to s3://${S3_BUCKET}/ ${DRY_RUN_FLAG}"
aws s3 sync spotify-analytics-dashboard/build/ "s3://${S3_BUCKET}/" --delete ${DRY_RUN_FLAG}

# Ensure index.html is served fresh (avoid stale SPA shell)
if [[ -z "${DRY_RUN_FLAG}" ]]; then
  if [[ -f spotify-analytics-dashboard/build/index.html ]]; then
    log "Setting no-cache headers on index.html"
    aws s3 cp spotify-analytics-dashboard/build/index.html \
      "s3://${S3_BUCKET}/index.html" \
      --cache-control "no-cache, no-store, must-revalidate" \
      --content-type "text/html"
  fi
fi

# Optionally invalidate CloudFront cache
if [[ -n "${CLOUDFRONT_DISTRIBUTION_ID}" && -z "${DRY_RUN_FLAG}" ]]; then
  log "Creating CloudFront invalidation for /*"
  aws cloudfront create-invalidation \
    --distribution-id "${CLOUDFRONT_DISTRIBUTION_ID}" \
    --paths "/*" >/dev/null && log "CloudFront invalidation submitted"
fi

log "Deployment completed successfully."
