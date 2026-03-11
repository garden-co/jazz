#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Quick local deploy helper for jazz-cloud-server jazz-cloud2/dev stack.

Usage:
  ./deploy-local.sh [options]

Options:
  --aws-profile <profile>             AWS profile to use (optional)
  --aws-region <region>               AWS region (default: us-east-2)
  --stack <name>                      Pulumi stack name (short or org/project/stack; default: dev)
  --account-id <id>                   AWS account ID (default: from STS)
  --allowed-account-id <id>           Pulumi allowedAccountId (default: account-id)
  --repo <name>                       ECR repo name (default: jazz-cloud-server)
  --tag <tag>                         Image tag (default: git short SHA)
  --image <uri>                       Full image URI (skip build when used with --skip-build)
  --domain <fqdn>                     DNS hostname (default: cloud2.aws.cloud.jazz.tools)
  --name-prefix <prefix>              Pulumi namePrefix override (optional)
  --route53-delegation-role-arn <arn> Optional Route53 delegation role
  --shared-services-stack <name>      Pulumi shared services stack reference override
  --internal-api-secret <value>       Internal API secret (optional; generated if missing)
  --secret-hash-key <value>           Secret hash key (optional; generated if missing)
  --skip-build                        Skip Docker build/push (requires --image)
  --yes                               Pass --yes to pulumi up
  -h, --help                          Show help

Secrets persistence:
  Generated secrets are written to:
    crates/jazz-cloud-server/deploy/pulumi/.deploy-secrets-<stack-id>.env
  so repeat deploys reuse the same key material.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

AWS_REGION="us-east-2"
STACK="dev"
ECR_REPOSITORY="jazz-cloud-server"
IMAGE_TAG="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
DOMAIN_NAME="cloud2.aws.cloud.jazz.tools"
SKIP_BUILD=0
PULUMI_YES=0

AWS_PROFILE_ARG=""
ACCOUNT_ID=""
ALLOWED_ACCOUNT_ID=""
IMAGE_URI=""
NAME_PREFIX=""
ROUTE53_DELEGATION_ROLE_ARN=""
SHARED_SERVICES_STACK=""
INTERNAL_API_SECRET="${INTERNAL_API_SECRET:-}"
SECRET_HASH_KEY="${SECRET_HASH_KEY:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --)
      shift
      ;;
    --aws-profile)
      AWS_PROFILE_ARG="$2"
      shift 2
      ;;
    --aws-region)
      AWS_REGION="$2"
      shift 2
      ;;
    --stack)
      STACK="$2"
      shift 2
      ;;
    --account-id)
      ACCOUNT_ID="$2"
      shift 2
      ;;
    --allowed-account-id)
      ALLOWED_ACCOUNT_ID="$2"
      shift 2
      ;;
    --repo)
      ECR_REPOSITORY="$2"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="$2"
      shift 2
      ;;
    --image)
      IMAGE_URI="$2"
      shift 2
      ;;
    --domain)
      DOMAIN_NAME="$2"
      shift 2
      ;;
    --name-prefix)
      NAME_PREFIX="$2"
      shift 2
      ;;
    --route53-delegation-role-arn)
      ROUTE53_DELEGATION_ROLE_ARN="$2"
      shift 2
      ;;
    --shared-services-stack)
      SHARED_SERVICES_STACK="$2"
      shift 2
      ;;
    --internal-api-secret)
      INTERNAL_API_SECRET="$2"
      shift 2
      ;;
    --secret-hash-key)
      SECRET_HASH_KEY="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --yes)
      PULUMI_YES=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

need_cmd aws
need_cmd docker
need_cmd git
need_cmd pnpm
need_cmd pulumi
need_cmd rsync
need_cmd mktemp
need_cmd openssl

if ! docker buildx version >/dev/null 2>&1; then
  die "docker buildx is required (for linux/amd64 image build)"
fi

if [[ -n "${AWS_PROFILE_ARG}" ]]; then
  export AWS_PROFILE="${AWS_PROFILE_ARG}"
fi
export AWS_REGION

if [[ -z "${ACCOUNT_ID}" ]]; then
  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
fi

if [[ -z "${ALLOWED_ACCOUNT_ID}" ]]; then
  ALLOWED_ACCOUNT_ID="${ACCOUNT_ID}"
fi

if [[ -z "${IMAGE_URI}" ]]; then
  IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"
fi

if [[ "${SKIP_BUILD}" -eq 1 && -z "${IMAGE_URI}" ]]; then
  die "--skip-build requires --image"
fi

STACK_ID_FOR_PATH="$(printf '%s' "${STACK}" | tr '/:' '__')"
SECRETS_FILE="${SCRIPT_DIR}/.deploy-secrets-${STACK_ID_FOR_PATH}.env"
if [[ -f "${SECRETS_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${SECRETS_FILE}"
fi

INTERNAL_API_SECRET="${INTERNAL_API_SECRET:-${DEPLOY_INTERNAL_API_SECRET:-}}"
SECRET_HASH_KEY="${SECRET_HASH_KEY:-${DEPLOY_SECRET_HASH_KEY:-}}"

GENERATED_SECRETS=0
if [[ -z "${INTERNAL_API_SECRET}" ]]; then
  INTERNAL_API_SECRET="$(openssl rand -hex 32)"
  GENERATED_SECRETS=1
fi
if [[ -z "${SECRET_HASH_KEY}" ]]; then
  SECRET_HASH_KEY="$(openssl rand -hex 32)"
  GENERATED_SECRETS=1
fi

if [[ "${GENERATED_SECRETS}" -eq 1 || ! -f "${SECRETS_FILE}" ]]; then
  cat > "${SECRETS_FILE}" <<EOF
# Auto-generated by deploy-local.sh for stack ${STACK}
DEPLOY_INTERNAL_API_SECRET=${INTERNAL_API_SECRET}
DEPLOY_SECRET_HASH_KEY=${SECRET_HASH_KEY}
EOF
  chmod 600 "${SECRETS_FILE}"
  echo "Saved deploy secrets to ${SECRETS_FILE}"
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "Building and pushing ${IMAGE_URI}"
  aws ecr describe-repositories --repository-names "${ECR_REPOSITORY}" --region "${AWS_REGION}" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${ECR_REPOSITORY}" --region "${AWS_REGION}" >/dev/null

  aws ecr get-login-password --region "${AWS_REGION}" | \
    docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com" >/dev/null

  TMP_CONTEXT="$(mktemp -d -t jazz-cloud-server-build-XXXXXX)"
  cleanup() {
    rm -rf "${TMP_CONTEXT}"
  }
  trap cleanup EXIT

  rsync -a \
    --exclude='.git' \
    --exclude='target' \
    --exclude='node_modules' \
    "${REPO_ROOT}/Cargo.toml" \
    "${REPO_ROOT}/Cargo.lock" \
    "${REPO_ROOT}/crates" \
    "${REPO_ROOT}/examples" \
    "${REPO_ROOT}/patched-crates" \
    "${TMP_CONTEXT}/"

  docker buildx build \
    --platform linux/amd64 \
    --file "${SCRIPT_DIR}/Dockerfile" \
    -t "${IMAGE_URI}" \
    --push \
    "${TMP_CONTEXT}"

  trap - EXIT
  cleanup
else
  echo "Skipping build/push, using image ${IMAGE_URI}"
fi

cd "${SCRIPT_DIR}"

if ! pulumi whoami >/dev/null 2>&1; then
  die "pulumi is not logged in. Run: pulumi login"
fi

if [[ ! -d "${SCRIPT_DIR}/node_modules" ]]; then
  pnpm install --ignore-workspace
fi

pulumi stack select "${STACK}" >/dev/null 2>&1 || pulumi stack init "${STACK}"

pulumi config set region "${AWS_REGION}"
pulumi config set allowedAccountId "${ALLOWED_ACCOUNT_ID}"
pulumi config set domainName "${DOMAIN_NAME}"
pulumi config set containerImage "${IMAGE_URI}"

if [[ -n "${NAME_PREFIX}" ]]; then
  pulumi config set namePrefix "${NAME_PREFIX}"
fi
if [[ -n "${ROUTE53_DELEGATION_ROLE_ARN}" ]]; then
  pulumi config set route53DelegationRoleArn "${ROUTE53_DELEGATION_ROLE_ARN}"
fi
if [[ -n "${SHARED_SERVICES_STACK}" ]]; then
  pulumi config set sharedServicesStack "${SHARED_SERVICES_STACK}"
fi

PULUMI_ARGS=()
if [[ "${PULUMI_YES}" -eq 1 ]]; then
  PULUMI_ARGS+=(--yes)
fi

echo
echo "Deploying stack ${STACK} in ${AWS_REGION}"
echo "Image: ${IMAGE_URI}"
echo "Domain: ${DOMAIN_NAME}"
echo

JAZZ_CLOUD2_INTERNAL_API_SECRET="${INTERNAL_API_SECRET}" \
JAZZ_CLOUD2_SECRET_HASH_KEY="${SECRET_HASH_KEY}" \
  pulumi up "${PULUMI_ARGS[@]}"

echo
echo "Done."
echo "Internal API secret source: ${SECRETS_FILE}"
