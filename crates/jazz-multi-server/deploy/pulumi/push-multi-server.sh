#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Push jazz-multi-server image to ECR and update Pulumi stack image tag.

Usage:
  ./push-multi-server.sh [options]

Options:
  --stack <name>        Pulumi stack (default: cloud2)
  --tag <tag>           Image tag (default: sha-<short git sha>)
  --image <uri>         Full image URI override
  --repo <name>         ECR repository name (default: jazz-multi-server)
  --account-id <id>     AWS account ID (default: from STS)
  --region <region>     AWS region (default: us-east-2)
  --aws-profile <name>  AWS profile to use
  --no-config-update    Skip pulumi config tag update
  -h, --help            Show help

Examples:
  ./push-multi-server.sh --aws-profile jazz
  ./push-multi-server.sh --stack cloud2 --tag v0.1.0
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

STACK="cloud2"
TAG="sha-$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
IMAGE_URI=""
ECR_REPOSITORY="jazz-multi-server"
ACCOUNT_ID=""
AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_PROFILE_ARG=""
NO_CONFIG_UPDATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack)
      STACK="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    --image)
      IMAGE_URI="$2"
      shift 2
      ;;
    --repo)
      ECR_REPOSITORY="$2"
      shift 2
      ;;
    --account-id)
      ACCOUNT_ID="$2"
      shift 2
      ;;
    --region)
      AWS_REGION="$2"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE_ARG="$2"
      shift 2
      ;;
    --no-config-update)
      NO_CONFIG_UPDATE=1
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
need_cmd pulumi

if ! docker buildx version >/dev/null 2>&1; then
  die "docker buildx is required"
fi

if [[ -n "${AWS_PROFILE_ARG}" ]]; then
  export AWS_PROFILE="${AWS_PROFILE_ARG}"
fi
export AWS_REGION

if [[ -z "${ACCOUNT_ID}" ]]; then
  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
fi

if [[ -z "${IMAGE_URI}" ]]; then
  IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${TAG}"
fi

REGISTRY="${IMAGE_URI%%/*}"
REPO_AND_TAG="${IMAGE_URI#*/}"
IMAGE_REPO="${REPO_AND_TAG%:*}"
IMAGE_TAG="${IMAGE_URI##*:}"
IMAGE_REGION="$(printf '%s' "${REGISTRY}" | cut -d. -f4)"

if [[ -z "${IMAGE_REGION}" ]]; then
  die "unable to parse region from image URI: ${IMAGE_URI}"
fi

echo "Pushing image ${IMAGE_URI}"

aws ecr describe-repositories --repository-names "${IMAGE_REPO}" --region "${IMAGE_REGION}" >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name "${IMAGE_REPO}" --region "${IMAGE_REGION}" >/dev/null

aws ecr get-login-password --region "${IMAGE_REGION}" | \
  docker login --username AWS --password-stdin "${REGISTRY}" >/dev/null

docker buildx build \
  --platform linux/amd64 \
  --file "${SCRIPT_DIR}/Dockerfile" \
  --tag "${IMAGE_URI}" \
  --push \
  "${REPO_ROOT}"

echo "Pushed: ${IMAGE_URI}"

if [[ "${NO_CONFIG_UPDATE}" -eq 0 ]]; then
  cd "${SCRIPT_DIR}"

  pulumi stack select "${STACK}" >/dev/null 2>&1 || pulumi stack init "${STACK}"

  pulumi config set containerImageRepository "${REGISTRY}/${IMAGE_REPO}" --stack "${STACK}"
  pulumi config set containerImageTag "${IMAGE_TAG}" --stack "${STACK}"

  # Ensure repo+tag drives deploys; this key would otherwise take precedence.
  if pulumi config get containerImage --stack "${STACK}" >/dev/null 2>&1; then
    pulumi config rm containerImage --stack "${STACK}" || true
  fi

  echo "Updated Pulumi stack '${STACK}' image config:"
  echo "  containerImageRepository=${REGISTRY}/${IMAGE_REPO}"
  echo "  containerImageTag=${IMAGE_TAG}"
  echo "Run: pulumi up --stack ${STACK}"
fi
