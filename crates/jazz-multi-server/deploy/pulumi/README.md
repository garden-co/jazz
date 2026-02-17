# jazz-cloud2 infrastructure

Infrastructure as Code for the `jazz-multi-server` MVP deployment using Pulumi and AWS.

This deployment is intentionally simple:

- single region (default `us-east-2`)
- single ECS service (EC2 capacity provider, desired count 1)
- ALB + HTTPS
- persistent EBS volume mounted at `/mnt/data`
- Route53 record `cloud2.aws.cloud.jazz.tools`

## Prerequisites

- [Node.js](https://nodejs.org/) v22+
- [pnpm](https://pnpm.io/)
- [AWS CLI](https://aws.amazon.com/cli/) v2
- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/)
- [Docker](https://docs.docker.com/get-docker/) with Buildx support
- AWS account with permissions for ECR + ECS + EC2 + ALB + ACM + Route53 + IAM + Secrets Manager

## Project structure

- `index.ts` - Pulumi program
- `Pulumi.yaml` - Pulumi project
- `Pulumi.dev.yaml` - tracked non-secret stack config for stack `dev`
- `Dockerfile` - container build for `jazz-multi-server`
- `push-multi-server.sh` - local ECR push + Pulumi image tag update
- `deploy-local.sh` - end-to-end local build/push/config/deploy helper

## Getting started (local)

### Step 1: install dependencies

```bash
cd crates/jazz-multi-server/deploy/pulumi
pnpm install --ignore-workspace
```

### Step 2: configure AWS SSO

If you have not already configured SSO:

```bash
aws configure sso --profile <your-profile>
```

Use these values:

- SSO start URL: `https://d-9067cab0c6.awsapps.com/start`
- SSO region: `us-east-1`
- Registration scopes: accept the default `sso:account:access`
- Default client region: `us-east-2`

Then log in:

```bash
aws sso login --profile <your-profile>
aws sts get-caller-identity --profile <your-profile>
```

### Step 3: bootstrap Pulumi stack

Select or create stack:

```bash
# Use short name if it works in your setup:
pulumi stack select dev || pulumi stack init dev

# If Pulumi asks for a fully qualified stack, use:
# pulumi stack select garden-computing/jazz-cloud2/dev || \
#   pulumi stack init garden-computing/jazz-cloud2/dev
```

Set baseline non-secret config:

```bash
ACCOUNT_ID="$(aws sts get-caller-identity --profile <your-profile> --query Account --output text)"
pulumi config set region us-east-2
pulumi config set allowedAccountId "${ACCOUNT_ID}"
pulumi config set domainName cloud2.aws.cloud.jazz.tools
pulumi config set containerImageRepository "${ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/jazz-multi-server"
pulumi config set containerImageTag latest
```

Set required secrets (one-time):

```bash
INTERNAL_API_SECRET="${INTERNAL_API_SECRET:-$(openssl rand -hex 32)}"
SECRET_HASH_KEY="${SECRET_HASH_KEY:-$(openssl rand -hex 32)}"

pulumi config set --secret internalApiSecret "${INTERNAL_API_SECRET}"
pulumi config set --secret secretHashKey "${SECRET_HASH_KEY}"
```

Notes:

- `secretHashKey` must remain stable across deploys to preserve secret hash validation semantics.
- If you use `deploy-local.sh`, missing secrets are auto-generated and persisted in `.deploy-secrets-<stack-id>.env`.

### Step 4: push image to ECR (infra-composer style)

Use the local push flow that mirrors `infra-composer`:

```bash
pnpm push:image:local -- --aws-profile <your-profile> --stack dev
```

This script:

- logs in via `aws ecr get-login-password`
- builds and pushes linux/amd64 image
- updates stack config:
  - `containerImageRepository`
  - `containerImageTag`
- removes `containerImage` key if present (so repo+tag is authoritative)

### Step 5: deploy infra

```bash
pulumi up
```

### Step 6: verify deployment

```bash
pulumi stack output
curl -i https://cloud2.aws.cloud.jazz.tools/health
```

## Alternative: one-command local deploy

If you want build+push+config+deploy in one command:

```bash
./deploy-local.sh --aws-profile <your-profile> --stack dev --yes
```

## Stack config reference

Required:

- one of:
  - `containerImage` (full URI), or
  - `containerImageRepository` + `containerImageTag`
- `internalApiSecret` (secret)
- `secretHashKey` (secret)

Common:

- `allowedAccountId`
- `region` (default `us-east-2`)
- `domainName` (default `cloud2.aws.cloud.jazz.tools`)
- `sharedServicesStack` (default `garden-computing/jazz-aws/shared-services`)

Optional:

- `route53DelegationRoleArn` (for cross-account DNS writes)
- `instanceType` (default `t3.large`)
- `dataVolumeSizeGiB` (default `100`)
- `appPort` (default `1625`)
- `workerThreads`
- `dataRoot` (default `/mnt/data`)
- `publicSubnetCidrs` (default `["10.42.0.0/24","10.42.1.0/24"]`)

## Releasing

### Local release (recommended for now)

```bash
pnpm push:image:local -- --aws-profile <your-profile> --stack dev
pulumi up
```

### Pulumi Cloud + GitHub model (optional)

This repo also includes workflows for image publish + PR-based stack tag updates:

- `.github/workflows/publish-multi-server-image.yml`
- `.github/workflows/deploy-multi-server-image.yml`

In that model:

1. publish workflow pushes image
2. deploy workflow updates `Pulumi.<stack>.yaml` tag in a PR and auto-merges
3. Pulumi Cloud runs `pulumi up` on merge to `main`

## Cleaning up

Destroy stack resources:

```bash
pulumi destroy
```

## Troubleshooting

### AWS SSO session expired

```bash
aws sso login --profile <your-profile>
```

### Buildx not available

Install/enable Docker Buildx and re-run:

```bash
docker buildx version
```

### Cross-account DNS issues

Set `route53DelegationRoleArn` in the stack and ensure that role can mutate
`cloud2.aws.cloud.jazz.tools` in the parent hosted zone.

### Error: "--stack flag requires fully qualified name"

If you see:

`If you're using the --stack flag, pass the fully qualified name (org/project/stack)`

either:

- avoid `--stack` after selecting the stack once with `pulumi stack select`, or
- use the full name format, for example:
  `garden-computing/jazz-cloud2/dev`
