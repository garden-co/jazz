# jazz-cloud2 infrastructure

Infrastructure as Code for the `jazz-cloud-server` MVP deployment using Pulumi and AWS.

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
- `Pulumi.dev.yaml` - tracked shared stack config for stack `dev`
- `Dockerfile` - container build for `jazz-cloud-server`
- `push-cloud-server.sh` - local ECR push + Pulumi image tag update
- `deploy-local.sh` - end-to-end local build/push/config/deploy helper

Tracked stack files intentionally omit runner-specific credential settings so the same
stack works in Pulumi Cloud Deployments and local one-off runs.

## Pulumi Cloud Deployments (recommended)

Use Pulumi Cloud as the system that runs `pulumi preview` on PRs and `pulumi up`
after merges to `main`.

Recommended stack settings for `garden-co/jazz2`:

- repository: `garden-co/jazz2`
- branch: `main`
- project path: `crates/jazz-cloud-server/deploy/pulumi`
- path filter: `crates/jazz-cloud-server/deploy/pulumi/**`
- PR previews: enabled

Recommended deployment environment:

- AWS credentials provided by Pulumi Deployments OIDC or deployment env vars
- no committed `awsPrimaryProfile` / `awsDnsProfile` in `Pulumi.<stack>.yaml`
- `route53DelegationRoleArn` if DNS writes need a second account/role
- `rootZoneId` if the deployment runner cannot read the shared-services stack
  reference

The GitHub workflows in this repo are designed around that model:

1. `publish-cloud-server-image.yml` builds and pushes the container image
2. `deploy-cloud-server-image.yml` updates `Pulumi.<stack>.yaml` in a PR
3. Pulumi Cloud previews that PR, then applies on merge to `main`

## Getting started (local)

### Step 1: install dependencies

```bash
cd crates/jazz-cloud-server/deploy/pulumi
npm install
```

### Step 2: configure AWS SSO

Configure local AWS credentials for the staging account. If DNS writes require a
second account, prefer a delegation role so local config stays compatible with
Pulumi Cloud.

```bash
aws configure sso --profile jazz2:staging
```

Use these values:

- SSO start URL: `https://d-9a675128f3.awsapps.com/start/`
- SSO region: `us-east-2`
- Registration scopes: accept the default `sso:account:access`
- Default client region: `us-east-2`

Then log in:

```bash
aws sso login --profile jazz2:staging

aws sts get-caller-identity --profile jazz2:staging
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
STAGING_PROFILE="jazz2:staging"
STAGING_ACCOUNT_ID="$(aws sts get-caller-identity --profile "${STAGING_PROFILE}" --query Account --output text)"

pulumi config set region us-east-2
pulumi config set allowedAccountId "${STAGING_ACCOUNT_ID}"
pulumi config set domainName cloud2.aws.cloud.jazz.tools
pulumi config set containerImageRepository "${STAGING_ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/jazz-cloud-server"
pulumi config set containerImageTag latest
```

For local `pulumi preview` / `pulumi up`, export your compute credentials before
running Pulumi:

```bash
export AWS_PROFILE="${STAGING_PROFILE}"
```

If DNS writes require a second account, configure one of:

```bash
# Preferred: a role the staging credentials can assume for Route53 changes.
pulumi config set route53DelegationRoleArn arn:aws:iam::851454408348:role/jazz-route53-delegation-staging

# Local-only fallback; do not commit this in shared stack files.
pulumi config set awsDnsProfile jazz2:shared
```

Set required secrets (one-time):

```bash
INTERNAL_API_SECRET="${INTERNAL_API_SECRET:-$(openssl rand -hex 32)}"
SECRET_HASH_KEY="${SECRET_HASH_KEY:-$(openssl rand -hex 32)}"

export JAZZ_CLOUD2_INTERNAL_API_SECRET="${INTERNAL_API_SECRET}"
export JAZZ_CLOUD2_SECRET_HASH_KEY="${SECRET_HASH_KEY}"
```

Notes:

- `secretHashKey` must remain stable across deploys to preserve secret hash validation semantics.
- Pulumi Cloud Deployments can read the same values from deployment environment
  variables `JAZZ_CLOUD2_INTERNAL_API_SECRET` and `JAZZ_CLOUD2_SECRET_HASH_KEY`.
- If you use `deploy-local.sh`, missing secrets are auto-generated and persisted in
  `.deploy-secrets-<stack-id>.env`.
- If you prefer stack config instead of env vars, `pulumi config set --secret` still
  works for `internalApiSecret` and `secretHashKey`.

### Step 4: push image to ECR (infra-composer style)

Use the local push flow that mirrors `infra-composer`:

```bash
pnpm push:image:local -- --aws-profile jazz2:staging --stack dev
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
./deploy-local.sh --aws-profile jazz2:staging --stack dev --yes
```

## Stack config reference

Required:

- one of:
  - `containerImage` (full URI), or
  - `containerImageRepository` + `containerImageTag`
- one of:
  - `internalApiSecret` (secret config), or
  - `JAZZ_CLOUD2_INTERNAL_API_SECRET` (deployment env var)
- one of:
  - `secretHashKey` (secret config), or
  - `JAZZ_CLOUD2_SECRET_HASH_KEY` (deployment env var)

Common:

- `allowedAccountId`
- `region` (default `us-east-2`)
- `domainName` (default `cloud2.aws.cloud.jazz.tools`)
- `sharedServicesStack` (default `garden-computing/jazz-aws/shared-services`)

Optional:

- `awsPrimaryProfile` (local-only profile override for ECS/EC2/ECR resources)
- `awsDnsProfile` (local-only profile override for Route53/ACM DNS resources)
- `route53DelegationRoleArn` (alternative to `awsDnsProfile` when using assume-role DNS writes)
- `rootZoneId` (avoid the shared-services `StackReference` when deploy runners lack access)
- `instanceType` (default `t3.large`)
- `dataVolumeSizeGiB` (default `100`)
- `appPort` (default `1625`)
- `workerThreads`
- `dataRoot` (default `/mnt/data`)
- `publicSubnetCidrs` (default `["10.42.0.0/24","10.42.1.0/24"]`)

## Releasing

### Pulumi Cloud release (recommended)

This is the intended day-to-day path:

1. Run `.github/workflows/publish-cloud-server-image.yml` to publish a new image
2. Let `.github/workflows/deploy-cloud-server-image.yml` open/update the deployment PR
3. Wait for the Pulumi Cloud preview on that PR
4. Merge the PR so Pulumi Cloud runs `pulumi up`

Required GitHub configuration:

- repo variable `AWS_ACCOUNT_ID`
- repo secret `AWS_GITHUB_ACTIONS_ROLE_ARN`

Pulumi Cloud must also be configured with AWS credentials for deployments.
Set `JAZZ_CLOUD2_INTERNAL_API_SECRET` and `JAZZ_CLOUD2_SECRET_HASH_KEY` in the
Pulumi deployment environment unless you manage them as stack secrets instead.

### Local release

```bash
pnpm push:image:local -- --aws-profile jazz2:staging --stack dev
pulumi up
```

## Cleaning up

Destroy stack resources:

```bash
pulumi destroy
```

## Troubleshooting

### AWS SSO session expired

```bash
aws sso login --profile jazz2:staging
```

### Buildx not available

Install/enable Docker Buildx and re-run:

```bash
docker buildx version
```

### Cross-account DNS issues

Use either a delegation role or a local-only shared-services profile for DNS writes:

```bash
pulumi config set route53DelegationRoleArn arn:aws:iam::851454408348:role/jazz-route53-delegation-staging
# or, for local-only use:
pulumi config set awsDnsProfile jazz2:shared
```

If you use a local-only profile override, do not commit it in the shared stack file.

### Shared-services stack reference access

If Pulumi Deployments cannot read `garden-computing/jazz-aws/shared-services`,
set the hosted zone ID directly:

```bash
pulumi config set rootZoneId Z1234567890ABC
```

### Error: "--stack flag requires fully qualified name"

If you see:

`If you're using the --stack flag, pass the fully qualified name (org/project/stack)`

either:

- avoid `--stack` after selecting the stack once with `pulumi stack select`, or
- use the full name format, for example:
  `garden-computing/jazz-cloud2/dev`
