# jazz-multi-server cloud2 Pulumi sketch

This is an MVP deployment sketch for a single hosted `jazz-multi-server` instance on AWS:

- 1 region (`us-east-2` default)
- 1 ECS service on EC2 capacity
- 1 ALB with HTTPS
- 1 persistent EBS volume mounted at `/mnt/data`
- Route53 `A` record for `cloud2.aws.cloud.jazz.tools`

## What it creates

- VPC + 2 public subnets (ALB across both)
- ECS cluster + ASG-backed capacity provider (desired=1)
- ECS task definition + service (desired=1)
- ACM cert (DNS validated)
- ALB listeners (80 -> 443 redirect, 443 forward)
- Route53 alias record for the deployment hostname
- Secrets Manager secrets for:
  - `JAZZ_INTERNAL_API_SECRET`
  - `JAZZ_SECRET_HASH_KEY`

## Stack config

Set these on your stack before `pulumi up`:

- Required:
  - `containerImage` (image for `jazz-multi-server`)
  - `internalApiSecret` (secret)
  - `secretHashKey` (secret)
- Usually required in CI/guardrails:
  - `allowedAccountId` (AWS account ID for this stack)

Defaults:

- `region=us-east-2`
- `domainName=cloud2.aws.cloud.jazz.tools`
- `sharedServicesStack=garden-computing/jazz-aws/shared-services`
- `rootZoneId` from `sharedServicesStack.awsCloudZoneId`

Optional:

- `route53DelegationRoleArn` (if stack account cannot directly write Route53 in root zone)
- `instanceType=t3.large`
- `dataVolumeSizeGiB=100`
- `appPort=1625`
- `workerThreads` (server worker thread override)
- `dataRoot=/mnt/data`
- `publicSubnetCidrs=["10.42.0.0/24","10.42.1.0/24"]`

## Example bootstrap

```bash
cd crates/jazz-multi-server/deploy/pulumi
pnpm install

pulumi stack init cloud2
pulumi config set containerImage 851454408348.dkr.ecr.us-east-2.amazonaws.com/jazz-multi-server:<tag>
pulumi config set --secret internalApiSecret '<redacted>'
pulumi config set --secret secretHashKey '<redacted>'
pulumi config set allowedAccountId 851454408348

pulumi preview
pulumi up
```

## Local quick deploy script

You can use:

```bash
cd crates/jazz-multi-server/deploy/pulumi
./deploy-local.sh --aws-profile <profile> --yes
```

The script will:

- Build and push `jazz-multi-server` to ECR (linux/amd64)
- Initialize/select Pulumi stack `cloud2`
- Set Pulumi config values
- Generate missing secrets and persist them locally for reuse
- Run `pulumi up`

Important local file:

- `.deploy-secrets-<stack>.env`
  - Stores generated `internalApiSecret` and `secretHashKey`
  - Reused on subsequent deploys so secret hashing remains stable

### Inputs and secrets

Required inputs:

- AWS credentials (`AWS_PROFILE` or default credentials chain)
- Pulumi login/session (`pulumi login` done)

Required deploy secrets:

- `internalApiSecret` (for `/internal/apps/*` auth)
- `secretHashKey` (used to hash backend/admin secrets; must stay stable across redeploys)

How secrets are handled:

- If you pass `--internal-api-secret` / `--secret-hash-key`, those are used.
- If omitted, the script auto-generates both and writes them to `.deploy-secrets-<stack>.env`.

Useful optional inputs:

- `--account-id` / `--allowed-account-id`
- `--domain` (defaults to `cloud2.aws.cloud.jazz.tools`)
- `--route53-delegation-role-arn` (for cross-account DNS writes)
- `--image` with `--skip-build` (if image already exists)

## Notes

- This is intentionally single-instance and minimal.
- Internal API auth is secret-based only right now; network-level restriction for `/internal/apps/*` is still a TODO.
- If you deploy this into a non-shared-services AWS account, configure `route53DelegationRoleArn` (and ensure that role has write permission for `cloud2.aws.cloud.jazz.tools` in the parent zone).
