---
"jazz-tools": patch
---

`s3()` blob stores now use the AWS credentials the host attaches, so self-hosted backends on ECS, Fargate, EKS Pod Identity, Lambda and EC2 need no static keys: after `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE`, the backend tries the container credentials endpoint (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` / `_FULL_URI`) and then the EC2 instance profile (IMDSv2), refreshing the keys before they expire. Static keys now take precedence over `AWS_ROLE_ARN` when both are set, as in the AWS SDKs (a `vercel()` core still always assumes its role). When no credentials are found, the error names every source tried.
