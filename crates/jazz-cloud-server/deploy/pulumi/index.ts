import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";

const cfg = new pulumi.Config();

const region = (cfg.get("region") ?? "us-east-2") as aws.Region;
const environment = cfg.get("environment") ?? pulumi.getStack();
const namePrefix = cfg.get("namePrefix") ?? `jazz-${environment}-cloud2`;
const domainName = cfg.get("domainName") ?? "cloud2.aws.cloud.jazz.tools";

const containerImage = cfg.get("containerImage");
const containerImageRepository = cfg.get("containerImageRepository");
const containerImageTag = cfg.get("containerImageTag");

if (!containerImage && !(containerImageRepository && containerImageTag)) {
  throw new Error(
    "configure either `containerImage` or (`containerImageRepository` + `containerImageTag`)",
  );
}

const resolvedContainerImage = containerImage ?? `${containerImageRepository}:${containerImageTag}`;

const appPort = cfg.getNumber("appPort") ?? 1625;
const workerThreads = cfg.getNumber("workerThreads");
const dataRoot = cfg.get("dataRoot") ?? "/mnt/data";
const healthCheckPath = cfg.get("healthCheckPath") ?? "/health";
const rustLog = cfg.get("rustLog") ?? "info";
const containerMemoryReservationMiB = cfg.getNumber("containerMemoryReservationMiB") ?? 512;
const containerMemoryMiB = cfg.getNumber("containerMemoryMiB");

const allowedAccountId = cfg.get("allowedAccountId");
const awsPrimaryProfile = cfg.get("awsPrimaryProfile");
const awsDnsProfile = cfg.get("awsDnsProfile");
const route53DelegationRoleArn = cfg.get("route53DelegationRoleArn");
const sharedServicesStackName =
  cfg.get("sharedServicesStack") ?? "garden-computing/jazz-aws/shared-services";

const instanceType = cfg.get("instanceType") ?? "t3.large";
const dataVolumeSizeGiB = cfg.getNumber("dataVolumeSizeGiB") ?? 100;
const publicSubnetCidrs = cfg.getObject<string[]>("publicSubnetCidrs") ?? [
  "10.42.0.0/24",
  "10.42.1.0/24",
];

if (publicSubnetCidrs.length < 2) {
  throw new Error(
    "config `publicSubnetCidrs` must include at least two CIDR blocks for ALB subnets",
  );
}

const internalApiSecret = cfg.requireSecret("internalApiSecret");
const secretHashKey = cfg.requireSecret("secretHashKey");

const tags = {
  Project: "jazz",
  Environment: environment,
};

const providerArgs: aws.ProviderArgs = {
  region,
  defaultTags: { tags },
  skipRegionValidation: true,
};

if (allowedAccountId) {
  providerArgs.allowedAccountIds = [allowedAccountId];
}
if (awsPrimaryProfile) {
  providerArgs.profile = awsPrimaryProfile;
}

const primary = new aws.Provider("primary", providerArgs);

const dnsProviderArgs: aws.ProviderArgs = {
  region,
  skipRegionValidation: true,
  defaultTags: { tags },
};

if (awsDnsProfile) {
  dnsProviderArgs.profile = awsDnsProfile;
}
if (route53DelegationRoleArn) {
  dnsProviderArgs.assumeRole = { roleArn: route53DelegationRoleArn };
}

const dnsProvider =
  awsDnsProfile || route53DelegationRoleArn
    ? new aws.Provider("dns-delegation", dnsProviderArgs)
    : primary;

const rootZoneIdFromConfig = cfg.get("rootZoneId");
const rootZoneId = rootZoneIdFromConfig
  ? pulumi.output(rootZoneIdFromConfig)
  : new pulumi.StackReference(sharedServicesStackName)
      .getOutput("awsCloudZoneId")
      .apply((id) => String(id));

const availabilityZones = aws.getAvailabilityZonesOutput(
  { state: "available" },
  { provider: primary },
);

const vpc = new aws.ec2.Vpc(
  "vpc",
  {
    cidrBlock: cfg.get("vpcCidr") ?? "10.42.0.0/16",
    enableDnsHostnames: true,
    enableDnsSupport: true,
    tags: {
      ...tags,
      Name: `${namePrefix}-vpc`,
    },
  },
  { provider: primary },
);

const internetGateway = new aws.ec2.InternetGateway(
  "igw",
  {
    vpcId: vpc.id,
    tags: {
      ...tags,
      Name: `${namePrefix}-igw`,
    },
  },
  { provider: primary },
);

const publicRouteTable = new aws.ec2.RouteTable(
  "public-rt",
  {
    vpcId: vpc.id,
    routes: [
      {
        cidrBlock: "0.0.0.0/0",
        gatewayId: internetGateway.id,
      },
    ],
    tags: {
      ...tags,
      Name: `${namePrefix}-public-rt`,
    },
  },
  { provider: primary },
);

const publicSubnets = publicSubnetCidrs.map(
  (cidrBlock, index) =>
    new aws.ec2.Subnet(
      `public-subnet-${index + 1}`,
      {
        vpcId: vpc.id,
        cidrBlock,
        mapPublicIpOnLaunch: true,
        availabilityZone: availabilityZones.names.apply((names) => names[index]),
        tags: {
          ...tags,
          Name: `${namePrefix}-public-subnet-${index + 1}`,
        },
      },
      { provider: primary },
    ),
);

publicSubnets.forEach((subnet, index) => {
  new aws.ec2.RouteTableAssociation(
    `public-rta-${index + 1}`,
    {
      routeTableId: publicRouteTable.id,
      subnetId: subnet.id,
    },
    { provider: primary },
  );
});

const albSecurityGroup = new aws.ec2.SecurityGroup(
  "alb-sg",
  {
    vpcId: vpc.id,
    description: "ALB ingress for cloud2 cloud-server",
    ingress: [
      { fromPort: 80, toPort: 80, protocol: "tcp", cidrBlocks: ["0.0.0.0/0"] },
      { fromPort: 443, toPort: 443, protocol: "tcp", cidrBlocks: ["0.0.0.0/0"] },
    ],
    egress: [{ fromPort: 0, toPort: 0, protocol: "-1", cidrBlocks: ["0.0.0.0/0"] }],
    tags: {
      ...tags,
      Name: `${namePrefix}-alb-sg`,
    },
  },
  { provider: primary },
);

const instanceSecurityGroup = new aws.ec2.SecurityGroup(
  "instance-sg",
  {
    vpcId: vpc.id,
    description: "ECS container instance ingress from ALB",
    egress: [{ fromPort: 0, toPort: 0, protocol: "-1", cidrBlocks: ["0.0.0.0/0"] }],
    tags: {
      ...tags,
      Name: `${namePrefix}-instance-sg`,
    },
  },
  { provider: primary },
);

new aws.ec2.SecurityGroupRule(
  "app-ingress-from-alb",
  {
    type: "ingress",
    fromPort: appPort,
    toPort: appPort,
    protocol: "tcp",
    securityGroupId: instanceSecurityGroup.id,
    sourceSecurityGroupId: albSecurityGroup.id,
  },
  { provider: primary },
);

const cluster = new aws.ecs.Cluster(
  "cluster",
  {
    name: `${namePrefix}-cluster`,
    settings: [{ name: "containerInsights", value: "enabled" }],
    tags: {
      ...tags,
      Name: `${namePrefix}-cluster`,
    },
  },
  { provider: primary },
);

const dataVolume = new aws.ebs.Volume(
  "data-volume",
  {
    availabilityZone: publicSubnets[0].availabilityZone,
    size: dataVolumeSizeGiB,
    type: "gp3",
    tags: {
      ...tags,
      Name: `${namePrefix}-data`,
      Component: "cloud-server-data",
    },
  },
  { provider: primary },
);

const instanceRole = new aws.iam.Role(
  "instance-role",
  {
    name: `${namePrefix}-instance-role`,
    assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({ Service: "ec2.amazonaws.com" }),
    tags,
  },
  { provider: primary },
);

[
  "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
  "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
  "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
].forEach((policyArn, index) => {
  new aws.iam.RolePolicyAttachment(
    `instance-managed-policy-${index + 1}`,
    {
      role: instanceRole.name,
      policyArn,
    },
    { provider: primary },
  );
});

new aws.iam.RolePolicy(
  "instance-ebs-policy",
  {
    role: instanceRole.name,
    policy: JSON.stringify({
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: [
            "ec2:DescribeInstances",
            "ec2:DescribeVolumes",
            "ec2:AttachVolume",
            "ec2:CreateTags",
          ],
          Resource: "*",
        },
      ],
    }),
  },
  { provider: primary },
);

const instanceProfile = new aws.iam.InstanceProfile(
  "instance-profile",
  {
    name: `${namePrefix}-instance-profile`,
    role: instanceRole.name,
    tags,
  },
  { provider: primary },
);

const taskExecutionRole = new aws.iam.Role(
  "task-execution-role",
  {
    name: `${namePrefix}-task-execution-role`,
    assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({ Service: "ecs-tasks.amazonaws.com" }),
    tags,
  },
  { provider: primary },
);

new aws.iam.RolePolicyAttachment(
  "task-execution-managed-policy",
  {
    role: taskExecutionRole.name,
    policyArn: "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
  },
  { provider: primary },
);

const taskRole = new aws.iam.Role(
  "task-role",
  {
    name: `${namePrefix}-task-role`,
    assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({ Service: "ecs-tasks.amazonaws.com" }),
    tags,
  },
  { provider: primary },
);

const internalApiSecretResource = new aws.secretsmanager.Secret(
  "internal-api-secret",
  {
    name: `${namePrefix}/internal-api-secret`,
    recoveryWindowInDays: 0,
    tags,
  },
  { provider: primary },
);

new aws.secretsmanager.SecretVersion(
  "internal-api-secret-version",
  {
    secretId: internalApiSecretResource.id,
    secretString: internalApiSecret,
  },
  { provider: primary },
);

const secretHashKeyResource = new aws.secretsmanager.Secret(
  "secret-hash-key",
  {
    name: `${namePrefix}/secret-hash-key`,
    recoveryWindowInDays: 0,
    tags,
  },
  { provider: primary },
);

new aws.secretsmanager.SecretVersion(
  "secret-hash-key-version",
  {
    secretId: secretHashKeyResource.id,
    secretString: secretHashKey,
  },
  { provider: primary },
);

new aws.iam.RolePolicy(
  "task-execution-secret-read-policy",
  {
    role: taskExecutionRole.name,
    policy: pulumi
      .all([internalApiSecretResource.arn, secretHashKeyResource.arn])
      .apply(([internalApiSecretArn, secretHashKeyArn]) =>
        JSON.stringify({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: ["secretsmanager:GetSecretValue"],
              Resource: [internalApiSecretArn, secretHashKeyArn],
            },
          ],
        }),
      ),
  },
  { provider: primary },
);

const ecsOptimizedAmi = aws.ssm.getParameterOutput(
  {
    name: "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
  },
  { provider: primary },
);

const userData = pulumi.interpolate`#!/bin/bash
set -euxo pipefail

echo ECS_CLUSTER=${cluster.name} >> /etc/ecs/ecs.config
echo ECS_ENABLE_TASK_IAM_ROLE=true >> /etc/ecs/ecs.config
echo ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST=true >> /etc/ecs/ecs.config

echo "Ensuring persistent data volume is attached"
VOLUME_ID=${dataVolume.id}
REGION=${region}

TOKEN=$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/meta-data/instance-id")

for i in {1..60}; do
  STATE=$(aws ec2 describe-volumes --region "$REGION" --volume-ids "$VOLUME_ID" --query 'Volumes[0].State' --output text)
  if [ "$STATE" = "available" ] || [ "$STATE" = "in-use" ]; then
    break
  fi
  sleep 5
done

ATTACHED_INSTANCE=$(aws ec2 describe-volumes --region "$REGION" --volume-ids "$VOLUME_ID" --query 'Volumes[0].Attachments[0].InstanceId' --output text || true)
if [ "$ATTACHED_INSTANCE" = "None" ] || [ -z "$ATTACHED_INSTANCE" ]; then
  aws ec2 attach-volume --region "$REGION" --volume-id "$VOLUME_ID" --instance-id "$INSTANCE_ID" --device /dev/xvdf || true
fi

for i in {1..60}; do
  if [ -e /dev/xvdf ] || [ -e /dev/nvme1n1 ]; then
    break
  fi
  sleep 2
done

DEVICE="/dev/xvdf"
if [ -e /dev/nvme1n1 ]; then
  DEVICE="/dev/nvme1n1"
fi

if ! blkid "$DEVICE"; then
  mkfs.ext4 "$DEVICE"
fi

mkdir -p /mnt/data
if ! mountpoint -q /mnt/data; then
  mount "$DEVICE" /mnt/data
fi

if ! grep -q '/mnt/data' /etc/fstab; then
  echo "$DEVICE /mnt/data ext4 defaults,nofail 0 2" >> /etc/fstab
fi
`;

const launchTemplate = new aws.ec2.LaunchTemplate(
  "launch-template",
  {
    namePrefix: `${namePrefix}-lt-`,
    imageId: ecsOptimizedAmi.value,
    instanceType,
    iamInstanceProfile: {
      name: instanceProfile.name,
    },
    networkInterfaces: [
      {
        associatePublicIpAddress: "true",
        securityGroups: [instanceSecurityGroup.id],
      },
    ],
    metadataOptions: {
      httpEndpoint: "enabled",
      httpTokens: "required",
    },
    userData: userData.apply((script) => Buffer.from(script, "utf8").toString("base64")),
    tags,
  },
  { provider: primary },
);

const autoScalingGroup = new aws.autoscaling.Group(
  "asg",
  {
    name: `${namePrefix}-asg`,
    minSize: 1,
    maxSize: 1,
    desiredCapacity: 1,
    healthCheckType: "EC2",
    healthCheckGracePeriod: 300,
    vpcZoneIdentifiers: [publicSubnets[0].id],
    launchTemplate: {
      id: launchTemplate.id,
      version: "$Latest",
    },
    tags: [
      {
        key: "Name",
        value: `${namePrefix}-ecs-instance`,
        propagateAtLaunch: true,
      },
    ],
  },
  { provider: primary },
);

const capacityProvider = new aws.ecs.CapacityProvider(
  "capacity-provider",
  {
    name: `${namePrefix}-cp`,
    autoScalingGroupProvider: {
      autoScalingGroupArn: autoScalingGroup.arn,
      managedTerminationProtection: "DISABLED",
      managedScaling: {
        status: "DISABLED",
      },
    },
    tags,
  },
  { provider: primary },
);

const clusterCapacityProviders = new aws.ecs.ClusterCapacityProviders(
  "cluster-capacity-providers",
  {
    clusterName: cluster.name,
    capacityProviders: [capacityProvider.name],
    defaultCapacityProviderStrategies: [
      {
        capacityProvider: capacityProvider.name,
        weight: 1,
        base: 1,
      },
    ],
  },
  { provider: primary },
);

const logGroup = new aws.cloudwatch.LogGroup(
  "app-logs",
  {
    name: `/ecs/${namePrefix}`,
    retentionInDays: 7,
    tags,
  },
  { provider: primary },
);

const commandArgs = ["--port", String(appPort), "--data-root", dataRoot];
if (workerThreads !== undefined) {
  commandArgs.push("--worker-threads", String(workerThreads));
}

const taskDefinition = new aws.ecs.TaskDefinition(
  "task-definition",
  {
    family: `${namePrefix}-task`,
    requiresCompatibilities: ["EC2"],
    networkMode: "bridge",
    executionRoleArn: taskExecutionRole.arn,
    taskRoleArn: taskRole.arn,
    containerDefinitions: pulumi
      .all([logGroup.name, internalApiSecretResource.arn, secretHashKeyResource.arn])
      .apply(([logGroupName, internalApiSecretArn, secretHashKeyArn]) =>
        JSON.stringify([
          {
            name: "app",
            image: resolvedContainerImage,
            essential: true,
            memoryReservation: containerMemoryReservationMiB,
            memory: containerMemoryMiB,
            command: commandArgs,
            portMappings: [
              {
                containerPort: appPort,
                hostPort: appPort,
                protocol: "tcp",
              },
            ],
            mountPoints: [
              {
                sourceVolume: "data-volume",
                containerPath: "/mnt/data",
                readOnly: false,
              },
            ],
            environment: [{ name: "RUST_LOG", value: rustLog }],
            secrets: [
              { name: "JAZZ_INTERNAL_API_SECRET", valueFrom: internalApiSecretArn },
              { name: "JAZZ_SECRET_HASH_KEY", valueFrom: secretHashKeyArn },
            ],
            logConfiguration: {
              logDriver: "awslogs",
              options: {
                "awslogs-group": logGroupName,
                "awslogs-region": region,
                "awslogs-stream-prefix": "app",
              },
            },
          },
        ]),
      ),
    volumes: [
      {
        name: "data-volume",
        hostPath: "/mnt/data",
      },
    ],
    tags,
  },
  { provider: primary },
);

const loadBalancer = new aws.lb.LoadBalancer(
  "alb",
  {
    name: `${namePrefix}-alb`,
    loadBalancerType: "application",
    securityGroups: [albSecurityGroup.id],
    subnets: publicSubnets.map((subnet) => subnet.id),
    idleTimeout: 60,
    dropInvalidHeaderFields: true,
    tags,
  },
  { provider: primary },
);

const targetGroup = new aws.lb.TargetGroup(
  "tg",
  {
    name: `${namePrefix}-tg`,
    port: appPort,
    protocol: "HTTP",
    targetType: "instance",
    vpcId: vpc.id,
    healthCheck: {
      enabled: true,
      path: healthCheckPath,
      protocol: "HTTP",
      matcher: "200",
      interval: 30,
      timeout: 5,
      healthyThreshold: 2,
      unhealthyThreshold: 3,
    },
    tags,
  },
  { provider: primary },
);

const certificate = new aws.acm.Certificate(
  "tls-certificate",
  {
    domainName,
    validationMethod: "DNS",
    tags,
  },
  { provider: primary },
);

const certValidationOption = certificate.domainValidationOptions.apply((options) => {
  if (options.length === 0) {
    throw new Error("ACM did not return domain validation options");
  }
  return options[0];
});

const certValidationRecord = new aws.route53.Record(
  "tls-validation-record",
  {
    zoneId: rootZoneId,
    name: certValidationOption.apply((option) => option.resourceRecordName),
    type: certValidationOption.apply((option) => option.resourceRecordType),
    ttl: 60,
    records: [certValidationOption.apply((option) => option.resourceRecordValue)],
    allowOverwrite: true,
  },
  { provider: dnsProvider },
);

const certificateValidation = new aws.acm.CertificateValidation(
  "tls-certificate-validation",
  {
    certificateArn: certificate.arn,
    validationRecordFqdns: [certValidationRecord.fqdn],
  },
  { provider: primary },
);

const httpsListener = new aws.lb.Listener(
  "https-listener",
  {
    loadBalancerArn: loadBalancer.arn,
    port: 443,
    protocol: "HTTPS",
    sslPolicy: "ELBSecurityPolicy-TLS13-1-2-2021-06",
    certificateArn: certificateValidation.certificateArn,
    defaultActions: [{ type: "forward", targetGroupArn: targetGroup.arn }],
  },
  { provider: primary },
);

new aws.lb.Listener(
  "http-listener",
  {
    loadBalancerArn: loadBalancer.arn,
    port: 80,
    protocol: "HTTP",
    defaultActions: [
      {
        type: "redirect",
        redirect: {
          port: "443",
          protocol: "HTTPS",
          statusCode: "HTTP_301",
        },
      },
    ],
  },
  { provider: primary },
);

const service = new aws.ecs.Service(
  "service",
  {
    name: `${namePrefix}-service`,
    cluster: cluster.arn,
    desiredCount: 1,
    taskDefinition: taskDefinition.arn,
    deploymentMinimumHealthyPercent: 0,
    deploymentMaximumPercent: 100,
    healthCheckGracePeriodSeconds: 300,
    forceNewDeployment: true,
    capacityProviderStrategies: [
      {
        capacityProvider: capacityProvider.name,
        weight: 1,
        base: 1,
      },
    ],
    loadBalancers: [
      {
        targetGroupArn: targetGroup.arn,
        containerName: "app",
        containerPort: appPort,
      },
    ],
    enableEcsManagedTags: true,
    waitForSteadyState: true,
    tags,
  },
  {
    provider: primary,
    dependsOn: [httpsListener, clusterCapacityProviders],
  },
);

new aws.route53.Record(
  "cloud2-alias-record",
  {
    zoneId: rootZoneId,
    name: domainName,
    type: "A",
    aliases: [
      {
        name: loadBalancer.dnsName,
        zoneId: loadBalancer.zoneId,
        evaluateTargetHealth: true,
      },
    ],
    allowOverwrite: true,
  },
  { provider: dnsProvider },
);

// TODO: Restrict /internal/apps/* to internal networks or an authenticated private ingress layer.

export const url = pulumi.interpolate`https://${domainName}`;
export const dnsName = domainName;
export const rootHostedZoneId = rootZoneId;
export const albDnsName = loadBalancer.dnsName;
export const clusterName = cluster.name;
export const ecsServiceName = service.name;
export const ecsCapacityProviderName = capacityProvider.name;
export const dataVolumeId = dataVolume.id;
