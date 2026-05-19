import fs from "node:fs";
import { setTimeout as sleepTimer } from "node:timers/promises";
import { pathToFileURL } from "node:url";

const REQUIRED_ENV = ["VERCEL_ORG_ID", "VERCEL_PROJECT_ID", "VERCEL_TOKEN", "GITHUB_OUTPUT"];

const DEFAULT_ATTEMPTS = 30;
const DEFAULT_DELAY_MS = 20_000;

function requireEnv(env) {
  for (const name of REQUIRED_ENV) {
    if (!env[name]) {
      throw new Error(`Missing required environment variable ${name}.`);
    }
  }
}

export function buildDeploymentsUrl(env) {
  const params = new URLSearchParams({
    projectId: env.VERCEL_PROJECT_ID,
    target: "production",
    state: "READY",
    branch: "main",
    limit: "20",
    teamId: env.VERCEL_ORG_ID,
  });

  return `https://api.vercel.com/v6/deployments?${params}`;
}

export function describeDeployment(deployment) {
  return [
    deployment.url,
    `state=${deployment.readyState ?? "unknown"}`,
    `target=${deployment.target ?? "unknown"}`,
    `substate=${deployment.readySubstate ?? "unknown"}`,
  ].join(" ");
}

function findDeploymentToPromote(deployments) {
  const latest = deployments.find(
    (deployment) =>
      deployment.url &&
      (deployment.readySubstate === "STAGED" || deployment.readySubstate === "PROMOTED"),
  );

  if (!latest) {
    return null;
  }

  return {
    deployment: latest,
    alreadyPromoted: latest.readySubstate === "PROMOTED",
  };
}

async function listDeployments({ fetchImpl, listUrl, token }) {
  const response = await fetchImpl(listUrl, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Vercel deployment lookup failed (${response.status}): ${body}`);
  }

  const body = await response.json();
  return Array.isArray(body.deployments) ? body.deployments : [];
}

function writeGithubOutput(outputFile, result) {
  const output = [
    `deployment_url=${result.deploymentUrl}`,
    `already_promoted=${result.alreadyPromoted ? "true" : "false"}`,
  ].join("\n");

  fs.appendFileSync(outputFile, `${output}\n`);
}

export async function resolveInspectorDeployment({
  env = process.env,
  fetchImpl = globalThis.fetch,
  log = console.log,
  attempts = DEFAULT_ATTEMPTS,
  delayMs = DEFAULT_DELAY_MS,
  sleep = sleepTimer,
} = {}) {
  requireEnv(env);

  const listUrl = buildDeploymentsUrl(env);
  let lastDeployments = [];

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    lastDeployments = await listDeployments({
      fetchImpl,
      listUrl,
      token: env.VERCEL_TOKEN,
    });

    const match = findDeploymentToPromote(lastDeployments);
    if (match) {
      const deploymentUrl = `https://${match.deployment.url}`;
      const result = {
        deploymentUrl,
        alreadyPromoted: match.alreadyPromoted,
      };

      writeGithubOutput(env.GITHUB_OUTPUT, result);
      log(`Resolved inspector deployment: ${describeDeployment(match.deployment)}`);
      return result;
    }

    log(`No staged inspector production deployment on main yet (${attempt}/${attempts}).`);

    if (lastDeployments.length > 0) {
      log("Matching READY production deployments:");
      for (const deployment of lastDeployments) {
        log(`- ${describeDeployment(deployment)}`);
      }
    }

    if (attempt < attempts) {
      await sleep(delayMs);
    }
  }

  throw new Error(
    [
      "No staged inspector production deployment found on main.",
      "Make sure the inspector Vercel project has a ready production deployment from main and has auto-assign custom production domains disabled.",
      lastDeployments.length > 0
        ? `Last matching deployments:\n${lastDeployments
            .map((deployment) => `- ${describeDeployment(deployment)}`)
            .join("\n")}`
        : "No matching READY production deployments were returned by Vercel.",
    ].join("\n"),
  );
}

async function main() {
  await resolveInspectorDeployment();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
