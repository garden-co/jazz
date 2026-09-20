import { setTimeout as sleepTimer } from "node:timers/promises";
import { pathToFileURL } from "node:url";

// The same promotion endpoint used by Vercel CLI, scoped without user/team discovery.
export async function promoteInspectorDeployment({
  env = process.env,
  fetchImpl = globalThis.fetch,
  sleep = sleepTimer,
  log = console.log,
  attempts = 60,
  delayMs = 5_000,
  requestTimeoutMs = 10_000,
} = {}) {
  for (const key of [
    "VERCEL_ORG_ID",
    "VERCEL_PROJECT_ID",
    "VERCEL_TOKEN",
    "DEPLOYMENT_URL",
    "DEPLOY_SHA",
  ]) {
    if (!env[key]) throw new Error(`Missing required environment variable ${key}.`);
  }
  const deadline = Date.now() + 5 * 60_000;
  const base = "https://api.vercel.com";
  const team = new URLSearchParams({ teamId: env.VERCEL_ORG_ID });
  const projectPath = `/v9/projects/${encodeURIComponent(env.VERCEL_PROJECT_ID)}`;
  async function request(path, method = "GET") {
    const response = await fetchImpl(`${base}${path}?${team}`, {
      method,
      headers: {
        Authorization: `Bearer ${env.VERCEL_TOKEN}`,
        "Content-Type": "application/json",
      },
      ...(method === "POST" ? { body: "{}" } : {}),
      signal: AbortSignal.timeout(Math.max(1, Math.min(requestTimeoutMs, deadline - Date.now()))),
    });
    // API response bodies can contain sensitive details; never include them in errors.
    if (!response.ok) throw new Error(`Vercel ${method} request failed (${response.status}).`);
    return method === "GET" ? response.json() : undefined;
  }
  const url = new URL(env.DEPLOYMENT_URL);
  if (
    url.protocol !== "https:" ||
    !url.hostname.endsWith(".vercel.app") ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    url.username ||
    url.password ||
    url.port
  ) {
    throw new Error("Invalid inspector deployment URL.");
  }
  const deployment = await request(`/v13/deployments/${encodeURIComponent(url.hostname)}`);
  const project = await request(projectPath);
  if (
    project.id !== env.VERCEL_PROJECT_ID ||
    project.accountId !== env.VERCEL_ORG_ID ||
    deployment.projectId !== project.id ||
    deployment.ownerId !== env.VERCEL_ORG_ID ||
    !deployment.id ||
    deployment.url !== url.hostname ||
    deployment.target !== "production" ||
    deployment.readyState !== "READY" ||
    (deployment.meta?.githubCommitSha ?? deployment.gitSource?.sha ?? deployment.sha) !==
      env.DEPLOY_SHA ||
    !["STAGED", "PROMOTED"].includes(deployment.readySubstate)
  ) {
    throw new Error("Inspector deployment identity or production readiness mismatch.");
  }
  if (project.rollingRelease)
    throw new Error("Inspector promotion does not support rolling releases.");
  if (env.DRY_RUN === "true") {
    log("Inspector promotion dry run verified deployment and project; no changes requested.");
    return;
  }
  if (
    project.targets?.production?.id === deployment.id &&
    project.lastAliasRequest?.toDeploymentId === deployment.id &&
    project.lastAliasRequest?.jobStatus === "succeeded"
  ) {
    log("Inspector deployment is already the production target.");
    return;
  }
  await request(
    `/v10/projects/${encodeURIComponent(project.id)}/promote/${encodeURIComponent(deployment.id)}`,
    "POST",
  );
  for (let attempt = 0; attempt < attempts && Date.now() < deadline; attempt++) {
    const current = await request(projectPath);
    if (current.rollingRelease)
      throw new Error("Inspector promotion does not support rolling releases.");
    if (
      current.targets?.production?.id === deployment.id &&
      current.lastAliasRequest?.toDeploymentId === deployment.id &&
      current.lastAliasRequest?.jobStatus === "succeeded"
    ) {
      log("Inspector production target verified after promotion.");
      return;
    }
    const alias = current.lastAliasRequest;
    if (
      alias?.toDeploymentId === deployment.id &&
      ["failed", "skipped"].includes(alias.jobStatus)
    ) {
      throw new Error(`Inspector promotion ${alias.jobStatus}.`);
    }
    if (attempt + 1 < attempts) await sleep(Math.min(delayMs, Math.max(0, deadline - Date.now())));
  }
  throw new Error("Timed out verifying inspector production target after promotion.");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  promoteInspectorDeployment().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
