import { setTimeout as sleepTimer } from "node:timers/promises";
import { pathToFileURL } from "node:url";

const RUNBOOK = "See packages/inspector/README.md, 'Staging a release on Vercel'.";

// Read-only: proves the token can read the configured project in the configured
// team before a release depends on it.
export async function preflightInspectorProject({
  env = process.env,
  fetchImpl = globalThis.fetch,
  log = console.log,
  requestTimeoutMs = 10_000,
} = {}) {
  for (const key of ["VERCEL_ORG_ID", "VERCEL_PROJECT_ID", "VERCEL_TOKEN"]) {
    if (!env[key])
      throw new Error(`Missing ${key}. Configure the matching VERCEL_INSPECTOR_* GitHub secret.`);
  }
  const team = new URLSearchParams({ teamId: env.VERCEL_ORG_ID });
  const projectUrl = `https://api.vercel.com/v9/projects/${encodeURIComponent(env.VERCEL_PROJECT_ID)}`;
  // Promotion also reads the project's domains, so prove that read here too.
  async function read(path, what) {
    const response = await fetchImpl(`${projectUrl}${path}?${team}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${env.VERCEL_TOKEN}` },
      signal: AbortSignal.timeout(requestTimeoutMs),
    });
    if (!response.ok) {
      throw new Error(
        `VERCEL_INSPECTOR_TOKEN cannot read the Inspector ${what} in VERCEL_INSPECTOR_ORG_ID (${response.status}). ` +
          `Give the token access to that team and project, or fix the project/team IDs. ${RUNBOOK}`,
      );
    }
    try {
      return await response.json();
    } catch {
      throw new Error("Vercel GET request returned invalid JSON.");
    }
  }
  const project = await read("", "project");
  if (project.id !== env.VERCEL_PROJECT_ID || project.accountId !== env.VERCEL_ORG_ID) {
    throw new Error(
      `VERCEL_INSPECTOR_PROJECT_ID does not belong to VERCEL_INSPECTOR_ORG_ID. ${RUNBOOK}`,
    );
  }
  const domains = await read("/domains", "project domains");
  if (!Array.isArray(domains.domains)) {
    throw new Error("Inspector production domain list is malformed.");
  }
  log("Inspector Vercel token can read the configured project, team and domains.");
}

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
    if (Date.now() >= deadline) throw new Error("Timed out verifying inspector promotion.");
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
    if (method === "GET") {
      try {
        return await response.json();
      } catch {
        throw new Error("Vercel GET request returned invalid JSON.");
      }
    }
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
  // Public project responses can omit lastAliasRequest. Verify the observable
  // target state and each verified production domain instead of requiring it.
  // API contracts: https://vercel.com/docs/rest-api/projects/retrieve-project-domains-by-project-by-id-or-name
  // and https://vercel.com/docs/rest-api/aliases/get-an-alias
  // Ids and states only, so a timeout says what production still points at.
  let lastObserved = "nothing observed";
  async function productionIsVerified(current, { beforePromotion = false } = {}) {
    const target = current.targets?.production;
    const alias = current.lastAliasRequest;
    lastObserved = [
      `production target ${target?.id ?? "none"}`,
      `substate=${target?.readySubstate ?? "unknown"}`,
      `aliasAssigned=${target?.aliasAssigned ?? "unknown"}`,
      `aliasError=${target?.aliasError?.code ?? (target?.aliasError ? "yes" : "none")}`,
      `lastAliasRequest=${alias ? `${alias.toDeploymentId}:${alias.jobStatus}` : "none"}`,
    ].join(" ");
    if (
      alias?.toDeploymentId === deployment.id &&
      ["failed", "skipped"].includes(alias.jobStatus)
    ) {
      // A prior failed attempt must not prevent an explicit retry.
      if (beforePromotion) return false;
      throw new Error(`Inspector promotion ${alias.jobStatus}.`);
    }
    if (
      current.id !== project.id ||
      current.accountId !== project.accountId ||
      target?.id !== deployment.id ||
      target.readyState !== "READY" ||
      target.readySubstate !== "PROMOTED" ||
      target.aliasError ||
      !(
        target.aliasAssigned === true ||
        (typeof target.aliasAssigned === "number" && target.aliasAssigned > 0)
      ) ||
      (alias?.toDeploymentId === deployment.id && alias.jobStatus !== "succeeded")
    )
      return false;
    const domains = await request(`${projectPath}/domains`);
    // Fail closed on pagination: don't claim verification from a partial list.
    if (!Array.isArray(domains.domains) || domains.pagination?.next != null) {
      throw new Error("Inspector production domain list is malformed or incomplete.");
    }
    const productionDomains = domains.domains.filter(
      (domain) =>
        domain.verified === true &&
        !domain.gitBranch &&
        !domain.customEnvironmentId &&
        !domain.redirect,
    );
    if (productionDomains.length === 0)
      throw new Error("Inspector has no verified production domains.");
    for (const domain of productionDomains) {
      if (domain.projectId !== project.id || typeof domain.name !== "string" || !domain.name) {
        throw new Error("Inspector production domain identity mismatch.");
      }
      const assigned = await request(`/v4/aliases/${encodeURIComponent(domain.name)}`);
      if (
        assigned.projectId !== project.id ||
        assigned.alias !== domain.name ||
        assigned.deploymentId !== deployment.id ||
        assigned.redirect
      ) {
        lastObserved = `domain ${domain.name} points at ${assigned.deploymentId ?? "nothing"}`;
        return false;
      }
    }
    // Domains may take time to move, and the target may change while checking.
    const confirmed = await request(projectPath);
    lastObserved = `production target moved to ${confirmed.targets?.production?.id ?? "none"}`;
    return (
      !confirmed.rollingRelease &&
      confirmed.id === project.id &&
      confirmed.accountId === project.accountId &&
      confirmed.targets?.production?.id === deployment.id
    );
  }
  if (await productionIsVerified(project, { beforePromotion: true })) {
    log("Inspector deployment is already the verified production target.");
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
    if (await productionIsVerified(current)) {
      log("Inspector production target and domain aliases verified after promotion.");
      return;
    }
    if (attempt + 1 < attempts) await sleep(Math.min(delayMs, Math.max(0, deadline - Date.now())));
  }
  throw new Error(
    `Timed out verifying inspector production target after promotion to ${deployment.id}. ` +
      `Last observed: ${lastObserved}. ${RUNBOOK}`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const run =
    process.argv[2] === "preflight" ? preflightInspectorProject : promoteInspectorDeployment;
  run().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
