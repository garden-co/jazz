// Shared helpers for the Neon Realtime ports: one Neon project in aws-us-east-1
// (Realtime's beta regions), one branch per hero app. Needs a Neon login: an
// API key in NEON_API_KEY, or the CLI's own `neon login`.
import { existsSync, readFileSync } from "node:fs";
import { sh } from "./vite-app.mjs";

export const NEON_PROJECT_NAME = process.env.NEON_PROJECT_NAME ?? "jazz-app-comparisons";
export const NEON_REGION = "aws-us-east-1";

/** NEON_API_KEY, or undefined for the CLI's own login (neon login). */
const apiKey = () => process.env.NEON_API_KEY || undefined;

function parseEnv(file) {
  if (!existsSync(file)) return {};
  return Object.fromEntries(
    readFileSync(file, "utf8")
      .split("\n")
      .filter((line) => /^[A-Z_]+=/.test(line))
      .map((line) => {
        const i = line.indexOf("=");
        return [line.slice(0, i), line.slice(i + 1).replace(/^"|"$/g, "")];
      }),
  );
}

/**
 * Installs dependencies, links the app directory to its branch of the Neon
 * project (creating project and branch on first use), applies the app's
 * neon.ts (Realtime + allowed origins, Managed Better Auth where used) and
 * pulls the branch's env into .env.local. Returns that env.
 */
export function ensureNeon(dir, { branch, services = ["postgres", "realtime"], authOrigins = [] }) {
  if (!existsSync(`${dir}/node_modules`)) sh(dir, "pnpm install --frozen-lockfile >&2");
  const key = apiKey();
  const env = key ? { NEON_API_KEY: key } : {};
  const file = `${dir}/.env.local`;
  const need = ["DATABASE_URL", "NEON_REALTIME_URL", "NEON_REALTIME_SECRET", ...(services.includes("auth") ? ["NEON_AUTH_BASE_URL"] : [])];
  if (!need.every((key) => parseEnv(file)[key])) {
    const neon = (args) => sh(dir, `pnpm exec neon ${args} >&2`, env);
    const projects = JSON.parse(sh(dir, "pnpm exec neon projects list --output json", env));
    const list = Array.isArray(projects) ? projects : (projects.projects ?? []);
    let project = list.find((p) => p.name === NEON_PROJECT_NAME);
    if (!project) {
      neon(`link --yes --project-name ${NEON_PROJECT_NAME} --region-id ${NEON_REGION}`);
      const again = JSON.parse(sh(dir, "pnpm exec neon projects list --output json", env));
      project = (Array.isArray(again) ? again : again.projects).find((p) => p.name === NEON_PROJECT_NAME);
    }
    if (project.region_id !== NEON_REGION) throw new Error(`${NEON_PROJECT_NAME} is in ${project.region_id}, not ${NEON_REGION}`);
    const branches = JSON.parse(sh(dir, `pnpm exec neon branches list --project-id ${project.id} --output json`, env));
    const branchList = Array.isArray(branches) ? branches : branches.branches;
    if (!branchList.some((b) => b.name === branch)) neon(`branches create --project-id ${project.id} --name ${branch}`);
    neon(`link --yes --project-id ${project.id} --branch ${branch}`);
    neon("deploy --yes");
    neon(`env pull ${services.map((s) => `--service ${s}`).join(" ")}`);
    // Managed Better Auth accepts sign-ins only from trusted origins (localhost is pre-trusted).
    if (authOrigins.length) {
      const trusted = sh(dir, "pnpm exec neon neon-auth domain list", env);
      for (const origin of authOrigins) if (!trusted.includes(origin)) neon(`neon-auth domain add ${origin}`);
    }
  }
  return { ...parseEnv(file), ...env };
}
