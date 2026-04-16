import * as fs from "node:fs";
import * as path from "node:path";
import { parse as parseYaml } from "yaml";

export type PackageManifest = {
  name?: string;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  [key: string]: unknown;
};

type WorkspaceConfig = {
  catalogs?: Record<string, Record<string, string>>;
};

type FetchPackageVersion = (packageName: string) => Promise<string>;

const WORKSPACE_SUBDIRS = ["packages", "crates"] as const;
const FETCH_TIMEOUT_MS = 8_000;

// --------------------------------------------------------------------------
// Pure resolver
// --------------------------------------------------------------------------

function isVersionPin(suffix: string): boolean {
  return /^[0-9]/.test(suffix);
}

function badWorkspaceForm(depName: string, value: string): Error {
  return new Error(`Unrecognised workspace: range form for "${depName}": ${value}`);
}

function resolveWorkspaceValue(depName: string, suffix: string, fetchedVersion: string): string {
  if (suffix === "*" || suffix === "^") return `^${fetchedVersion}`;
  if (suffix === "~") return `~${fetchedVersion}`;
  throw badWorkspaceForm(depName, `workspace:${suffix}`);
}

function resolveCatalogValue(
  depName: string,
  value: string,
  workspaceConfig: WorkspaceConfig,
): string {
  const catalogName = value.slice("catalog:".length);
  if (!workspaceConfig.catalogs) {
    throw new Error(
      `Cannot resolve "catalog:${catalogName}" for "${depName}" — workspaceConfig has no catalogs field`,
    );
  }
  const catalog = workspaceConfig.catalogs[catalogName];
  if (!catalog) {
    throw new Error(`Catalog "${catalogName}" not found in workspaceConfig (dep: "${depName}")`);
  }
  if (!(depName in catalog)) {
    throw new Error(`Dep "${depName}" not found in catalog "${catalogName}"`);
  }
  return catalog[depName];
}

async function applyResolvedManifest(
  manifest: PackageManifest,
  workspaceConfig: WorkspaceConfig,
  fetchPackageVersion: FetchPackageVersion,
): Promise<PackageManifest> {
  const deps = manifest.dependencies ?? {};
  const devDeps = manifest.devDependencies ?? {};

  const toFetch = new Set<string>();
  for (const [name, value] of [...Object.entries(deps), ...Object.entries(devDeps)]) {
    if (value.startsWith("workspace:")) {
      const suffix = value.slice("workspace:".length);
      if (suffix === "*" || suffix === "^" || suffix === "~") {
        toFetch.add(name);
      } else if (!isVersionPin(suffix)) {
        throw badWorkspaceForm(name, value);
      }
    }
  }

  const fetchedVersions = new Map<string, string>();
  await Promise.all(
    [...toFetch].map(async (name) => {
      fetchedVersions.set(name, await fetchPackageVersion(name));
    }),
  );

  function resolveEntry(depName: string, value: string): string {
    if (value.startsWith("workspace:")) {
      const suffix = value.slice("workspace:".length);
      if (isVersionPin(suffix)) return suffix;
      const version = fetchedVersions.get(depName);
      if (!version) throw new Error(`Internal: no fetched version for "${depName}"`);
      return resolveWorkspaceValue(depName, suffix, version);
    }
    if (value.startsWith("catalog:")) {
      return resolveCatalogValue(depName, value, workspaceConfig);
    }
    if (/^[a-z][a-z0-9+.-]*:/i.test(value)) {
      throw new Error(`Unrecognised protocol specifier for "${depName}": ${value}`);
    }
    return value;
  }

  function resolveRecord(record: Record<string, string>): Record<string, string> {
    return Object.fromEntries(
      Object.entries(record).map(([name, value]) => [name, resolveEntry(name, value)]),
    );
  }

  const result: PackageManifest = { ...manifest };
  if (manifest.dependencies) result.dependencies = resolveRecord(manifest.dependencies);
  if (manifest.devDependencies) result.devDependencies = resolveRecord(manifest.devDependencies);
  return result;
}

// --------------------------------------------------------------------------
// Workspace YAML parsing
// --------------------------------------------------------------------------

interface WorkspaceYamlShape {
  catalog?: Record<string, string>;
  catalogs?: Record<string, Record<string, string>>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isStringRecord(value: unknown): value is Record<string, string> {
  if (!isRecord(value)) return false;
  for (const v of Object.values(value)) {
    if (typeof v !== "string") return false;
  }
  return true;
}

function parseWorkspaceYaml(text: string, source: string): WorkspaceYamlShape {
  let raw: unknown;
  try {
    raw = parseYaml(text);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`Failed to parse ${source} as YAML: ${msg}`);
  }
  if (!isRecord(raw)) {
    const got = raw === null ? "null" : Array.isArray(raw) ? "array" : typeof raw;
    throw new Error(`${source} did not parse to an object (got ${got})`);
  }

  const result: WorkspaceYamlShape = {};
  if ("catalog" in raw) {
    if (!isStringRecord(raw.catalog)) {
      throw new Error(`${source}: "catalog" must be a mapping of package -> version string`);
    }
    result.catalog = raw.catalog;
  }
  if ("catalogs" in raw) {
    if (!isRecord(raw.catalogs)) {
      throw new Error(`${source}: "catalogs" must be a mapping`);
    }
    const catalogs: Record<string, Record<string, string>> = {};
    for (const [name, entries] of Object.entries(raw.catalogs)) {
      if (!isStringRecord(entries)) {
        throw new Error(
          `${source}: "catalogs.${name}" must be a mapping of package -> version string`,
        );
      }
      catalogs[name] = entries;
    }
    result.catalogs = catalogs;
  }
  return result;
}

function buildWorkspaceConfig(parsed: WorkspaceYamlShape): WorkspaceConfig {
  const workspaceConfig: WorkspaceConfig = { catalogs: parsed.catalogs };
  if (parsed.catalog && !workspaceConfig.catalogs?.["default"]) {
    workspaceConfig.catalogs = { ...workspaceConfig.catalogs, default: parsed.catalog };
  }
  return workspaceConfig;
}

// --------------------------------------------------------------------------
// Remote fetcher (GitHub raw)
// --------------------------------------------------------------------------

async function fetchWithTimeout(url: string): Promise<Response> {
  try {
    return await globalThis.fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  } catch (err) {
    if (err instanceof Error && err.name === "TimeoutError") {
      throw new Error(`Request to ${url} timed out after ${FETCH_TIMEOUT_MS}ms`);
    }
    throw err;
  }
}

/** Fetch with a single retry on transient failure. 404s are not retried. */
async function fetchOnce(url: string): Promise<Response> {
  try {
    return await fetchWithTimeout(url);
  } catch {
    return await fetchWithTimeout(url);
  }
}

export async function resolveRemoteDeps(
  manifest: PackageManifest,
  repoConfig: { repo: string; branch: string },
): Promise<PackageManifest> {
  const rawBase = `https://raw.githubusercontent.com/${repoConfig.repo}/refs/heads/${repoConfig.branch}`;
  const workspaceUrl = `${rawBase}/pnpm-workspace.yaml`;

  const wsRes = await fetchOnce(workspaceUrl);
  if (!wsRes.ok) throw new Error(`Failed to fetch pnpm-workspace.yaml: ${wsRes.status}`);
  const parsed = parseWorkspaceYaml(await wsRes.text(), workspaceUrl);
  const workspaceConfig = buildWorkspaceConfig(parsed);

  // Race packages/ and crates/ in parallel; take the first ok response.
  // Non-404 failures surface as errors rather than silently falling through.
  const fetchPackageVersion: FetchPackageVersion = async (name) => {
    const responses = await Promise.all(
      WORKSPACE_SUBDIRS.map((subdir) => fetchOnce(`${rawBase}/${subdir}/${name}/package.json`)),
    );
    for (const res of responses) {
      if (!res.ok) continue;
      const pkg = (await res.json()) as { version?: unknown };
      if (typeof pkg.version !== "string" || !pkg.version) {
        throw new Error(`Package "${name}" has no version field in upstream package.json`);
      }
      return pkg.version;
    }
    const transient = responses.find((r) => !r.ok && r.status !== 404);
    if (transient) {
      throw new Error(
        `GitHub returned ${transient.status} when looking up "${name}" in upstream repo`,
      );
    }
    throw new Error(`Package "${name}" not found in upstream repo`);
  };

  return applyResolvedManifest(manifest, workspaceConfig, fetchPackageVersion);
}

// --------------------------------------------------------------------------
// Local fetcher (dev-mode via JAZZ_STARTER_PATH)
// --------------------------------------------------------------------------

export async function resolveLocalDeps(
  manifest: PackageManifest,
  repoRoot: string,
): Promise<PackageManifest> {
  const wsPath = path.join(repoRoot, "pnpm-workspace.yaml");
  const wsYaml = fs.readFileSync(wsPath, "utf-8");
  const parsed = parseWorkspaceYaml(wsYaml, wsPath);
  const workspaceConfig = buildWorkspaceConfig(parsed);

  const fetchPackageVersion: FetchPackageVersion = async (name) => {
    for (const subdir of WORKSPACE_SUBDIRS) {
      const candidate = path.join(repoRoot, subdir, name, "package.json");
      if (fs.existsSync(candidate)) {
        const pkgJson = JSON.parse(fs.readFileSync(candidate, "utf-8")) as { version: string };
        return pkgJson.version;
      }
    }
    throw new Error(`Package "${name}" not found in any workspace subdir`);
  };

  return applyResolvedManifest(manifest, workspaceConfig, fetchPackageVersion);
}
