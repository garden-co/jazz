import * as fs from "node:fs";
import * as path from "node:path";
import { randomBytes } from "node:crypto";
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
      if (!["*", "^", "~"].includes(suffix) && !isVersionPin(suffix))
        throw badWorkspaceForm(name, value);
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
      const version = await fetchPackageVersion(name);
      fetchedVersions.set(name, version);
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
// Local fetcher
// --------------------------------------------------------------------------

/** Rewrite a starter manifest's workspace: and catalog: ranges to the versions in this repo. */
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

// --------------------------------------------------------------------------
// Scaffold
// --------------------------------------------------------------------------

const SCAFFOLD_COPY_SKIP = new Set(["node_modules", ".next", ".jazz", ".turbo", ".env", ".git"]);

/**
 * Copy `starters/<starter>` into `targetDir` the way a user would receive it:
 * concrete dependency versions, the app's own package name, and a generated
 * BETTER_AUTH_SECRET for starters that use Better Auth. Install is left to the
 * caller, which pins workspace tarballs first.
 */
export async function scaffoldStarter(opts: {
  repoRoot: string;
  starter: string;
  appName: string;
  targetDir: string;
}): Promise<void> {
  const starterDir = path.join(opts.repoRoot, "starters", opts.starter);
  if (!fs.existsSync(starterDir)) throw new Error(`Unknown starter "${opts.starter}"`);
  if (fs.existsSync(opts.targetDir)) {
    throw new Error(`Target directory "${opts.targetDir}" already exists`);
  }
  await fs.promises.cp(starterDir, opts.targetDir, {
    recursive: true,
    filter: (src) => !SCAFFOLD_COPY_SKIP.has(path.basename(src)),
  });

  const pkgJsonPath = path.join(opts.targetDir, "package.json");
  const rawManifest = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8")) as PackageManifest;
  const resolved = await resolveLocalDeps(rawManifest, opts.repoRoot);
  fs.writeFileSync(
    pkgJsonPath,
    JSON.stringify({ ...resolved, name: opts.appName }, null, 2) + "\n",
    "utf-8",
  );

  if (opts.starter.endsWith("-betterauth") || opts.starter.endsWith("-hybrid")) {
    const secret = randomBytes(32).toString("base64url");
    fs.writeFileSync(path.join(opts.targetDir, ".env"), `BETTER_AUTH_SECRET=${secret}\n`, "utf-8");
  }
}
