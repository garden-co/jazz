import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const workspaceRoot = path.resolve(fileURLToPath(new URL("../..", import.meta.url)));
const workspacePackageDirs = ["packages", "crates"];

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeJson(file, value) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function readWorkspaceConfig(root) {
  const workspaceYamlPath = path.join(root, "pnpm-workspace.yaml");
  const parsed = parseYaml(fs.readFileSync(workspaceYamlPath, "utf8"));
  const catalogs = { ...(parsed?.catalogs ?? {}) };
  if (parsed?.catalog && catalogs.default === undefined) catalogs.default = parsed.catalog;
  return { catalogs };
}

function resolveWorkspacePackageVersion(packageName, root) {
  for (const subdir of workspacePackageDirs) {
    const candidate = path.join(root, subdir, packageName, "package.json");
    if (!fs.existsSync(candidate)) continue;
    const version = readJson(candidate).version;
    if (typeof version !== "string" || version.length === 0) {
      throw new Error(`Workspace package "${packageName}" is missing a version string`);
    }
    return version;
  }
  throw new Error(`Workspace package "${packageName}" was not found under packages/ or crates/`);
}

function resolveWorkspaceRange(packageName, value, root) {
  const suffix = value.slice("workspace:".length);
  if (/^[0-9]/.test(suffix)) return suffix;
  const version = resolveWorkspacePackageVersion(packageName, root);
  if (suffix === "*") return `^${version}`;
  if (suffix === "^") return `^${version}`;
  if (suffix === "~") return `~${version}`;
  throw new Error(`Unrecognised workspace range for "${packageName}": ${value}`);
}

function resolveCatalogRange(packageName, value, config) {
  const catalogName = value.slice("catalog:".length);
  const catalog = config.catalogs?.[catalogName];
  if (!catalog) throw new Error(`Catalog "${catalogName}" is not defined for "${packageName}"`);
  const resolved = catalog[packageName];
  if (typeof resolved !== "string" || resolved.length === 0) {
    throw new Error(`Catalog "${catalogName}" has no entry for "${packageName}"`);
  }
  return resolved;
}

function resolveRecord(record, root, config) {
  if (!record) return record;
  return Object.fromEntries(
    Object.entries(record).map(([packageName, value]) => {
      if (typeof value !== "string") return [packageName, value];
      if (value.startsWith("workspace:")) {
        return [packageName, resolveWorkspaceRange(packageName, value, root)];
      }
      if (value.startsWith("catalog:")) {
        return [packageName, resolveCatalogRange(packageName, value, config)];
      }
      return [packageName, value];
    }),
  );
}

export function materializeJazzRnConsumerManifest(manifest, tarball, root = workspaceRoot) {
  const workspaceConfig = readWorkspaceConfig(root);
  const nextManifest = structuredClone(manifest);
  nextManifest.dependencies = { ...(nextManifest.dependencies ?? {}) };
  nextManifest.dependencies["jazz-rn"] = `file:${tarball}`;
  delete nextManifest.dependencies["jazz-tools"];
  nextManifest.dependencies = resolveRecord(nextManifest.dependencies, root, workspaceConfig);
  nextManifest.devDependencies = resolveRecord(nextManifest.devDependencies, root, workspaceConfig);
  return nextManifest;
}

export function prepareJazzRnConsumerFixture({
  fixtureSource,
  fixtureDestination,
  tarball,
  root = workspaceRoot,
}) {
  fs.cpSync(fixtureSource, fixtureDestination, { recursive: true });
  const packageJsonPath = path.join(fixtureDestination, "package.json");
  const manifest = readJson(packageJsonPath);
  writeJson(packageJsonPath, materializeJazzRnConsumerManifest(manifest, tarball, root));
}

function main(argv) {
  const [fixtureSource, fixtureDestination, tarball] = argv;
  if (!fixtureSource || !fixtureDestination || !tarball) {
    throw new Error(
      "usage: node dev/scripts/prepare-jazz-rn-consumer-fixture.mjs <fixture-source> <fixture-destination> <jazz-rn-tarball>",
    );
  }
  prepareJazzRnConsumerFixture({
    fixtureSource: path.resolve(fixtureSource),
    fixtureDestination: path.resolve(fixtureDestination),
    tarball: path.resolve(tarball),
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2));
}
