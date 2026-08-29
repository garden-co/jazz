#!/usr/bin/env node
/**
 * Persistence serializer boundary. Rust is tokenized, rather than searched by
 * regular expression, so comments, raw identifiers, aliases, macro arguments,
 * type paths, and function-item references cannot evade this policy.
 */
import fs from "node:fs";
import path from "node:path";
import childProcess from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  describeEndpoint,
  rustTokens,
  serializerExternCrates,
  serializerImports,
  serializerReferences,
} from "./rust-token-audit.mjs";

const defaultRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const argv = process.argv.slice(2);
const rootIndex = argv.indexOf("--root");
const root = rootIndex === -1 ? defaultRoot : path.resolve(argv[rootIndex + 1] ?? "");
if (rootIndex !== -1 && !argv[rootIndex + 1]) fail("--root requires a path");
if (
  argv.some(
    (argument, index) =>
      argument.startsWith("-") && argument !== "--root" && index !== rootIndex + 1,
  )
)
  fail("usage: node dev/gates/default-serialization-persistence.mjs [--root PATH]");

const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
let registry;
try {
  registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
} catch (error) {
  fail("cannot read registry: " + error.message);
}
if (
  registry?.schemaVersion !== 3 ||
  !Array.isArray(registry.scope?.paths) ||
  !Array.isArray(registry.scope?.serializerCrates) ||
  !Array.isArray(registry.allowances) ||
  !Array.isArray(registry.directDependencySnapshots)
)
  fail("registry must have schemaVersion 3, scope, allowances, and directDependencySnapshots");

const serializerPackages = new Map([
  ["postcard", "postcard"],
  ["serde_json", "serde_json"],
  ["ciborium", "ciborium"],
  ["bincode", "bincode"],
  ["rmp-serde", "rmp_serde"],
]);
const metadata = cargoMetadata(root);
const snapshots = snapshotDependencies(metadata);
if (JSON.stringify(registry.directDependencySnapshots) !== JSON.stringify(snapshots))
  fail(
    "directDependencySnapshots differs from cargo metadata/Cargo.toml direct external dependencies; audit and update the registry",
  );
const directSerializerCrates = [
  ...new Set(
    snapshots
      .flatMap((snapshot) => snapshot.dependencies)
      .filter((dependency) => serializerPackages.has(dependency.package))
      .map((dependency) => serializerPackages.get(dependency.package)),
  ),
].sort();
if (
  JSON.stringify([...registry.scope.serializerCrates].sort()) !==
  JSON.stringify(directSerializerCrates)
)
  fail(
    "scope.serializerCrates must exactly inventory known direct serializer dependencies: " +
      directSerializerCrates.join(", "),
  );

const files = new Map();
for (const scoped of registry.scope.paths) collectRust(path.join(root, scoped), files);
const expected = new Map();
const allowanceIds = new Set();
for (const allowance of registry.allowances) {
  if (
    !allowance?.id ||
    !allowance.classification ||
    !Array.isArray(allowance.endpoints) ||
    allowance.endpoints.length === 0
  )
    fail("invalid allowance " + JSON.stringify(allowance));
  if (allowanceIds.has(allowance.id)) fail("duplicate allowance ID " + allowance.id);
  allowanceIds.add(allowance.id);
  for (const endpoint of allowance.endpoints) {
    if (!validEndpoint(endpoint)) fail(allowance.id + ": invalid endpoint");
    if (!files.has(endpoint.path))
      fail(allowance.id + ": source is outside scope or absent: " + endpoint.path);
    const key = endpointKey(endpoint);
    if (expected.has(key)) fail(allowance.id + ": duplicate exact endpoint " + key);
    expected.set(key, allowance.id);
  }
}

const serializerRoots = new Set(serializerPackages.values());
for (const [relative, source] of files) {
  const tokens = rustTokens(source);
  for (const external of serializerExternCrates(tokens, serializerRoots))
    fail(
      relative +
        ":" +
        external.line +
        ": serializer extern crates are prohibited at the persistence boundary (" +
        external.root +
        "); use an explicitly registered fully-qualified canonical endpoint instead",
    );
  for (const imported of serializerImports(tokens, serializerRoots))
    fail(
      relative +
        ":" +
        imported.line +
        ": serializer imports are prohibited at the persistence boundary (" +
        imported.root +
        "); use an explicitly registered fully-qualified canonical endpoint instead",
    );
  for (const reference of serializerReferences(tokens, serializerRoots)) {
    const endpoint = describeEndpoint(tokens, reference, relative);
    const key = endpointKey(endpoint);
    if (!expected.delete(key))
      fail(
        relative +
          ":" +
          endpoint.location.line +
          ":" +
          endpoint.location.column +
          ": unregistered default serialization reference " +
          endpoint.canonicalPath,
      );
  }
}
if (expected.size)
  fail(
    "registered serializer endpoint is absent, moved, or changed boundary: " +
      expected.keys().next().value,
  );

console.log(
  "default-serialization-persistence: token-checked " +
    files.size +
    " persistence-owner source file(s), " +
    registry.allowances.length +
    " reviewed exception group(s), and " +
    snapshots.length +
    " direct-dependency snapshot(s)",
);

function validEndpoint(endpoint) {
  return (
    endpoint &&
    typeof endpoint.path === "string" &&
    typeof endpoint.canonicalPath === "string" &&
    endpoint.location &&
    Number.isInteger(endpoint.location.line) &&
    Number.isInteger(endpoint.location.column) &&
    endpoint.span &&
    endpoint.span.start &&
    endpoint.span.end &&
    Number.isInteger(endpoint.span.start.line) &&
    Number.isInteger(endpoint.span.start.column) &&
    Number.isInteger(endpoint.span.end.line) &&
    Number.isInteger(endpoint.span.end.column) &&
    endpoint.enclosing &&
    Array.isArray(endpoint.enclosing.modules) &&
    typeof endpoint.enclosing.item === "string" &&
    (endpoint.boundary === "production" || endpoint.boundary === "test")
  );
}
function endpointKey(endpoint) {
  return JSON.stringify({
    path: endpoint.path,
    canonicalPath: endpoint.canonicalPath,
    location: endpoint.location,
    span: endpoint.span,
    enclosing: endpoint.enclosing,
    boundary: endpoint.boundary,
  });
}
function cargoMetadata(directory) {
  const result = childProcess.spawnSync(
    "cargo",
    ["metadata", "--no-deps", "--format-version", "1"],
    {
      cwd: directory,
      encoding: "utf8",
    },
  );
  if (result.status !== 0)
    fail("cargo metadata failed: " + (result.stderr || result.stdout).trim());
  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    fail("cargo metadata returned invalid JSON: " + error.message);
  }
}
function packageMap(metadata) {
  return metadata.packages.map((pkg) => ({
    prefix:
      path.posix.dirname(path.relative(root, pkg.manifest_path).replaceAll(path.sep, "/")) + "/",
    name: pkg.name,
  }));
}
function packageFor(relative, packages) {
  const candidates = packages
    .filter((entry) => relative.startsWith(entry.prefix))
    .sort((left, right) => right.prefix.length - left.prefix.length);
  if (!candidates.length) fail("no Cargo package owns scoped source " + relative);
  return candidates[0].name;
}
function snapshotDependencies(metadata) {
  const packages = packageMap(metadata);
  const wanted = new Set(
    registry.scope.paths.map((scoped) => {
      const relative = scoped.replaceAll("\\", "/");
      return packageFor(relative.endsWith(".rs") ? relative : relative + "/_audit.rs", packages);
    }),
  );
  return metadata.packages
    .filter((pkg) => wanted.has(pkg.name))
    .map((pkg) => ({
      crate: pkg.name,
      manifest: path.relative(root, pkg.manifest_path).replaceAll(path.sep, "/"),
      dependencies: pkg.dependencies
        .filter((dependency) => !dependency.path)
        .map((dependency) => ({
          crate: dependency.rename ?? dependency.name.replaceAll("-", "_"),
          package: dependency.name,
        }))
        .sort(
          (left, right) =>
            left.crate.localeCompare(right.crate) || left.package.localeCompare(right.package),
        ),
    }))
    .sort((left, right) => left.crate.localeCompare(right.crate));
}
function collectRust(absolute, files) {
  if (!fs.existsSync(absolute)) fail("scope path is absent: " + path.relative(root, absolute));
  const stat = fs.statSync(absolute);
  if (stat.isFile()) {
    if (absolute.endsWith(".rs"))
      files.set(
        path.relative(root, absolute).replaceAll(path.sep, "/"),
        fs.readFileSync(absolute, "utf8"),
      );
    return;
  }
  for (const entry of fs.readdirSync(absolute, { withFileTypes: true })) {
    // External test trees construct malformed/historical bytes. Inline tests
    // remain governed because their owning source is scanned.
    if (entry.isDirectory() && entry.name === "tests") continue;
    collectRust(path.join(absolute, entry.name), files);
  }
}
function fail(message) {
  console.error("default-serialization-persistence: ERROR: " + message);
  process.exitCode = 1;
  throw new Error(message);
}
