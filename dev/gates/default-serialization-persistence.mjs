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
  persistenceSourceEscapes,
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
const snapshotOnly = argv.includes("--print-direct-dependency-snapshots");
const refreshRegistry = argv.includes("--refresh-registry-snapshot");
const refreshEndpoints = argv.includes("--refresh-registry-endpoints");
const serializerPackages = new Map([
  ["postcard", "postcard"],
  ["serde_json", "serde_json"],
  ["ciborium", "ciborium"],
  ["bincode", "bincode"],
  ["rmp-serde", "rmp_serde"],
  ["ron", "ron"],
  ["serde_yaml", "serde_yaml"],
  ["toml", "toml"],
]);
if (
  argv.some(
    (argument, index) =>
      argument.startsWith("-") &&
      argument !== "--root" &&
      argument !== "--print-direct-dependency-snapshots" &&
      argument !== "--refresh-registry-snapshot" &&
      argument !== "--refresh-registry-endpoints" &&
      index !== rootIndex + 1,
  )
)
  fail("usage: node dev/gates/default-serialization-persistence.mjs [--root PATH] [--print-direct-dependency-snapshots|--refresh-registry-snapshot|--refresh-registry-endpoints]");

const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
let registry;
try {
  registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
} catch (error) {
  fail("cannot read registry: " + error.message);
}
if (snapshotOnly) {
  if (!Array.isArray(registry?.scope?.paths)) fail("registry scope.paths is required for snapshots");
  console.log(JSON.stringify(snapshotDependencies(cargoMetadata(root), registry.scope.paths), null, 2));
  process.exit(0);
}
if (refreshRegistry) {
  const snapshots = snapshotDependencies(cargoMetadata(root), registry.scope.paths);
  registry.schemaVersion = 5;
  registry.directDependencySnapshots = snapshots;
  registry.dependencyClassifications = snapshots
    .flatMap((snapshot) =>
      snapshot.dependencies.map((dependency) => ({
        crate: snapshot.crate,
        dependency: dependency.identity,
        classification: dependency.path
          ? "governed-path-dependency"
          : serializerPackages.has(dependency.package)
            ? "governed-serializer"
            : "reviewed-non-serializer",
        ...(serializerPackages.has(dependency.package) && !dependency.path
          ? { roots: [dependency.crate] }
          : {}),
      })),
    )
    .sort((left, right) =>
      left.crate.localeCompare(right.crate) || left.dependency.localeCompare(right.dependency),
    );
  registry.scope.serializerCrates = serializerRootsFromSnapshots(snapshots);
  fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2) + "\n");
  console.log("refreshed direct dependency snapshot and explicit classifications; review this policy change");
  process.exit(0);
}
if (
  registry?.schemaVersion !== 5 ||
  !Array.isArray(registry.scope?.paths) ||
  !Array.isArray(registry.scope?.serializerCrates) ||
  !Array.isArray(registry.allowances) ||
  !Array.isArray(registry.directDependencySnapshots) ||
  !Array.isArray(registry.dependencyClassifications)
)
  fail("registry must have schemaVersion 4, scope, allowances, directDependencySnapshots, and dependencyClassifications");

const metadata = cargoMetadata(root);
const snapshots = snapshotDependencies(metadata);
if (JSON.stringify(registry.directDependencySnapshots) !== JSON.stringify(snapshots))
  fail(
    "directDependencySnapshots differs from cargo metadata/Cargo.toml direct external dependencies; audit and update the registry",
  );
const classifiedDependencies = new Map();
for (const entry of registry.dependencyClassifications) {
  if (
    !entry ||
    typeof entry.crate !== "string" ||
    typeof entry.dependency !== "string" ||
    !["governed-serializer", "reviewed-non-serializer", "governed-path-dependency"].includes(
      entry.classification,
    ) ||
    (entry.classification === "governed-serializer" && !Array.isArray(entry.roots))
  )
    fail("invalid dependency classification " + JSON.stringify(entry));
  const key = entry.crate + "\u0000" + entry.dependency;
  if (classifiedDependencies.has(key)) fail("duplicate dependency classification " + key);
  classifiedDependencies.set(key, entry);
}
const directDependencies = snapshots.flatMap((snapshot) =>
  snapshot.dependencies.map((dependency) => ({ crate: snapshot.crate, dependency })),
);
if (classifiedDependencies.size !== directDependencies.length)
  fail("every direct external dependency available to a persistence owner needs an explicit classification");
for (const { crate, dependency } of directDependencies) {
  const classification = classifiedDependencies.get(crate + "\u0000" + dependency.identity);
  if (!classification)
    fail("unclassified direct dependency " + crate + ": " + dependency.identity);
  if (dependency.path) {
    if (classification.classification !== "governed-path-dependency")
      fail("direct path/workspace dependency must be governed: " + crate + ": " + dependency.identity);
  } else if (classification.classification === "governed-path-dependency") {
    fail("governed path dependency is no longer a path/workspace dependency: " + crate + ": " + dependency.identity);
  } else if (classification.classification === "governed-serializer") {
    if (!serializerPackages.has(dependency.package))
      fail("only known serializer packages may be governed: " + dependency.package);
    const expectedRoots = [dependency.crate].sort();
    if (JSON.stringify([...classification.roots].sort()) !== JSON.stringify(expectedRoots))
      fail("governed serializer roots must exactly match direct crate alias for " + dependency.identity);
  } else if (serializerPackages.has(dependency.package)) {
    fail("known serializer package must be governed: " + crate + ": " + dependency.identity);
  }
}
const uniqueSerializerRoots = serializerRootsFromSnapshots(snapshots);
const serializerRoots = new Set(uniqueSerializerRoots);
if (
  JSON.stringify([...new Set(registry.scope.serializerCrates)].sort()) !==
  JSON.stringify(uniqueSerializerRoots)
)
  fail(
    "scope.serializerCrates must exactly inventory known direct serializer dependencies: " +
      uniqueSerializerRoots.join(", "),
  );

// Keep a physical ownership receipt alongside the logical source spelling.
// A logical path alone is not a boundary: a symlink can make an apparently
// in-scope `foo.rs` resolve to arbitrary source outside the persistence owner.
const files = new Map();
const fileOwnership = new Map();
for (const scoped of registry.scope.paths)
  collectRust(path.join(root, scoped), files, fileOwnership, ownerScope(path.join(root, scoped)));
if (refreshEndpoints) {
  const stale = new Map();
  for (const allowance of registry.allowances) {
    for (const endpoint of allowance.endpoints) {
      const key = endpoint.path + "\u0000" + endpoint.canonicalPath;
      const entries = stale.get(key) ?? [];
      entries.push(endpoint);
      stale.set(key, entries);
    }
  }
  for (const [relative, source] of files) {
    const tokens = rustTokens(source);
    for (const reference of serializerReferences(tokens, serializerRoots)) {
      const endpoint = describeEndpoint(tokens, reference, relative);
      const entries = stale.get(relative + "\u0000" + endpoint.canonicalPath);
      if (!entries?.length)
        fail("cannot refresh unregistered endpoint " + relative + ": " + endpoint.canonicalPath);
      Object.assign(entries.shift(), endpoint);
    }
  }
  const missing = [...stale.entries()].find(([, entries]) => entries.length);
  if (missing) fail("cannot refresh absent endpoint " + missing[0]);
  fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2) + "\n");
  console.log("refreshed exact endpoint identities; review this registry policy change");
  process.exit(0);
}
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

for (const [relative, source] of files) {
  const tokens = rustTokens(source);
  for (const escape of persistenceSourceEscapes(tokens)) {
    // Existing node implementation files use include! as a deliberately
    // namespace-sharing split. They are safe only because the literal resolves
    // to another source file already collected by this audit. A computed,
    // absolute, parent-traversing, or otherwise uncollected include is a source
    // escape and fails closed. `#[path] mod` has no such contained form here.
    if (
      escape.kind === "include!" &&
      includedSourceIsCollected(relative, escape.literal, files, fileOwnership)
    )
      continue;
    fail(
      relative +
        ":" +
        escape.line +
        ": " +
        escape.kind +
        " is prohibited at the persistence boundary unless it is an already-collected literal source file",
    );
  }
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

function includedSourceIsCollected(relative, literal, files, fileOwnership) {
  const value = rustStringLiteral(literal);
  if (value === undefined) return false;
  if (path.isAbsolute(value)) return false;
  const owner = fileOwnership.get(relative);
  if (!owner) return false;
  const absolute = path.resolve(root, path.dirname(relative), value);
  let realpath;
  try {
    realpath = fs.realpathSync(absolute);
  } catch {
    return false;
  }
  if (!isWithin(owner.scope, realpath)) return false;
  const target = path.relative(root, absolute).replaceAll(path.sep, "/");
  const collected = fileOwnership.get(target);
  return (
    files.has(target) &&
    collected?.realpath === realpath &&
    collected.scope === owner.scope
  );
}

function rustStringLiteral(literal) {
  if (typeof literal !== "string") return undefined;
  if (literal.startsWith('"')) {
    try {
      return JSON.parse(literal);
    } catch {
      return undefined;
    }
  }
  const raw = literal.match(/^(?:br|rb|r)(#{0,255})"([\s\S]*)"\1$/);
  return raw?.[2];
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
    Array.isArray(endpoint.enclosing.items) &&
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
export function cargoMetadata(directory) {
  const result = childProcess.spawnSync(
    "cargo",
    ["metadata", "--format-version", "1"],
    {
      cwd: directory,
      encoding: "utf8",
      // The resolved workspace graph is intentionally part of the snapshot;
      // it is larger than Node's 1MiB spawnSync default in this workspace.
      maxBuffer: 16 * 1024 * 1024,
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
export function snapshotDependencies(metadata, scopePaths = registry.scope.paths) {
  const packages = packageMap(metadata);
  const wanted = new Set(
    scopePaths.map((scoped) => {
      const relative = scoped.replaceAll("\\", "/");
      return packageFor(relative.endsWith(".rs") ? relative : relative + "/_audit.rs", packages);
    }),
  );
  const resolved = new Map(metadata.packages.map((pkg) => [pkg.id, pkg]));
  const resolvedNodes = new Map((metadata.resolve?.nodes ?? []).map((node) => [node.id, node]));
  return metadata.packages
    .filter((pkg) => wanted.has(pkg.name))
    .map((pkg) => ({
      crate: pkg.name,
      manifest: path.relative(root, pkg.manifest_path).replaceAll(path.sep, "/"),
      dependencies: pkg.dependencies
        .map((dependency) => snapshotDependency(pkg, dependency, resolved, resolvedNodes))
        .sort(
          (left, right) =>
            left.identity.localeCompare(right.identity),
        ),
    }))
    .sort((left, right) => left.crate.localeCompare(right.crate));
}
function snapshotDependency(owner, dependency, resolved, resolvedNodes) {
  const crate = dependency.rename ?? dependency.name.replaceAll("-", "_");
  const node = resolvedNodes.get(owner.id);
  const resolvedEdge = node?.deps.find(
    (edge) => edge.name === crate || edge.name === dependency.name.replaceAll("-", "_"),
  );
  const resolvedPackage = resolvedEdge ? resolved.get(resolvedEdge.pkg) : undefined;
  const target = dependency.target ?? null;
  const kind = dependency.kind ?? null;
  // cargo metadata exposes the effective manifest semantics. `workspace` is
  // not preserved as a first-class field, so record the observable inherited
  // form when it is present in the manifest; all other fields are exact.
  const pathDependency = dependency.path
    ? resolvedPackage ??
      [...resolved.values()].find(
        (candidate) =>
          path.resolve(path.dirname(candidate.manifest_path)) === path.resolve(dependency.path),
      )
    : undefined;
  if (dependency.path && !pathDependency)
    fail("cannot resolve direct path/workspace dependency source: " + dependency.path);
  return {
    identity: [crate, dependency.name, kind ?? "normal", target ?? "all"].join("|"),
    crate,
    package: dependency.name,
    rename: dependency.rename ?? null,
    requirement: dependency.req,
    resolvedVersion: resolvedPackage?.version ?? null,
    source: dependency.source ?? null,
    registry: dependency.registry ?? null,
    // Cargo reports path dependencies as absolute host paths. Keep a portable
    // workspace-relative identity instead; the registry records the package
    // source surface, not the checkout location.
    path: dependency.path ? path.relative(root, dependency.path).replaceAll(path.sep, "/") : null,
    features: [...dependency.features].sort(),
    defaultFeatures: dependency.uses_default_features,
    optional: dependency.optional,
    target,
    kind,
    workspaceInherited: manifestUsesWorkspaceDependency(owner.manifest_path, crate, dependency.name),
    // This is deliberately the small public re-export surface, not a source
    // hash. Ordinary implementation edits in a workspace dependency must not
    // churn every persistence audit snapshot, but adding/removing an explicit
    // serializer spelling that a persistence owner can reach must be reviewed.
    ...(pathDependency
      ? { serializerReexports: publicSerializerReexports(pathDependency) }
      : {}),
  };
}

function serializerRootsFromSnapshots(snapshots) {
  return [
    ...new Set(
      snapshots.flatMap((snapshot) =>
        snapshot.dependencies.flatMap((dependency) => {
          if (serializerPackages.has(dependency.package)) return [dependency.crate];
          if (!dependency.path) return [];
          return (dependency.serializerReexports ?? []).map(
            (reexport) => dependency.crate + "::" + reexport.alias,
          );
        }),
      ),
    ),
  ].sort();
}

function publicSerializerReexports(pkg) {
  const lib = pkg.targets?.find((target) => target.kind.includes("lib"));
  if (!lib || !fs.existsSync(lib.src_path)) return [];
  const serializerRoots = new Set(
    pkg.dependencies
      .filter((dependency) => serializerPackages.has(dependency.name))
      .map((dependency) => dependency.rename ?? dependency.name.replaceAll("-", "_")),
  );
  if (!serializerRoots.size) return [];
  return explicitPublicSerializerReexports(rustTokens(fs.readFileSync(lib.src_path, "utf8")), serializerRoots);
}

function explicitPublicSerializerReexports(tokens, serializerRoots) {
  const reexports = [];
  for (let index = 0; index + 3 < tokens.length; index += 1) {
    if (tokens[index].text !== "pub" || tokens[index + 1]?.text !== "use") continue;
    let cursor = index + 2;
    // `pub use ::serde_json::...` is an equivalent public root spelling.
    if (tokens[cursor]?.text === "::") cursor += 1;
    const root = tokens[cursor];
    if (root?.kind !== "ident" || !serializerRoots.has(root.text)) continue;
    let alias = root.text;
    cursor += 1;
    if (tokens[cursor]?.text === "as" && tokens[cursor + 1]?.kind === "ident") {
      alias = tokens[cursor + 1].text;
      cursor += 2;
    }
    if (tokens[cursor]?.text === ";") {
      reexports.push({ root: root.text, alias });
      continue;
    }
    // A grouped `self` is the only member which re-exports the serializer
    // crate root itself.  The other members are values/types, not a new root
    // spelling an owner can use for generic serialization.  Track every
    // top-level `self` alias, including `self` without `as`.
    if (tokens[cursor]?.text !== "::" || tokens[cursor + 1]?.text !== "{") continue;
    let depth = 1;
    for (cursor += 2; cursor < tokens.length && depth; cursor += 1) {
      const token = tokens[cursor];
      if (token.text === "{") {
        depth += 1;
        continue;
      }
      if (token.text === "}") {
        depth -= 1;
        continue;
      }
      if (depth !== 1 || token.text !== "self") continue;
      let selfAlias = root.text;
      if (tokens[cursor + 1]?.text === "as" && tokens[cursor + 2]?.kind === "ident") {
        selfAlias = tokens[cursor + 2].text;
        cursor += 2;
      }
      reexports.push({ root: root.text, alias: selfAlias });
    }
  }
  return reexports.sort(
    (left, right) => left.alias.localeCompare(right.alias) || left.root.localeCompare(right.root),
  );
}
function manifestUsesWorkspaceDependency(manifestPath, crate, packageName) {
  const source = fs.readFileSync(manifestPath, "utf8");
  // This is intentionally a narrow, conservative snapshot field. Cargo has
  // already supplied effective semantics above; this records the two standard
  // syntactic forms without pretending to parse TOML fully.
  const escaped = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const names = [crate, packageName].map(escaped).join("|");
  return new RegExp(
    `^(?:(?:${names})\\.workspace\\s*=\\s*true|(?:${names})\\s*=\\s*\\{[^\\n}]*\\bworkspace\\s*=\\s*true[^\\n}]*\\})`,
    "m",
  ).test(source);
}
function ownerScope(absolute) {
  if (!fs.existsSync(absolute)) fail("scope path is absent: " + path.relative(root, absolute));
  const stat = fs.lstatSync(absolute);
  if (stat.isSymbolicLink())
    fail("audited persistence scope is a symbolic link: " + path.relative(root, absolute));
  const realpath = fs.realpathSync(absolute);
  return stat.isDirectory() ? realpath : path.dirname(realpath);
}

function collectRust(absolute, files, fileOwnership, scope) {
  if (!fs.existsSync(absolute)) fail("scope path is absent: " + path.relative(root, absolute));
  const stat = fs.lstatSync(absolute);
  if (stat.isSymbolicLink())
    fail("audited persistence entry is a symbolic link: " + path.relative(root, absolute));
  const realpath = fs.realpathSync(absolute);
  if (!isWithin(scope, realpath))
    fail(
      "audited persistence entry resolves outside its owner scope: " +
        path.relative(root, absolute),
    );
  if (stat.isFile()) {
    if (absolute.endsWith(".rs")) {
      const relative = path.relative(root, absolute).replaceAll(path.sep, "/");
      const previous = fileOwnership.get(relative);
      if (previous && (previous.realpath !== realpath || previous.scope !== scope))
        fail("audited persistence source has ambiguous ownership: " + relative);
      files.set(relative, fs.readFileSync(absolute, "utf8"));
      fileOwnership.set(relative, { realpath, scope });
    }
    return;
  }
  for (const entry of fs.readdirSync(absolute, { withFileTypes: true })) {
    // External test trees construct malformed/historical bytes. Inline tests
    // remain governed because their owning source is scanned.
    if (entry.isDirectory() && entry.name === "tests") continue;
    collectRust(path.join(absolute, entry.name), files, fileOwnership, scope);
  }
}

function isWithin(scope, candidate) {
  const relative = path.relative(scope, candidate);
  return relative === "" || (!relative.startsWith(".." + path.sep) && relative !== ".." && !path.isAbsolute(relative));
}
function fail(message) {
  console.error("default-serialization-persistence: ERROR: " + message);
  process.exitCode = 1;
  throw new Error(message);
}
