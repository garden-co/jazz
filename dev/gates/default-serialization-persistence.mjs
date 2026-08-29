#!/usr/bin/env node
/**
 * Fail closed if a durable-storage owner gains a raw convenience serializer.
 *
 * Authoritative state uses the normative Groove record/scalar or ordered-key
 * codecs. A small number of source uses remain for semantic JSON parsing,
 * in-memory query helpers, and the explicitly versioned catalogue JSON
 * payload. They live in the registry with an exact source-count and boundary
 * classification so a new default serde/postcard call cannot silently become
 * durable state.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

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
  fail(`cannot read registry: ${error.message}`);
}
if (
  registry?.schemaVersion !== 2 ||
  !Array.isArray(registry.scope?.paths) ||
  !Array.isArray(registry.allowances)
)
  fail("registry must have schemaVersion 2, scope.paths, and allowances");

// Every spelling is named once.  This deliberately recognizes convenience
// families, rather than a hand-picked list of calls seen in today's source:
// adding a new `serde_json::*` helper under a persistence owner fails closed.
const forbiddenApis = new Set([
  "postcard::to_allocvec",
  "postcard::to_stdvec",
  "postcard::to_slice",
  "postcard::to_extend",
  "postcard::to_io",
  "postcard::to_allocvec_cobs",
  "postcard::to_stdvec_cobs",
  "postcard::to_slice_cobs",
  "postcard::from_bytes",
  "postcard::from_bytes_cobs",
  "postcard::take_from_bytes",
  "postcard::take_from_bytes_cobs",
  "postcard::experimental::serialized_size",
  "serde_json::to_vec",
  "serde_json::to_writer",
  "serde_json::to_writer_pretty",
  "serde_json::to_string",
  "serde_json::to_string_pretty",
  "serde_json::from_slice",
  "serde_json::from_reader",
  "serde_json::from_str",
  "serde_json::Serializer::new",
  "serde_json::Serializer::pretty",
  "serde_json::Deserializer::from_slice",
  "serde_json::Deserializer::from_str",
  "serde_json::Deserializer::from_reader",
  "bincode::serialize",
  "bincode::serialize_into",
  "bincode::deserialize",
  "bincode::deserialize_from",
  "rmp_serde::to_vec",
  "rmp_serde::to_vec_named",
  "rmp_serde::from_slice",
  "rmp_serde::from_read",
]);
const files = new Map();
for (const scoped of registry.scope.paths) collectRust(path.join(root, scoped), files);

const seen = new Set();
for (const allowance of registry.allowances) {
  if (
    !allowance?.id ||
    !allowance.path ||
    !forbiddenApis.has(allowance.api) ||
    !Number.isInteger(allowance.expectedOccurrences) ||
    !allowance.classification
  )
    fail(`invalid allowance ${JSON.stringify(allowance)}`);
  if (seen.has(allowance.id)) fail(`duplicate allowance ID ${allowance.id}`);
  seen.add(allowance.id);
  const source = files.get(allowance.path);
  if (source === undefined)
    fail(`${allowance.id}: source is outside scope or absent: ${allowance.path}`);
  const matches = serializerCalls(source).filter((match) => match.api === allowance.api);
  if (matches.length !== allowance.expectedOccurrences)
    fail(
      `${allowance.id}: expected ${allowance.expectedOccurrences} ${allowance.api} occurrence(s) in ${allowance.path}, found ${matches.length}`,
    );
}

for (const [relative, source] of files) {
  for (const match of serializerCalls(source)) {
    const permitted = registry.allowances.some(
      (allowance) => allowance.path === relative && allowance.api === match.api,
    );
    if (!permitted) {
      const line = source.slice(0, match.index).split("\n").length;
      fail(`${relative}:${line}: unregistered default serialization ${match.api}`);
    }
  }
}

/**
 * Finds calls to the forbidden serializer APIs, resolving the simple Rust use
 * trees that make a textual `postcard::` scan evadable. This is intentionally
 * conservative and token-shaped rather than a regex allowlist: aliases,
 * grouped imports, globs, and leading `::` resolve to the canonical API name.
 * Bare imports are ignored where a local binding in the enclosing block shadows
 * them, avoiding a false positive on an unrelated local function or closure.
 */
function serializerCalls(source) {
  const imports = parseImports(source);
  const calls = [];
  const seen = new Set();
  const add = (api, index) => {
    const key = `${api}:${index}`;
    if (!seen.has(key)) {
      seen.add(key);
      calls.push({ api, index });
    }
  };

  for (const api of forbiddenApis) {
    const direct = new RegExp(`(?<![A-Za-z0-9_])(?:::)?${escapeRegex(api)}\\b`, "g");
    for (const match of source.matchAll(direct)) {
      if (isCallAt(source, match.index + match[0].length)) add(api, match.index);
    }
  }

  for (const [alias, namespace] of imports.namespaces) {
    for (const api of forbiddenApis) {
      if (!api.startsWith(`${namespace}::`)) continue;
      const suffix = api.slice(namespace.length + 2);
      const pattern = new RegExp(`\\b${escapeRegex(alias)}::${escapeRegex(suffix)}\\b`, "g");
      for (const match of source.matchAll(pattern)) {
        if (isCallAt(source, match.index + match[0].length)) add(api, match.index);
      }
    }
  }

  for (const [local, api] of imports.functions) {
    const pattern = new RegExp(`\\b${escapeRegex(local)}\\b`, "g");
    for (const match of source.matchAll(pattern)) {
      if (
        isCallAt(source, match.index + match[0].length) &&
        !isLocallyShadowed(source, match.index, local)
      ) {
        add(api, match.index);
      }
    }
  }

  return calls.sort((left, right) => left.index - right.index);
}

function parseImports(source) {
  const namespaces = new Map();
  const functions = new Map();
  for (const match of source.matchAll(/(?:^|\n)\s*(?:pub\s*(?:\([^)]*\)\s*)?)?use\s+([^;]+);/g)) {
    registerUseTree(match[1].trim(), namespaces, functions);
  }
  return { namespaces, functions };
}

function registerUseTree(tree, namespaces, functions) {
  const brace = tree.indexOf("{");
  if (brace !== -1 && tree.endsWith("}")) {
    const prefix = normalizePath(tree.slice(0, brace).replace(/::$/, ""));
    for (const member of splitUseMembers(tree.slice(brace + 1, -1))) {
      const [path, alias] = splitUseAlias(member);
      if (path === "*") {
        registerGlob(prefix, functions);
      } else if (path === "self") {
        namespaces.set(alias ?? prefix, prefix);
      } else {
        registerImportedPath(`${prefix}::${path}`, alias, namespaces, functions);
      }
    }
    return;
  }
  const [path, alias] = splitUseAlias(tree);
  if (path.endsWith("::*")) {
    registerGlob(normalizePath(path.slice(0, -3)), functions);
  } else {
    registerImportedPath(path, alias, namespaces, functions);
  }
}

function splitUseMembers(body) {
  const members = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < body.length; index += 1) {
    if (body[index] === "{") depth += 1;
    if (body[index] === "}") depth -= 1;
    if (body[index] === "," && depth === 0) {
      members.push(body.slice(start, index).trim());
      start = index + 1;
    }
  }
  members.push(body.slice(start).trim());
  return members.filter(Boolean);
}

function splitUseAlias(member) {
  const match = member.trim().match(/^(.*?)\s+as\s+([A-Za-z_][A-Za-z0-9_]*)$/);
  return match ? [match[1].trim(), match[2]] : [member.trim(), undefined];
}

function registerImportedPath(path, alias, namespaces, functions) {
  const canonical = normalizePath(path);
  if (["postcard", "serde_json", "bincode", "rmp_serde"].includes(canonical)) {
    namespaces.set(alias ?? canonical, canonical);
    return;
  }
  if (canonical === "serde_json::Serializer" || canonical === "serde_json::Deserializer") {
    namespaces.set(alias ?? canonical.slice(canonical.lastIndexOf("::") + 2), canonical);
    return;
  }
  if (forbiddenApis.has(canonical)) {
    functions.set(alias ?? canonical.slice(canonical.lastIndexOf("::") + 2), canonical);
  }
}

function registerGlob(namespace, functions) {
  for (const api of forbiddenApis) {
    if (api.startsWith(`${namespace}::`) && !api.slice(namespace.length + 2).includes("::")) {
      functions.set(api.slice(namespace.length + 2), api);
    }
  }
}

function normalizePath(path) {
  return path.trim().replace(/^::/, "").replace(/\s+/g, "");
}

function isCallAt(source, index) {
  let cursor = index;
  while (/\s/.test(source[cursor] ?? "")) cursor += 1;
  if (source.startsWith("::", cursor)) return true; // turbofish call; the following parser owns syntax.
  return source[cursor] === "(";
}

function isLocallyShadowed(source, index, name) {
  const blockStart = source.lastIndexOf("{", index);
  const prefix = source.slice(blockStart + 1, index);
  const binding = new RegExp(
    `\\b(?:let|const|static|fn|struct|enum|mod)\\s+${escapeRegex(name)}\\b`,
  );
  return binding.test(prefix);
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

console.log(
  `default-serialization-persistence: checked ${files.size} persistence-owner source file(s) and ${registry.allowances.length} registered exceptions`,
);

function collectRust(absolute, files) {
  if (!fs.existsSync(absolute)) fail(`scope path is absent: ${path.relative(root, absolute)}`);
  const stat = fs.statSync(absolute);
  if (stat.isFile()) {
    if (absolute.endsWith(".rs"))
      files.set(path.relative(root, absolute), fs.readFileSync(absolute, "utf8"));
    return;
  }
  for (const entry of fs.readdirSync(absolute, { withFileTypes: true })) {
    // Tests deliberately manufacture malformed and historical byte sequences;
    // they cannot add a production persistence spelling. Inline test modules
    // remain visible through their owning source file and therefore need an
    // explicit registry receipt.
    if (entry.isDirectory() && entry.name === "tests") continue;
    collectRust(path.join(absolute, entry.name), files);
  }
}

function fail(message) {
  console.error(`default-serialization-persistence: ERROR: ${message}`);
  process.exitCode = 1;
  throw new Error(message);
}
