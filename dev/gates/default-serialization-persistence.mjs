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
  const code = rustCodeMask(source);
  const imports = parseImports(code);
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
    for (const match of code.matchAll(direct)) {
      if (isCallAt(code, match.index + match[0].length)) add(api, match.index);
    }
  }

  for (const [alias, namespace] of imports.namespaces) {
    for (const api of forbiddenApis) {
      if (!api.startsWith(`${namespace}::`)) continue;
      const suffix = api.slice(namespace.length + 2);
      const pattern = new RegExp(`\\b${escapeRegex(alias)}::${escapeRegex(suffix)}\\b`, "g");
      for (const match of code.matchAll(pattern)) {
        if (isCallAt(code, match.index + match[0].length)) add(api, match.index);
      }
    }
  }

  for (const [local, api] of imports.functions) {
    const pattern = new RegExp(`\\b${escapeRegex(local)}\\b`, "g");
    for (const match of code.matchAll(pattern)) {
      if (
        isCallAt(code, match.index + match[0].length) &&
        !isLocallyShadowed(code, match.index, local)
      ) {
        add(api, match.index);
      }
    }
  }

  return calls.sort((left, right) => left.index - right.index);
}

// Keep offsets and newlines intact so diagnostics still point into the source,
// while comments and string/byte/raw-string literals cannot manufacture a call.
function rustCodeMask(source) {
  const masked = source.split("");
  const blank = (index) => {
    if (masked[index] !== "\n") masked[index] = " ";
  };
  for (let index = 0; index < source.length; index += 1) {
    if (source.startsWith("//", index)) {
      for (; index < source.length && source[index] !== "\n"; index += 1) blank(index);
      continue;
    }
    if (source.startsWith("/*", index)) {
      let depth = 1;
      blank(index++);
      blank(index);
      for (index += 1; index < source.length && depth > 0; index += 1) {
        if (source.startsWith("/*", index)) {
          blank(index++);
          blank(index);
          depth += 1;
        } else if (source.startsWith("*/", index)) {
          blank(index++);
          blank(index);
          depth -= 1;
        } else {
          blank(index);
        }
      }
      index -= 1;
      continue;
    }
    const raw = source.slice(index).match(/^(?:br|rb|r)(#{0,255})"/);
    if (raw) {
      const delimiter = `"${raw[1]}`;
      const end = source.indexOf(delimiter, index + raw[0].length);
      const stop = end === -1 ? source.length : end + delimiter.length;
      for (; index < stop; index += 1) blank(index);
      index -= 1;
      continue;
    }
    const ordinaryString =
      source[index] === '"' ||
      ((source[index] === "b" || source[index] === "c") && source[index + 1] === '"');
    const character = source.slice(index).match(/^'(?:\\.|[^'\\\n])'/);
    if (character) {
      for (let offset = 0; offset < character[0].length; offset += 1) blank(index + offset);
      index += character[0].length - 1;
      continue;
    }
    if (ordinaryString) {
      const start = index;
      blank(start);
      if (source[index] !== '"') {
        index += 1;
        blank(index);
      }
      index += 1; // consume the opening quote before masking its contents
      for (; index < source.length; index += 1) {
        blank(index);
        if (source[index] === "\\") {
          index += 1;
          blank(index);
        } else if (source[index] === '"') {
          break;
        }
      }
    }
  }
  return masked.join("");
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
  registerUseTreeAt(tree, "", namespaces, functions);
}

function registerUseTreeAt(tree, base, namespaces, functions) {
  const [path, alias] = splitUseAlias(tree);
  const group = topLevelUseGroup(path);
  if (group) {
    const prefix = joinUsePath(base, group.prefix);
    for (const member of splitUseMembers(group.body)) {
      registerUseTreeAt(member, prefix, namespaces, functions);
    }
    return;
  }
  const resolved = joinUsePath(base, path);
  if (resolved.endsWith("::*")) {
    registerGlob(normalizePath(resolved.slice(0, -3)), functions);
  } else if (resolved === "self") {
    if (base) namespaces.set(alias ?? base, base);
  } else {
    registerImportedPath(resolved, alias, namespaces, functions);
  }
}

function topLevelUseGroup(tree) {
  let depth = 0;
  let start = -1;
  for (let index = 0; index < tree.length; index += 1) {
    if (tree[index] === "{") {
      if (depth === 0) start = index;
      depth += 1;
    } else if (tree[index] === "}") {
      depth -= 1;
      if (depth === 0) {
        if (tree.slice(index + 1).trim()) return undefined;
        return {
          prefix: tree.slice(0, start).replace(/::$/, "").trim(),
          body: tree.slice(start + 1, index),
        };
      }
    }
  }
  return undefined;
}

function joinUsePath(base, path) {
  const normalized = normalizePath(path);
  if (!base) return normalized;
  if (normalized === "self") return base;
  if (normalized === "super") return "";
  if (normalized.startsWith("super::")) return normalized.slice("super::".length);
  if (normalized.startsWith("self::")) return `${base}::${normalized.slice("self::".length)}`;
  return `${base}::${normalized}`;
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
  if (topLevelUseGroup(member)) return [member.trim(), undefined];
  const match = member.trim().match(/^(.*?)\s+as\s+((?:r#)?[A-Za-z_][A-Za-z0-9_]*)$/);
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
  const segments = path.trim().replace(/^::/, "").replace(/\s+/g, "").split("::");
  const knownRoot = segments.findLastIndex((segment) =>
    ["postcard", "serde_json", "bincode", "rmp_serde"].includes(segment),
  );
  return (knownRoot === -1 ? segments : segments.slice(knownRoot)).join("::");
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
