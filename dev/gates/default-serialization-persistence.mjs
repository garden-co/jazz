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
import crypto from "node:crypto";
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
  !Array.isArray(registry.scope?.serializerCrates) ||
  !Array.isArray(registry.allowances)
)
  fail("registry must have schemaVersion 2, scope.paths, scope.serializerCrates, and allowances");

const serializerFamilies = new Map([
  ["postcard", "postcard"],
  ["serde_json", "serde_json"],
  ["ciborium", "ciborium"],
  ["bincode", "bincode"],
  ["rmp-serde", "rmp_serde"],
]);
const cargoLock = fs.readFileSync(path.join(root, "Cargo.lock"), "utf8");
const availableSerializerCrates = [...serializerFamilies]
  .filter(([packageName]) => cargoLock.includes(`name = "${packageName}"`))
  .map(([, crateName]) => crateName)
  .sort();
if (
  JSON.stringify([...registry.scope.serializerCrates].sort()) !==
  JSON.stringify(availableSerializerCrates)
)
  fail(
    `scope.serializerCrates must exactly inventory known serde serializer dependencies from Cargo.lock: ${availableSerializerCrates.join(", ")}`,
  );

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
  "ciborium::ser::into_writer",
  "ciborium::de::from_reader",
]);
const files = new Map();
for (const scoped of registry.scope.paths) collectRust(path.join(root, scoped), files);

const seen = new Set();
for (const allowance of registry.allowances) {
  if (
    !allowance?.id ||
    !allowance.path ||
    !forbiddenApis.has(allowance.api) ||
    !Array.isArray(allowance.anchors) ||
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
  const anchors = matches.map((match) => sourceAnchor(source, match.index));
  if (JSON.stringify(anchors) !== JSON.stringify(allowance.anchors))
    fail(`${allowance.id}: registered endpoint anchor changed or call moved in ${allowance.path}`);
}

for (const [relative, source] of files) {
  for (const imported of forbiddenExternCrates(source)) {
    const line = source.slice(0, imported.index).split("\n").length;
    fail(
      `${relative}:${line}: serializer extern crates are prohibited at the persistence boundary (${imported.root}); use an explicitly registered fully-qualified canonical endpoint instead`,
    );
  }
  for (const imported of forbiddenPersistenceImports(source)) {
    const line = source.slice(0, imported.index).split("\n").length;
    fail(
      `${relative}:${line}: serializer imports are prohibited at the persistence boundary (${imported.root}); use an explicitly registered fully-qualified canonical endpoint instead`,
    );
  }
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

function forbiddenExternCrates(source) {
  const code = rustCodeMask(source);
  const imports = [];
  for (const match of code.matchAll(
    /(?:^|\n)\s*(?:pub(?:\s*\([^)]*\))?\s+)?extern\s+crate\s+(?:r#)?(postcard|serde_json|bincode|rmp_serde|ciborium)(?:\s+as\s+(?:r#)?[A-Za-z_][A-Za-z0-9_]*)?\s*;/g,
  )) {
    imports.push({ root: match[1], index: match.index });
  }
  return imports;
}

/** Finds only explicit serializer calls; imports are rejected separately. */
function serializerCalls(source) {
  const code = rustCodeMask(source);
  const calls = [];
  for (const api of forbiddenApis) {
    const direct = new RegExp(`(?<![A-Za-z0-9_])(?:::)?${rawRootApiPattern(api)}\\b`, "g");
    for (const match of code.matchAll(direct)) {
      if (isCallAt(code, match.index + match[0].length)) {
        calls.push({ api, index: match.index });
      }
    }
  }
  return calls.sort((left, right) => left.index - right.index);
}

function forbiddenPersistenceImports(source) {
  const code = rustCodeMask(source);
  const imports = [];
  for (const match of code.matchAll(/(?:^|\n)\s*(?:pub\s*(?:\([^)]*\)\s*)?)?use\s+([^;]+);/g)) {
    const root = match[1].match(
      /(?:^|::)(?:r#)?(postcard|serde_json|bincode|rmp_serde|ciborium)(?=::|\s|\{|$)/,
    )?.[1];
    if (root) imports.push({ root, index: match.index });
  }
  return imports;
}

function sourceAnchor(source, index) {
  const lines = source.split("\n");
  const line = source.slice(0, index).split("\n").length - 1;
  const context = lines
    .slice(Math.max(0, line - 1), line + 2)
    .join("\n")
    .trim();
  return crypto.createHash("sha256").update(context).digest("hex");
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

function isCallAt(source, index) {
  let cursor = index;
  while (/\s/.test(source[cursor] ?? "")) cursor += 1;
  if (source.startsWith("::", cursor)) return true; // turbofish call; the following parser owns syntax.
  return source[cursor] === "(";
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function rawRootApiPattern(api) {
  const [root, ...suffix] = api.split("::");
  return `(?:r#)?${escapeRegex(root)}::${suffix.map(escapeRegex).join("::")}`;
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
