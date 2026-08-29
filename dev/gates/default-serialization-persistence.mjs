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
  "postcard::to_extend",
  "postcard::to_io",
  "postcard::from_bytes",
  "postcard::take_from_bytes",
  "postcard::experimental::serialized_size",
  "serde_json::to_vec",
  "serde_json::to_writer",
  "serde_json::to_writer_pretty",
  "serde_json::to_string",
  "serde_json::to_string_pretty",
  "serde_json::from_slice",
  "serde_json::from_reader",
  "serde_json::from_str",
  "bincode::serialize",
  "bincode::serialize_into",
  "bincode::deserialize",
  "bincode::deserialize_from",
  "rmp_serde::to_vec",
  "rmp_serde::to_vec_named",
  "rmp_serde::from_slice",
  "rmp_serde::from_read",
]);
const forbidden = new RegExp(
  `\\b(?:${[...forbiddenApis]
    .sort((left, right) => right.length - left.length)
    .map((api) => api.replaceAll("::", "::"))
    .join("|")})\\b`,
  "g",
);
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
  const matches = [...source.matchAll(apiPattern(allowance.api))];
  if (matches.length !== allowance.expectedOccurrences)
    fail(
      `${allowance.id}: expected ${allowance.expectedOccurrences} ${allowance.api} occurrence(s) in ${allowance.path}, found ${matches.length}`,
    );
}

for (const [relative, source] of files) {
  for (const match of source.matchAll(forbidden)) {
    const permitted = registry.allowances.some(
      (allowance) => allowance.path === relative && allowance.api === match[0],
    );
    if (!permitted) {
      const line = source.slice(0, match.index).split("\n").length;
      fail(`${relative}:${line}: unregistered default serialization ${match[0]}`);
    }
  }
}

function apiPattern(api) {
  return new RegExp(`\\b${api.replaceAll("::", "::")}\\b`, "g");
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
