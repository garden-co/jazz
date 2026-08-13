#!/usr/bin/env node
/** Verify the generated artifacts actually loaded by browser correctness tests. */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyManifest } from "./provenance.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");

function text(path, rootDir = root) {
  const full = resolve(rootDir, path);
  if (!existsSync(full)) throw new Error(`missing ${path}`);
  return readFileSync(full, "utf8");
}

function classBody(source, name) {
  const start = source.indexOf(`class ${name}`);
  if (start < 0) throw new Error(`missing ${name} declaration`);
  const open = source.indexOf("{", start);
  let depth = 0;
  for (let i = open; i < source.length; i++) {
    if (source[i] === "{") depth++;
    if (source[i] === "}" && --depth === 0) return source.slice(open + 1, i);
  }
  throw new Error(`unterminated ${name} declaration`);
}

function arityFromDeclaration(body, method) {
  const match = body.match(new RegExp(`(?:^|\\n)\\s*(?:static\\s+)?${method}\\(([^)]*)\\)`, "m"));
  if (!match) throw new Error(`missing WasmDb.${method}`);
  return match[1].trim() === "" ? 0 : match[1].split(",").length;
}

function arityFromGlue(body, method) {
  const match = body.match(new RegExp(`\\n\\s*${method}\\(([^)]*)\\)\\s*\\{`, "m"));
  if (!match) throw new Error(`generated JS is missing WasmDb.${method}`);
  return match[1].trim() === "" ? 0 : match[1].split(",").length;
}

export function verifyCorrectnessTestArtifacts(rootDir = root) {
  const failures = [];
  for (const [kind, profile] of [
    ["wasm", "fast"],
    ["napi", "release"],
  ]) {
    try {
      const problem = verifyManifest(rootDir, kind, profile);
      if (problem) failures.push(`STALE ${kind} ${profile}: ${problem}`);
    } catch (error) {
      failures.push(`STALE ${kind} ${profile}: ${error.message}`);
    }
  }
  try {
    const expected = classBody(
      text("packages/jazz-tools/src/types/jazz-wasm.d.ts", rootDir),
      "WasmDb",
    );
    const generatedTypes = classBody(
      text("crates/jazz-wasm/pkg/jazz_wasm.d.ts", rootDir),
      "WasmDb",
    );
    const generatedGlue = classBody(text("crates/jazz-wasm/pkg/jazz_wasm.js", rootDir), "WasmDb");
    // These are the worker-boundary entry points whose arity is observable at
    // runtime and has previously drifted when bindgen glue was stale.
    for (const method of ["connectUpstream", "acceptSubscriber"]) {
      const sourceArity = arityFromDeclaration(expected, method);
      const typeArity = arityFromDeclaration(generatedTypes, method);
      const glueArity = arityFromGlue(generatedGlue, method);
      if (sourceArity !== typeArity || sourceArity !== glueArity)
        failures.push(
          `WASM ABI drift for WasmDb.${method}: consumer=${sourceArity}, d.ts=${typeArity}, glue=${glueArity}`,
        );
    }
    const rust = text("crates/jazz/src/wire.rs", rootDir);
    const ts = text("packages/jazz-tools/src/runtime/native-runtime/websocket.ts", rootDir);
    const rustVersion = rust.match(/^pub const WIRE_PROTOCOL_VERSION: u16 = (\d+);$/m)?.[1];
    const tsVersion = ts.match(/^export const WIRE_PROTOCOL_VERSION = (\d+);$/m)?.[1];
    if (!rustVersion || !tsVersion) failures.push("could not read Rust/TS wire protocol versions");
    else if (rustVersion !== tsVersion)
      failures.push(`wire protocol version mismatch: Rust=${rustVersion}, TS=${tsVersion}`);
  } catch (error) {
    failures.push(error.message);
  }
  return failures;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const failures = verifyCorrectnessTestArtifacts();
  if (failures.length) {
    for (const failure of failures) console.error(`correctness-artifacts: ${failure}`);
    console.error("Fix: pnpm build:test-artifacts");
    process.exitCode = 1;
  } else console.log("correctness-artifacts: release NAPI + fast WASM binding surface is current");
}
