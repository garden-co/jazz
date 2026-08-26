#!/usr/bin/env node
/** Verify the generated artifacts actually loaded by browser correctness tests. */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyManifest } from "./provenance.mjs";
import { verifyWasmGlueAbi } from "./wasm-glue-abi.mjs";
import { readCorrectnessArtifactSnapshot } from "./test-artifact-store.mjs";

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
  return parameterCount(methodParameters(body, method, `missing WasmDb.${method}`));
}

function arityFromGlue(body, method) {
  return parameterCount(methodParameters(body, method, `generated JS is missing WasmDb.${method}`));
}

function methodParameters(body, method, error) {
  const match = new RegExp(`(?:^|\\n)\\s*(?:static\\s+)?${method}\\s*\\(`, "m").exec(body);
  if (!match) throw new Error(error);
  const start = match.index + match[0].length;
  let depth = 1;
  for (let index = start; index < body.length; index++) {
    if (body[index] === "(") depth++;
    else if (body[index] === ")" && --depth === 0) return body.slice(start, index);
  }
  throw new Error(`unterminated WasmDb.${method} parameters`);
}

// TypeScript parameter types may contain commas in generic, tuple, function,
// or object types. Count only commas at the parameter-list top level, so an
// ABI check measures runtime arguments rather than type syntax.
function parameterCount(parameters) {
  if (parameters.trim() === "") return 0;
  let count = 1;
  const closes = { "<": ">", "(": ")", "[": "]", "{": "}" };
  const stack = [];
  for (const character of parameters) {
    if (closes[character]) stack.push(closes[character]);
    else if (stack.at(-1) === character) stack.pop();
    else if (character === "," && stack.length === 0) count++;
  }
  return count;
}

export function verifyCorrectnessTestArtifacts(rootDir = root) {
  const failures = [];
  let snapshot;
  try {
    snapshot = readCorrectnessArtifactSnapshot(rootDir);
    if (!snapshot) failures.push("missing worktree-private correctness artifact snapshot");
  } catch (error) {
    failures.push(error.message);
  }
  // The fallback keeps this verifier useful for its deliberately minimal unit
  // fixtures. Real correctness invocation is rejected above without a sealed
  // snapshot, but still reports every independently detectable ABI defect.
  const wasmPackage = snapshot?.wasmPackage ?? resolve(rootDir, "crates/jazz-wasm/pkg");
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
      readFileSync(resolve(wasmPackage, "jazz_wasm.d.ts"), "utf8"),
      "WasmDb",
    );
    const generatedGlue = classBody(readFileSync(resolve(wasmPackage, "jazz_wasm.js"), "utf8"), "WasmDb");
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
    const expectedTransport = classBody(
      text("packages/jazz-tools/src/types/jazz-wasm.d.ts", rootDir),
      "WasmTransport",
    );
    const generatedTransportTypes = classBody(
      readFileSync(resolve(wasmPackage, "jazz_wasm.d.ts"), "utf8"),
      "WasmTransport",
    );
    const generatedTransportGlue = classBody(
      readFileSync(resolve(wasmPackage, "jazz_wasm.js"), "utf8"),
      "WasmTransport",
    );
    const transportMethod = "recvAuxiliaryWireFrames";
    const sourceArity = arityFromDeclaration(expectedTransport, transportMethod);
    const typeArity = arityFromDeclaration(generatedTransportTypes, transportMethod);
    const glueArity = arityFromGlue(generatedTransportGlue, transportMethod);
    if (sourceArity !== typeArity || sourceArity !== glueArity)
      failures.push(
        `WASM ABI drift for WasmTransport.${transportMethod}: consumer=${sourceArity}, d.ts=${typeArity}, glue=${glueArity}`,
      );
    const workerWasm = resolve(rootDir, "packages/jazz-tools/dist/worker/jazz_wasm_bg.wasm");
    const workerGlue = resolve(rootDir, "packages/jazz-tools/dist/worker/jazz-broker-worker.js");
    if (!existsSync(workerWasm) || !existsSync(workerGlue)) {
      failures.push("browser worker artifacts are missing");
    } else {
      const problem = verifyWasmGlueAbi(readFileSync(workerWasm), readFileSync(workerGlue, "utf8"));
      if (problem) failures.push(`broker worker ${problem}`);
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
