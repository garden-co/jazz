#!/usr/bin/env node
/** Verify the generated artifacts actually loaded by browser correctness tests. */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyWasmGlueAbi } from "./wasm-glue-abi.mjs";
import {
  verifyCorrectnessArtifactConsumerEnvironment,
  verifyCorrectnessArtifactProducer,
} from "./correctness-artifact-producer.mjs";

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

function hasProperty(body, property) {
  return new RegExp(`(?:^|\\n)\\s*(?:readonly\\s+)?${property}\\s*(?::|\\()`, "m").test(body);
}

function hasGetter(body, property) {
  return new RegExp(`(?:^|\\n)\\s*get\\s+${property}\\s*\\(`, "m").test(body);
}

function verifyWasmWriteSurface(generatedTypes, generatedGlue, failures) {
  const generatedWriteTypes = classBody(generatedTypes, "WasmWrite");
  const generatedWriteGlue = classBody(generatedGlue, "WasmWrite");
  if (!hasProperty(generatedWriteTypes, "txId"))
    failures.push("generated WASM WasmWrite declaration is missing txId");
  if (hasProperty(generatedWriteTypes, "batchId"))
    failures.push("generated WASM WasmWrite declaration still exposes batchId");
  if (hasProperty(generatedWriteTypes, "transactionId"))
    failures.push("generated WASM WasmWrite declaration still exposes transactionId");
  if (!hasGetter(generatedWriteGlue, "txId"))
    failures.push("generated WASM WasmWrite glue is missing txId");
  if (hasGetter(generatedWriteGlue, "batchId"))
    failures.push("generated WASM WasmWrite glue still exposes batchId");
  if (hasGetter(generatedWriteGlue, "transactionId"))
    failures.push("generated WASM WasmWrite glue still exposes transactionId");
  if (!/\bwasmwrite_txId\b/.test(generatedTypes) || !/\bwasmwrite_txId\b/.test(generatedGlue))
    failures.push("generated WASM write export is missing wasmwrite_txId");
  if (/\bwasmwrite_batchId\b/.test(generatedTypes) || /\bwasmwrite_batchId\b/.test(generatedGlue))
    failures.push("generated WASM write export still exposes wasmwrite_batchId");
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

export function verifyCorrectnessTestArtifacts(rootDir = root, { allowUnsealedFixture = false } = {}) {
  const failures = [];
  let snapshot;
  try {
    snapshot = verifyCorrectnessArtifactProducer(rootDir).snapshot;
    if (!allowUnsealedFixture && process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1")
      verifyCorrectnessArtifactConsumerEnvironment(rootDir);
  } catch (error) {
    failures.push(error.message);
  }
  // Only unit fixtures opt into the mutable fallback.  Every command-line
  // correctness consumer is admitted through the producer manifest above.
  if (!snapshot && !allowUnsealedFixture) return failures;
  const wasmPackage = snapshot?.wasmPackage ?? resolve(rootDir, "crates/jazz-wasm/pkg");
  try {
    const expected = classBody(
      text("packages/jazz-tools/src/types/jazz-wasm.d.ts", rootDir),
      "WasmDb",
    );
    const generatedTypes = classBody(
      readFileSync(resolve(wasmPackage, "jazz_wasm.d.ts"), "utf8"),
      "WasmDb",
    );
    const generatedGlue = classBody(
      readFileSync(resolve(wasmPackage, "jazz_wasm.js"), "utf8"),
      "WasmDb",
    );
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
    verifyWasmWriteSurface(
      readFileSync(resolve(wasmPackage, "jazz_wasm.d.ts"), "utf8"),
      readFileSync(resolve(wasmPackage, "jazz_wasm.js"), "utf8"),
      failures,
    );
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
    console.error("Fix: pnpm build:correctness-artifacts && pnpm test:typescript-consumers");
    process.exitCode = 1;
  } else console.log("correctness-artifacts: release NAPI + fast WASM binding surface is current");
}
