#!/usr/bin/env node
// Check that no lattice or SHA-3 code is linked: the function names in a WASM
// module and the defined symbols of an unstripped native library.
//
//   check-endorsement-symbols.mjs [--list] RELEASE.wasm NATIVE_LIBRARY
//   check-endorsement-symbols.mjs [--list] --correctness-artifacts
//
// The second form checks the WASM module and Node binding of the sealed
// correctness artifacts the TypeScript partitions build, as CI runs it.
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { functionNames } from "../artifacts/wasm-names.mjs";
import { verifyCorrectnessArtifactProducer } from "../artifacts/correctness-artifact-producer.mjs";

// TLS handshakes and SHA384 occur in the Node binding.
const prohibited = /keccak|sha[-_]?3(?!84)|(?<!hand)shake|ml[-_]?dsa|ml[-_]?kem|x[-_]?wing/i;

export function isProhibitedSymbol(symbol) {
  return prohibited.test(symbol);
}

/** The WASM module and Node binding of the verified sealed correctness artifacts. */
export function correctnessArtifactPaths(root) {
  const { snapshot } = verifyCorrectnessArtifactProducer(root);
  const bindings = readdirSync(snapshot.napiGeneration).filter((name) => name.endsWith(".node"));
  if (bindings.length !== 1)
    throw new Error(`Expected one Node binding in ${snapshot.napiGeneration}: ${bindings}`);
  return [
    join(snapshot.wasmPackage, "jazz_wasm_bg.wasm"),
    join(snapshot.napiGeneration, bindings[0]),
  ];
}

async function main(argv) {
  const { values, positionals } = parseArgs({
    args: argv,
    options: { list: { type: "boolean" }, "correctness-artifacts": { type: "boolean" } },
    allowPositionals: true,
  });
  const usage =
    "Usage: check-endorsement-symbols.mjs [--list] (RELEASE.wasm NATIVE_LIBRARY | --correctness-artifacts)";
  if (values["correctness-artifacts"] ? positionals.length !== 0 : positionals.length !== 2)
    throw new Error(usage);
  const [wasm, native] = values["correctness-artifacts"]
    ? correctnessArtifactPaths(resolve(fileURLToPath(new URL("../..", import.meta.url))))
    : positionals;

  function inspect(symbol) {
    if (values.list) console.log(symbol);
    if (isProhibitedSymbol(symbol)) {
      console.error(`Prohibited symbol: ${symbol}`);
      process.exitCode = 1;
    }
  }

  const names = functionNames(readFileSync(wasm));
  if (!names.length) throw new Error(`No function names in ${wasm}`);
  for (const name of names) inspect(name);
  console.log(`${wasm}: ${names.length} function names`);

  const nm = spawn("nm", ["-C", "--defined-only", native], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  const lines = createInterface({ input: nm.stdout });
  let count = 0;
  lines.on("line", (symbol) => {
    inspect(symbol);
    count++;
  });
  const [code] = await once(nm, "close");
  if (code !== 0 || !count) throw new Error(`Could not read symbols from ${native}`);
  console.log(`${native}: ${count} symbols`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await main(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
