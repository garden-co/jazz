#!/usr/bin/env node
// Check that no lattice or SHA-3 code is linked into a shipped build: the
// function names of a WASM module and the defined symbols of an unstripped
// native library or executable. jazz-e2ee depends on `slh-dsa`, which carries
// SHAKE (SHA-3) parameter sets no build may use; only dead-code elimination
// keeps them out, so this checks the linked result.
//
//   check-no-lattice-sha3-symbols.mjs [--list] FILE...
//   check-no-lattice-sha3-symbols.mjs [--list] --correctness-artifacts
//
// Each FILE is a WASM module (by its magic number) or a native library or
// executable (read with nm). The package build checks the published modules
// with the first form; CI checks the native libraries with it too
// (dev/gates/native-crypto-symbols.sh). The second form checks the WASM module
// and Node binding of the sealed correctness artifacts the TypeScript
// partitions build.
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

const isWasm = (bytes) =>
  bytes.length >= 4 &&
  bytes[0] === 0 &&
  bytes[1] === 0x61 &&
  bytes[2] === 0x73 &&
  bytes[3] === 0x6d;

async function nativeSymbols(path, inspect) {
  const nm = spawn("nm", ["-C", "--defined-only", path], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  const lines = createInterface({ input: nm.stdout });
  let count = 0;
  lines.on("line", (symbol) => {
    inspect(symbol);
    count++;
  });
  const [code] = await once(nm, "close");
  if (code !== 0 || !count) throw new Error(`Could not read symbols from ${path}`);
  return count;
}

async function main(argv) {
  const { values, positionals } = parseArgs({
    args: argv,
    options: { list: { type: "boolean" }, "correctness-artifacts": { type: "boolean" } },
    allowPositionals: true,
  });
  const usage =
    "Usage: check-no-lattice-sha3-symbols.mjs [--list] (FILE... | --correctness-artifacts)";
  if (values["correctness-artifacts"] ? positionals.length !== 0 : positionals.length < 1)
    throw new Error(usage);
  const files = values["correctness-artifacts"]
    ? correctnessArtifactPaths(resolve(fileURLToPath(new URL("../..", import.meta.url))))
    : positionals;

  function inspect(symbol) {
    if (values.list) console.log(symbol);
    if (isProhibitedSymbol(symbol)) {
      console.error(`Prohibited symbol: ${symbol}`);
      process.exitCode = 1;
    }
  }

  for (const file of files) {
    const bytes = readFileSync(file);
    if (isWasm(bytes)) {
      const names = functionNames(bytes);
      if (!names.length) throw new Error(`No function names in ${file}`);
      for (const name of names) inspect(name);
      console.log(`${file}: ${names.length} function names`);
    } else {
      console.log(`${file}: ${await nativeSymbols(file, inspect)} symbols`);
    }
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await main(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
