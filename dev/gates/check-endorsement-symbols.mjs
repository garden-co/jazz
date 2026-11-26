#!/usr/bin/env node
// Check the function names in the release WASM and the unstripped Node binding.
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { parseArgs } from "node:util";
import { functionNames } from "../artifacts/wasm-names.mjs";

const { values, positionals } = parseArgs({
  options: { list: { type: "boolean" } },
  allowPositionals: true,
});
if (positionals.length !== 2)
  throw new Error("Usage: check-endorsement-symbols.mjs [--list] RELEASE.wasm NATIVE_LIBRARY");
const [wasm, native] = positionals;

// TLS handshakes and SHA384 occur in the Node binding.
const prohibited = /keccak|sha[-_]?3(?!84)|(?<!hand)shake|ml[-_]?dsa|ml[-_]?kem|x[-_]?wing/i;
function inspect(symbol) {
  if (values.list) console.log(symbol);
  if (prohibited.test(symbol)) {
    console.error(`Prohibited symbol: ${symbol}`);
    process.exitCode = 1;
  }
}

const names = functionNames(readFileSync(wasm));
if (!names.length) throw new Error(`No function names in ${wasm}`);
for (const name of names) inspect(name);
console.log(`${wasm}: ${names.length} function names`);

const nm = spawn("nm", ["-C", "--defined-only", native], { stdio: ["ignore", "pipe", "inherit"] });
const lines = createInterface({ input: nm.stdout });
let count = 0;
lines.on("line", (symbol) => {
  inspect(symbol);
  count++;
});
const [code] = await once(nm, "close");
if (code !== 0 || !count) throw new Error(`Could not read symbols from ${native}`);
console.log(`${native}: ${count} symbols`);
