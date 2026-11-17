// Run after wasm-pack builds `durable-object/pkg`: give the generated glue a
// way to replace its wasm instance. A Rust panic traps the instance and
// leaves its memory unusable (thread-local borrows held, the allocator
// mid-call, the stack pointer unwound nowhere), and wasm-bindgen's
// `initSync` keeps the first instance forever; `host.js` calls
// `__jazzReinstantiate` after a trap to start over on a fresh one.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const glue = fileURLToPath(new URL("./pkg/jazz_wasm_do.js", import.meta.url));
let source = readFileSync(glue, "utf8");
if (!source.includes("__jazzReinstantiate")) {
  if (!/^let wasmModule, wasm;$/m.test(source) || !source.includes("function initSync(")) {
    throw new Error(`${glue}: wasm-bindgen's glue changed; update reinstantiable.mjs`);
  }
  source += `
/** Drop the current instance and instantiate \`module\` afresh (see reinstantiable.mjs). */
export function __jazzReinstantiate(module) {
    wasm = undefined;
    return initSync({ module });
}
`;
  writeFileSync(glue, source);
}
