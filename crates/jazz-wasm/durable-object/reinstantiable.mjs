// Run after wasm-pack builds `durable-object/pkg`: give the generated glue a
// way to replace its wasm instance. A Rust panic traps the instance and
// leaves its memory unusable (thread-local borrows held, the allocator
// mid-call, the stack pointer unwound nowhere), and wasm-bindgen's
// `initSync` keeps the first instance forever; `host.js` calls
// `__jazzReinstantiate` after a trap to start over on a fresh one.
//
// The glue's finalizer registries (`*Finalization`, `CLOSURE_DTORS`) free
// into the module-level `wasm`: one left over from the old instance would
// free its objects' and closures' addresses into the new instance's heap.
// So each registry is made by a function, and replaced with a fresh one on
// reinstantiation: the old registries become unreachable, and an
// unreachable registry never calls back. A callback the engine queued
// before the swap still runs, so each registry's callbacks also do nothing
// once the instance it was made for is gone (its generation moved on).
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const glue = process.argv[2] ?? fileURLToPath(new URL("./pkg/jazz_wasm_do.js", import.meta.url));
let source = readFileSync(glue, "utf8");
if (!source.includes("__jazzReinstantiate")) {
  if (!/^let wasmModule, wasm;$/m.test(source) || !source.includes("function initSync(")) {
    throw new Error(`${glue}: wasm-bindgen's glue changed; update reinstantiable.mjs`);
  }
  const declared = (source.match(/new FinalizationRegistry\(/g) ?? []).length;
  const registries = [];
  source = source.replace(
    /^const (\w+) = (\(typeof FinalizationRegistry === 'undefined'\)\n\s+\? .*\n\s+: new FinalizationRegistry\(.*\);)$/gm,
    (_, name, made) => {
      registries.push(name);
      const guarded = made
        .replace(/;$/, "")
        .replace("new FinalizationRegistry(", "__jazzRegistry(");
      return `const __jazzMake${name} = () => ${guarded};\nlet ${name} = __jazzMake${name}();`;
    },
  );
  if (registries.length === 0 || registries.length !== declared) {
    throw new Error(
      `${glue}: found ${declared} finalizer registries, rewrote ${registries.length}; update reinstantiable.mjs`,
    );
  }
  // A function declaration is hoisted, so the registries made above it can
  // use it; the generation lives on it (a let here would not be set yet).
  source += `
/** A finalization registry whose callbacks run only for the instance it was made for. */
function __jazzRegistry(callback) {
    const generation = __jazzRegistry.generation ?? 0;
    return new FinalizationRegistry((held) => {
        if (generation === (__jazzRegistry.generation ?? 0)) callback(held);
    });
}

/** Drop the current instance and instantiate \`module\` afresh (see reinstantiable.mjs). */
export function __jazzReinstantiate(module) {
    __jazzRegistry.generation = (__jazzRegistry.generation ?? 0) + 1;
${registries.map((name) => `    ${name} = __jazzMake${name}();`).join("\n")}
    wasm = undefined;
    return initSync({ module });
}
`;
  writeFileSync(glue, source);
}
