/**
 * wasm-bindgen emits the JavaScript import object and its companion `.wasm`
 * together.  A worker bundle may copy the binary separately, so compare the
 * binary's named glue imports to the generated/bundled JavaScript before we
 * publish either artifact.
 */
export function verifyWasmGlueAbi(wasmBytes, glueSource) {
  let imports;
  try {
    imports = WebAssembly.Module.imports(new WebAssembly.Module(wasmBytes));
  } catch (error) {
    return `invalid WASM binary: ${error.message}`;
  }

  const missing = imports
    .filter(
      (entry) =>
        entry.kind === "function" &&
        (entry.module === "wbg" || entry.module === "./jazz_wasm_bg.js") &&
        !glueSource.includes(entry.name),
    )
    .map((entry) => `${entry.module}.${entry.name}`);
  return missing.length ? `WASM glue is missing required imports: ${missing.join(", ")}` : null;
}

export function assertWasmGlueAbi(wasmBytes, glueSource) {
  const problem = verifyWasmGlueAbi(wasmBytes, glueSource);
  if (problem) throw new Error(problem);
}
