// A `.wasm` import, as wrangler bundles one: a compiled WebAssembly module.
declare module "*.wasm" {
  const module: WebAssembly.Module;
  export default module;
}
