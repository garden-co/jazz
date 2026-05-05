// Indirection so tests can mock the dynamic import to simulate a missing
// peer dependency or other resolution failures. Production code calls through
// here and the bundler still treats it as a normal `import("jazz-wasm")`.
export function importJazzWasm(): Promise<typeof import("jazz-wasm")> {
  return import("jazz-wasm");
}
