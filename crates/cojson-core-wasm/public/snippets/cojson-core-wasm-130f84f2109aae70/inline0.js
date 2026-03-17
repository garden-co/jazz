
export function dumpWasmPanic(message) {
  queueMicrotask(() => {
    throw new Error(`Wasm panic: ${message}`);
  })
}
