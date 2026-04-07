import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("loadWasmModule build output", () => {
  it("emits a runtime import shim for the node-only wasm helper", async () => {
    const builtClientUrl = new URL("../../dist/runtime/client.js", import.meta.url);
    const builtClient = await readFile(builtClientUrl, "utf8");

    expect(builtClient).toContain('new Function("specifier", "return import(specifier)")');
    expect(builtClient).not.toMatch(/import\(\s*\/\* @vite-ignore \*\/\s*helperSpecifier/s);
  });

  it("keeps the copied worker entry on the runtime import shim path", async () => {
    const builtWorkerUrl = new URL("../../dist/worker/jazz-worker.ts", import.meta.url);
    const builtWorker = await readFile(builtWorkerUrl, "utf8");

    expect(builtWorker).toMatch(
      /const runtimeImportModule = new Function\(\s*"specifier",\s*"return import\(specifier\)",\s*\)/s,
    );
    expect(builtWorker).toContain('await runtimeImportModule("jazz-wasm")');
    expect(builtWorker).not.toContain('await import("jazz-wasm")');
  });
});
