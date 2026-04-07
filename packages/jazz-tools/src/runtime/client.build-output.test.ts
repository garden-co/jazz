import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("loadWasmModule build output", () => {
  it("keeps the node-only wasm bootstrap on process.getBuiltinModule", async () => {
    const builtClientUrl = new URL("../../dist/runtime/client.js", import.meta.url);
    const builtClient = await readFile(builtClientUrl, "utf8");

    expect(builtClient).toContain('process.getBuiltinModule?.("module")');
    expect(builtClient).toContain('process.getBuiltinModule?.("fs")');
    expect(builtClient).toContain('process.getBuiltinModule?.("path")');
    expect(builtClient).not.toContain('import("node:module")');
  });

  it("keeps the copied worker entry on the vitest-compatible import path", async () => {
    const builtWorkerUrl = new URL("../../dist/worker/jazz-worker.ts", import.meta.url);
    const builtWorker = await readFile(builtWorkerUrl, "utf8");

    expect(builtWorker).toContain("__vitest_browser_runner__");
    expect(builtWorker).toContain('await import("jazz-wasm")');
    expect(builtWorker).not.toContain('await runtimeImportModule("jazz-wasm")');
  });
});
