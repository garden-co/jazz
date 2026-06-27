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
});
