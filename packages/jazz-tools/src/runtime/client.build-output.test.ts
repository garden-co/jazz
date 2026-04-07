import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("loadWasmModule build output", () => {
  it("emits a runtime import shim for the node-only wasm helper", async () => {
    const builtClientUrl = new URL("../../dist/runtime/client.js", import.meta.url);
    const builtClient = await readFile(builtClientUrl, "utf8");

    expect(builtClient).toContain('new Function("specifier", "return import(specifier)")');
    expect(builtClient).not.toMatch(/import\(\s*\/\* @vite-ignore \*\/\s*helperSpecifier/s);
  });
});
