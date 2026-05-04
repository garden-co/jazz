import { describe, expect, it } from "vitest";

describe("WasmJazzClient binding shape", () => {
  it("is declared by jazz-wasm types", async () => {
    const wasm = await import("jazz-wasm");

    expect(typeof wasm.WasmJazzClient).toBe("function");
  });
});
