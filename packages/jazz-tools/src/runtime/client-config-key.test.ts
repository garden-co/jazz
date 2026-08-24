import { describe, expect, it } from "vitest";
import { serializeClientConfig } from "./client-config-key.js";

describe("serializeClientConfig", () => {
  it("canonicalizes top-level and nested config property order", () => {
    expect(
      serializeClientConfig({
        appId: "app",
        serverUrl: "https://jazz.example.com",
        runtimeSources: { baseUrl: "/runtime", wasmUrl: "/runtime/jazz.wasm" },
      }),
    ).toBe(
      serializeClientConfig({
        runtimeSources: { wasmUrl: "/runtime/jazz.wasm", baseUrl: "/runtime" },
        serverUrl: "https://jazz.example.com",
        appId: "app",
      }),
    );
  });

  it("preserves Date's JSON value semantics", () => {
    expect(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmSource: new Date("2026-01-01T00:00:00.000Z") as never },
      }),
    ).toBe(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmSource: new Date("2026-01-01T00:00:00.000Z") as never },
      }),
    );
    expect(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmSource: new Date("2026-01-02T00:00:00.000Z") as never },
      }),
    ).not.toBe(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmSource: new Date("2026-01-01T00:00:00.000Z") as never },
      }),
    );
  });

  it("keeps opaque objects and functions reference-safe", () => {
    class OpaqueRuntimeSource {}
    const first = new OpaqueRuntimeSource();
    const second = new OpaqueRuntimeSource();
    const fn = () => undefined;

    const firstKey = serializeClientConfig({
      appId: "app",
      runtimeSources: { wasmModule: first as never },
    });

    expect(firstKey).toBe(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmModule: first as never },
      }),
    );
    expect(firstKey).not.toBe(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmModule: second as never },
      }),
    );
    expect(
      serializeClientConfig({ appId: "app", runtimeSources: { wasmModule: fn as never } }),
    ).not.toBe(firstKey);
  });
});
