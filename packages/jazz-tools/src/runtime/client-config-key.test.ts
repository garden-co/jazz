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

  it("treats sparse array holes like undefined and null", () => {
    const sparse: unknown[] = [];
    sparse.length = 1;

    const serializeSource = (wasmSource: unknown) =>
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmSource: wasmSource as never },
      });

    expect(serializeSource(sparse)).toBe(serializeSource([undefined]));
    expect(serializeSource(sparse)).toBe(serializeSource([null]));
  });

  it("cannot collide opaque identity with a plain JSON value", () => {
    class OpaqueRuntimeSource {}
    const opaque = new OpaqueRuntimeSource();

    expect(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmModule: opaque as never },
      }),
    ).not.toBe(
      serializeClientConfig({
        appId: "app",
        runtimeSources: { wasmModule: { $jazzOpaqueValue: 0 } as never },
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

  it("rejects cyclic plain configuration with a deliberate error", () => {
    const runtimeSources: Record<string, unknown> = {};
    runtimeSources.self = runtimeSources;

    expect(() =>
      serializeClientConfig({ appId: "app", runtimeSources: runtimeSources as never }),
    ).toThrow(new TypeError("Cyclic values are not supported in client configuration"));
  });
});
