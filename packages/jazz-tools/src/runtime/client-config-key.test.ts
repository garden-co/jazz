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
});
