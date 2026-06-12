import { describe, expect, it } from "vitest";
import { resolveRuntimeConfigBrokerWorkerUrl } from "./runtime-config.js";

describe("resolveRuntimeConfigBrokerWorkerUrl", () => {
  const moduleUrl = "https://app.example/assets/runtime/runtime-config.js";
  const locationHref = "https://app.example/page";

  it("prefers the explicit brokerWorkerUrl", () => {
    expect(
      resolveRuntimeConfigBrokerWorkerUrl(moduleUrl, locationHref, {
        baseUrl: "https://cdn.example/jazz/",
        brokerWorkerUrl: "https://cdn.example/override/jazz-broker-worker.js",
      }),
    ).toBe("https://cdn.example/override/jazz-broker-worker.js");
  });

  it("derives the broker worker path from baseUrl", () => {
    expect(
      resolveRuntimeConfigBrokerWorkerUrl(moduleUrl, locationHref, {
        baseUrl: "https://cdn.example/jazz/",
      }),
    ).toBe("https://cdn.example/jazz/worker/jazz-broker-worker.js");
  });

  it("falls back to the module-relative path without runtime sources", () => {
    expect(resolveRuntimeConfigBrokerWorkerUrl(moduleUrl, locationHref, undefined)).toBe(
      "https://app.example/assets/worker/jazz-broker-worker.js",
    );
  });
});
