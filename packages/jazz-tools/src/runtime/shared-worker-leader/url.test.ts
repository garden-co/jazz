// packages/jazz-tools/src/runtime/shared-worker-leader/url.test.ts
import { describe, expect, it } from "vitest";
import { resolveSharedWorkerLeaderUrl } from "./url.js";

describe("resolveSharedWorkerLeaderUrl", () => {
  const moduleUrl = "https://example.test/_next/static/jazz/runtime/db.js";
  const locationHref = "https://example.test/index.html";

  it("returns sharedWorkerLeaderUrl verbatim when explicitly provided", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, {
      sharedWorkerLeaderUrl: "https://cdn.example.test/jazz-leader.js",
    });
    expect(url).toBe("https://cdn.example.test/jazz-leader.js");
  });

  it("derives the leader URL from baseUrl when no explicit URL is set", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, {
      baseUrl: "/jazz/",
    });
    expect(url).toBe("https://example.test/jazz/shared-worker-leader/shared-worker-leader.js");
  });

  it("falls back to a module-relative URL when neither override is set", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, undefined);
    expect(url).toMatch(/\/shared-worker-leader\/shared-worker-leader\.js$/);
  });

  it("preserves an absolute sharedWorkerLeaderUrl regardless of locationHref", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, undefined, {
      sharedWorkerLeaderUrl: "https://other.example.test/leader.js",
    });
    expect(url).toBe("https://other.example.test/leader.js");
  });
});
