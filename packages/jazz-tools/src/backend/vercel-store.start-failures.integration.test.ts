import { describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { openVercelCore } from "./vercel-store.js";
import {
  cleanups,
  fakeVercel,
  runCleanupsAfterEach,
  startTrustedCore,
} from "./fixtures/vercel-store.js";

runCleanupsAfterEach();

describe("vercel() store: cores that fail to start", () => {
  it("stops the Sandbox of a core that can't start, and holds off retrying", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());

    // The core stops (idle), and its next start can never succeed.
    fake.state.status = "stopped";
    fake.state.failStarts = {
      kind: "permanent",
      why: "could not download the core for 1.2.3 from https://registry.example/core.tgz (HTTP 404)",
      times: Infinity,
    };
    // Past the window in which a wake is trusted without a check.
    await new Promise((resolve) => setTimeout(resolve, 10_500));
    const t0 = Date.now();
    await expect(opened.handle("GET", "/health", [], new Uint8Array())).rejects.toThrow(
      /core jazz-prj-test-test could not start: could not download the core for 1\.2\.3 .*HTTP 404/,
    );
    // Reported within one health round, not after the minute-long wake deadline,
    // and the Sandbox isn't kept awake.
    expect(Date.now() - t0).toBeLessThan(15_000);
    expect(fake.state.status).toBe("stopped");
    // The next request fails at once, without waking the Sandbox again.
    const calls = fake.calls.length;
    await expect(opened.handle("GET", "/health", [], new Uint8Array())).rejects.toThrow(
      /could not start/,
    );
    expect(fake.calls.length).toBe(calls);
  }, 60_000);

  it("never reads an error file it didn't cause as its start's failure", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    // Left by an earlier session's start (say, before the URL was fixed),
    // and not yet removed by this session's start script.
    fake.files.set(
      "/tmp/jazz-1.2.3.error",
      Buffer.from("jazz-start-earlier permanent: could not download the core (HTTP 404)\n"),
    );
    fake.state.failing = true;
    // This session's first starts come up slowly: the stale file is still
    // there while the core isn't listening yet.
    fake.state.slowStarts = 2;
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.calls).not.toContain("stop");
  }, 60_000);

  it("retries a start that failed for a transient reason, and keeps the Sandbox", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    fake.state.failStarts = {
      kind: "transient",
      why: "could not download the core for 1.2.3 from https://registry.example/core.tgz (fetch failed: ECONNRESET)",
      times: 2,
    };
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.state.status).toBe("running");
    expect(fake.calls).not.toContain("stop");
    expect(fake.calls.filter((call) => call.startsWith("start ")).length).toBeGreaterThanOrEqual(3);
  }, 60_000);
});
