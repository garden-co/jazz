import { describe, expect, it } from "vitest";
import { uniqueDbName } from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";
import { browserTopologyPhase } from "./topology-harness.js";

describe("MusicAgent browser topology preflight", () => {
  it("starts an isolated core topology through the browser command boundary", async () => {
    const topology = await browserTopologyPhase("start MusicAgent topology core", () =>
      getJazzServerInfo(uniqueDbName("music-agent-topology-preflight")),
    );

    expect(topology.serverUrl).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
    expect(topology.adminSecret).toBe("jazz-browser-test-admin");
  }, 15_000);
});
