import { access } from "node:fs/promises";
import { afterEach, describe, expect, it } from "vitest";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { getAvailablePort } from "./test-helpers.js";

describe("dev-server re-export compatibility", () => {
  it("exports startLocalJazzServer and pushSchemaCatalogue from jazz-tools/testing path", async () => {
    const testing = await import("../testing/index.js");
    expect(typeof testing.startLocalJazzServer).toBe("function");
    expect(typeof testing.pushSchemaCatalogue).toBe("function");
  });

  it("exports the same functions from dev/index.ts", async () => {
    const dev = await import("./index.js");
    expect(typeof dev.startLocalJazzServer).toBe("function");
    expect(typeof dev.pushSchemaCatalogue).toBe("function");
    expect(typeof dev.watchSchema).toBe("function");
  });

  it("testing and dev export the same startLocalJazzServer reference", async () => {
    const testing = await import("../testing/index.js");
    const dev = await import("./index.js");
    expect(testing.startLocalJazzServer).toBe(dev.startLocalJazzServer);
    expect(testing.pushSchemaCatalogue).toBe(dev.pushSchemaCatalogue);
  });
});

describe("startLocalJazzServer via DevServer", () => {
  let handle: LocalJazzServerHandle | null = null;

  afterEach(async () => {
    if (handle) {
      await handle.stop();
      handle = null;
    }
  });

  it("starts a server and returns a working handle", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({ port, adminSecret: "test-admin" });

    expect(handle.port).toBe(port);
    expect(handle.url).toBe(`http://127.0.0.1:${port}`);
    expect(handle.adminSecret).toBe("test-admin");

    const healthResponse = await fetch(`${handle.url}/health`);
    expect(healthResponse.ok).toBe(true);
  }, 30_000);

  it("stops the server cleanly", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({ port });
    const url = handle.url;
    await handle.stop();
    handle = null;

    await expect(fetch(`${url}/health`).then((r) => r.ok)).rejects.toThrow();
  }, 30_000);

  it("passes edge upstream and peer secret options through DevServer", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({
      port,
      upstreamUrl: "ws://127.0.0.1:9",
      peerSecret: "cluster-peer-secret",
      inMemory: true,
    });

    expect(handle.port).toBe(port);
    const healthResponse = await fetch(`${handle.url}/health`);
    expect(healthResponse.ok).toBe(true);
  }, 30_000);

  it("uses an isolated temp data dir by default and cleans it up on stop", async () => {
    let first: LocalJazzServerHandle | null = null;
    let second: LocalJazzServerHandle | null = null;

    try {
      first = await startLocalJazzServer();
      const firstDataDir = first.dataDir;
      expect(firstDataDir).not.toBe("./data");
      await access(firstDataDir);

      await first.stop();
      first = null;
      await expect(access(firstDataDir)).rejects.toThrow();

      second = await startLocalJazzServer();
      expect(second.dataDir).not.toBe(firstDataDir);
      await access(second.dataDir);
    } finally {
      if (first) {
        await first.stop();
      }
      if (second) {
        await second.stop();
      }
    }
  }, 30_000);
});
