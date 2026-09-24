import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  start: vi.fn(),
}));

vi.mock("jazz-napi", () => ({
  JazzServer: { start: mocks.start },
}));

import { startLocalJazzServer } from "./dev-server.js";

describe("startLocalJazzServer option forwarding", () => {
  beforeEach(() => {
    mocks.start.mockReset();
    mocks.start.mockResolvedValue({
      appId: "forwarding-app",
      port: 19886,
      url: "http://192.0.2.10:19886",
      dataDir: "/tmp/jazz-forwarding-data",
      adminSecret: "forwarding-admin",
      backendSecret: "forwarding-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
  });

  it("keeps persistent storage and admin auth when adding an explicit host", async () => {
    const dataDir = "/tmp/jazz-forwarding-data";
    const adminSecret = "forwarding-admin";

    const handle = await startLocalJazzServer({
      host: "192.0.2.10",
      dataDir,
      adminSecret,
    });

    expect(mocks.start).toHaveBeenCalledWith(
      expect.objectContaining({
        host: "192.0.2.10",
        dataDir,
        adminSecret,
      }),
    );
    expect(handle.url).toBe("http://192.0.2.10:19886");
    await handle.stop();
  });
});
