import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  startLocalJazzServer: vi.fn(),
  deploy: vi.fn(),
  watchSchema: vi.fn(),
}));

vi.mock("./dev-server.js", () => ({
  startLocalJazzServer: mocks.startLocalJazzServer,
}));
vi.mock("./catalogue-project.js", () => ({
  deploy: mocks.deploy,
}));
vi.mock("./schema-watcher.js", () => ({
  watchSchema: mocks.watchSchema,
}));

import { ManagedDevRuntime } from "./managed-runtime.js";

describe("ManagedDevRuntime server option forwarding", () => {
  let schemaDir: string;

  beforeEach(async () => {
    schemaDir = await mkdtemp(join(tmpdir(), "jazz-managed-forwarding-"));
    await writeFile(join(schemaDir, "schema.ts"), "export default {};\n");

    mocks.startLocalJazzServer.mockReset();
    mocks.startLocalJazzServer.mockResolvedValue({
      appId: "managed-forwarding-app",
      port: 19887,
      url: "http://127.0.0.2:19887",
      dataDir: join(schemaDir, "node_modules", ".cache", "jazz-dev-server"),
      adminSecret: "managed-forwarding-admin",
      backendSecret: "managed-forwarding-backend",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    mocks.deploy.mockReset();
    mocks.deploy.mockResolvedValue({ schema: { hash: "managed-forwarding-schema" } });
    mocks.watchSchema.mockReset();
    mocks.watchSchema.mockReturnValue({ close: vi.fn() });
  });

  it("keeps persistent storage and admin auth when adding an explicit host", async () => {
    const runtime = new ManagedDevRuntime({
      appId: "VITE_JAZZ_APP_ID",
      serverUrl: "VITE_JAZZ_SERVER_URL",
      telemetryCollectorUrl: "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
    });
    const adminSecret = "managed-forwarding-admin";
    const dataDir = join(schemaDir, "node_modules", ".cache", "jazz-dev-server");

    try {
      await runtime.initialize({
        appId: "managed-forwarding-app",
        schemaDir,
        server: { host: "127.0.0.2", adminSecret },
      });

      expect(mocks.startLocalJazzServer).toHaveBeenCalledWith(
        expect.objectContaining({
          host: "127.0.0.2",
          dataDir,
          adminSecret,
        }),
      );
    } finally {
      await runtime.dispose();
      await rm(schemaDir, { recursive: true, force: true });
    }
  });
});
