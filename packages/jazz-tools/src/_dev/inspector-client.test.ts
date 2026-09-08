import { afterEach, describe, expect, it, vi } from "vitest";
import { createInspectorAttachmentClient } from "./inspector-client.js";
import { createDbWithRuntimeSource } from "../runtime/db.js";
import { createBrowserPhysicalDatabaseName } from "../runtime/browser-worker-config.js";
import { assertAccountConfig, copyAccountConfigAdmission } from "../accounts/config-capability.js";

vi.mock("../runtime/db.js", async (original) => ({
  ...(await original<typeof import("../runtime/db.js")>()),
  createDbWithRuntimeSource: vi.fn(),
}));
const ports: MessagePort[] = [];
afterEach(() => {
  for (const port of ports.splice(0)) port.close();
  vi.clearAllMocks();
});

describe("private inspector attachment admission", () => {
  it("closes the worker peer if native construction fails after verified preflight", async () => {
    const channel = new MessageChannel();
    ports.push(channel.port1, channel.port2);
    let receivedClose = false;
    channel.port2.addEventListener("message", ({ data }) => {
      if (data.type === "inspect-binding") {
        ports.push(data.leasePort);
        channel.port2.postMessage({
          type: "inspector-binding",
          id: data.id,
          binding: data.binding,
        });
      }
      if (data.type === "close") receivedClose = true;
    });
    channel.port2.start();
    const host = {
      appId: "inspector-test",
      jwtToken: `header.${btoa(JSON.stringify({ iss: "https://inspector.test", sub: "reader" }))}.signature`,
      accountId: "00000000-0000-4000-8000-000000000001",
      accountRegistryAuthority: "http://localhost/apps/inspector-test/accounts",
    };
    vi.mocked(createDbWithRuntimeSource).mockImplementation(async (config) => {
      expect(() => assertAccountConfig(config)).not.toThrow();
      // Copying scope metadata cannot recreate a capability, and an admitted
      // attachment cannot be normalized into an ordinary account context.
      expect(() => assertAccountConfig({ ...config })).toThrow("account_handle_required");
      expect(() => copyAccountConfigAdmission(config, { ...config, runtimeSources: {} })).toThrow();
      throw new Error("native load failed");
    });
    await expect(
      createInspectorAttachmentClient(
        host,
        host.appId,
        createBrowserPhysicalDatabaseName(host, "logical"),
        channel.port1,
      ),
    ).rejects.toThrow("native load failed");
    expect(createDbWithRuntimeSource).toHaveBeenCalledOnce();
    await vi.waitFor(() => expect(receivedClose).toBe(true));
  });
});
