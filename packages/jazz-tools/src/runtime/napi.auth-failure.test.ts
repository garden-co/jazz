import { randomUUID } from "node:crypto";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { startLocalJazzServer } from "../testing/local-jazz-server.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { httpUrlToWs } from "./url.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";

const MINIMAL_SCHEMA: WasmSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
};

beforeAll(async () => {
  await loadNapiModule();
});

describe("NAPI on_auth_failure", () => {
  it("fires callback with reason when server rejects auth on handshake", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-auth-failure-backend-secret";
    const adminSecret = "napi-auth-failure-admin-secret";

    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });

    const dataDir = await mkdtemp(join(tmpdir(), "jazz-napi-auth-failure-"));
    const dataPath = join(dataDir, "runtime.db");

    const { NapiRuntime } = await loadNapiModule();
    const runtime = new NapiRuntime(
      serializeRuntimeSchema(MINIMAL_SCHEMA),
      appId,
      "test",
      "main",
      dataPath,
    );

    try {
      const reasons: string[] = [];

      // onAuthFailure should exist on the runtime (this is what we're implementing).
      (runtime as unknown as { onAuthFailure(cb: (reason: string) => void): void }).onAuthFailure(
        (reason: string) => {
          reasons.push(reason);
        },
      );

      // Connect with an intentionally invalid JWT. The server requires
      // backendSecret auth; supplying a bad JWT triggers Unauthorized.
      runtime.connect(
        httpUrlToWs(server.url),
        JSON.stringify({ jwt_token: "definitely.invalid.jwt" }),
      );

      // Wait up to 10s for the server to reject and the callback to fire.
      const deadline = Date.now() + 10_000;
      while (reasons.length === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      expect(reasons.length).toBeGreaterThan(0);
      expect(reasons[0]).toMatch(/unauth/i);
    } finally {
      runtime.disconnect();
      await server.stop();
    }
  }, 30_000);
});
