import { randomUUID } from "node:crypto";
import { WebSocket } from "undici";
import { expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { startLocalJazzServer } from "../testing/index.js";
import { NativeRuntimeAdapter, encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { webSocketUrl } from "./native-runtime/websocket.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { testAccountId } from "./testing/account-fixtures.js";

const schema = {
  notes: {
    columns: [{ name: "owner", column_type: { type: "Text" }, nullable: false }],
    policies: {
      select: { using: { type: "True" } },
      insert: {
        with_check: {
          type: "Cmp",
          column: "owner",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "owner"] },
        },
      },
    },
  },
} satisfies WasmSchema;

it.each(["ordinary-backend", "ordinary-session", "backend"])(
  "settles explicit session writes using only admitted claims (%s)",
  async (route) => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;
    const { NapiDb } = await loadNapiModule();
    const appId = randomUUID();
    const server = await startLocalJazzServer({
      appId,
      inMemory: true,
      schema,
      permissions: { notes: schema.notes.policies },
    });
    const node = new Uint8Array(16).fill(1);
    const session = {
      issuer: "https://issuer.example",
      user_id: "alice",
      account_id: testAccountId("native-session-claim-admission"),
      claims: { owner: "alice" },
    };
    const author = new TextEncoder().encode(
      JSON.stringify([session.account_id, session.issuer, session.user_id]),
    );
    const config = openConfig(node, author, 1, false);
    const native =
      route === "backend"
        ? NapiDb.openMemoryAsBackend(encodeSchema(schema), config)
        : NapiDb.openMemory(encodeSchema(schema), config);
    const runtime = NativeRuntimeAdapter.fromDb(native as never, schema, node, author, 1, false);
    try {
      runtime.connect(
        webSocketUrl(server.url, appId),
        JSON.stringify({
          backend_secret: server.backendSecret,
          ...(route === "ordinary-session" ? { backend_session: session } : {}),
        }),
      );
      const write = runtime.insert(
        "notes",
        { owner: { type: "Text", value: "alice" } },
        JSON.stringify(session),
      );
      if (write.kind !== "committed") throw new Error("Expected a committed session write");
      const settled = runtime.waitForTransaction(await write.txId, "global");
      if (route === "ordinary-backend") {
        await expect(settled).rejects.toMatchObject({
          kind: "rejected",
          code: "permission_denied",
        });
      } else {
        await settled;
      }
    } finally {
      await runtime.close();
      await server.stop();
    }
  },
  15_000,
);
