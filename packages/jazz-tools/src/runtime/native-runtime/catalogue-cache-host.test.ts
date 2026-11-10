import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { startLocalJazzServer } from "../../testing/index.js";
import { deploy } from "../../dev/catalogue.js";
import { allowAll } from "../testing/allow-all.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { JazzClient } from "../client.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = allowAll(app.wasmSchema);
const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} restores actual authority metadata before starting the new replica`, async () => {
    const server = await startLocalJazzServer({ inMemory: true, schema, permissions });
    const ctor =
      host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
    const alice = testAuthorBytes("catalogue-cache:alice");
    const bob = testAuthorBytes("catalogue-cache:bob");
    const node = new Uint8Array(16).fill(91);
    const owner = ctor.openMemory(encodeSchema(schema), openConfig(node, alice, 1, false));
    const adapter = new NativeRuntimeAdapter(null, schema, node, alice, 1, false, {
      db: owner as never,
    });
    let client: JazzClient | undefined;
    let restored: ReturnType<typeof ctor.openMemoryWithCatalogue> | undefined;
    try {
      const { bundle } = await deploy({
        appId: server.appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      // The actual Alice-owned replica learns app metadata over the existing
      // authenticated bundle-pinned core transport; this is not a user-origin
      // app frontend or a fabricated Catalogue frame.
      client = await JazzClient.connectWithRuntime(adapter, {
        appId: server.appId,
        schema,
        serverUrl: server.url,
      });
      client.connectTransport(server.url, { admin_secret: server.adminSecret, bundle });
      await client.query(JSON.stringify({ table: "notes" }), { tier: "remote" });
      const state = await owner.takeAuthenticatedCatalogueState();
      expect(state.ready).toBe(true);
      expect(state.capture).toBeInstanceOf(Uint8Array);
      if (!state.capture) throw new Error("authenticated capture is unavailable");
      const identity = Array.from(owner.tableIdentity("notes"));
      expect(identity).toHaveLength(16);
      expect(typeof ctor.openMemoryWithCatalogue).toBe("function");
      restored = ctor.openMemoryWithCatalogue(
        encodeSchema(schema),
        openConfig(new Uint8Array(16).fill(92), bob, 1, false),
        state.capture,
      );
      expect(await restored.hasAuthenticatedCatalogue()).toBe(true);
      expect(Array.from(restored.tableIdentity("notes"))).toEqual(identity);
      expect((await restored.takeAuthenticatedCatalogueState()).capture).toBeUndefined();
      if (host === "NAPI") {
        const { NapiDb } = await loadNapiModule();
        const directory = await mkdtemp(join(tmpdir(), "jazz-catalogue-reopen-"));
        try {
          // The second actual factory open must retain the persisted replica
          // marker while distinguishing it from publication in this lifetime.
          for (let opening = 0; opening < 2; opening++) {
            const persistent = NapiDb.openPersistentWithCatalogue(
              directory,
              encodeSchema(schema),
              openConfig(new Uint8Array(16).fill(95), bob, 1, false),
              state.capture,
            );
            try {
              expect(await persistent.hasAuthenticatedCatalogue()).toBe(true);
              expect(Array.from(persistent.tableIdentity("notes"))).toEqual(identity);
            } finally {
              await persistent.close();
            }
          }
        } finally {
          await rm(directory, { recursive: true, force: true });
        }
      }
      expect(typeof ctor.openMemoryWithSelfSignedProofAndCatalogue).toBe("function");
      expect(() =>
        ctor.openMemoryWithSelfSignedProofAndCatalogue(
          encodeSchema(schema),
          openConfig(new Uint8Array(16).fill(93), bob, 1, false),
          "invalid-proof",
          server.appId,
          "bob",
          state.capture!,
        ),
      ).toThrow();
      expect(() =>
        ctor.openMemoryWithCatalogue(
          encodeSchema(schema),
          openConfig(new Uint8Array(16).fill(94), bob, 1, false),
          state.capture!.slice(0, -1),
        ),
      ).toThrow();
      await restored.close();
      expect(() => restored!.hasAuthenticatedCatalogue()).toThrow(/closed|retired/i);
      restored = undefined;
    } finally {
      await restored?.close();
      if (client) await client.shutdown();
      else await adapter.close();
      await owner.close();
      await server.stop();
    }
  }, 20_000);
}
