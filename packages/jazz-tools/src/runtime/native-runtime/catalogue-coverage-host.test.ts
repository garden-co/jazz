import { expect, it } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { startLocalJazzServer, startTestJazz } from "../../testing/index.js";
import { deploy } from "../../dev/catalogue.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../default-create-db.js";
import { allowAll } from "../testing/allow-all.js";
import { localAccountConfig, testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { JazzClient } from "../client.js";

type Pending = { poll(): Uint8Array | null; cancel(): void };
type Callback = (first: Error | string | null, second?: string) => void;
type Raw = {
  coverCatalogue(timeout: number, author?: Uint8Array, claims?: unknown): Uint8Array | Pending;
  tableIdentity(table: string): Uint8Array;
  columnIdentity(table: string, column: string): Uint8Array;
  setIdentityClaims(author: Uint8Array, claims: unknown): void;
  setTickScheduler(callback: Callback): void;
  setRemoteLinkHint(hint: string): void;
  registerSchema(bytes: Uint8Array): Raw;
  connectUpstream(): unknown;
  tick(): unknown;
  close(): unknown;
};
const app = s.defineApp(
  { notes: s.table({ title: s.string(), body: s.string() }, {}) },
  { migrations: "none" },
);
const permissions = allowAll(app.wasmSchema);
const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
const author = testAuthorBytes("catalogue-coverage:alice");
const node = new Uint8Array(16).fill(7);
async function open(host: "NAPI" | "WASM"): Promise<Raw> {
  const ctor =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  return ctor.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
}
// Privileged core links use the exact permission bundle they published.
async function startPinnedAuthority() {
  const server = await startLocalJazzServer({ inMemory: true, schema, permissions });
  try {
    const { bundle } = await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    return { ...server, bundle };
  } catch (error) {
    await server.stop();
    throw error;
  }
}

function observe(raw: Raw, overrides: Partial<Raw>): Raw {
  return new Proxy(raw, {
    get(target, key) {
      if (key in overrides) return Reflect.get(overrides, key);
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
function runtime(db: Raw, jsAuthor = author): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(null, schema, node, jsAuthor, 1, false, { db: db as never });
}

for (const host of ["NAPI", "WASM"] as const) {
  for (const late of [false, true]) {
    it(`${host} catalogue deadline wakes a quiet host (${late ? "late" : "early"} scheduler)`, async () => {
      const raw = await open(host);
      await raw.connectUpstream();
      raw.setRemoteLinkHint("live");
      let read: Pending | undefined;
      let adapter: NativeRuntimeAdapter | undefined;
      const wakes: string[] = [];
      let polls = 0;
      let resolve!: (error: unknown) => void;
      let reject!: (error: unknown) => void;
      const done = new Promise<unknown>((yes, no) => {
        resolve = yes;
        reject = no;
      });
      const watched = observe(raw, {
        setTickScheduler(callback) {
          raw.setTickScheduler((first, second) => {
            const wake = typeof first === "string" ? first : second;
            if (wake) wakes.push(wake);
            callback(first, second);
          });
        },
        async tick() {
          await raw.tick();
          if (read) {
            try {
              polls++;
              read.poll();
            } catch (error) {
              resolve(error);
            }
          }
        },
      });
      const watchdog = setTimeout(() => reject(new Error("catalogue deadline did not wake")), 2000);
      const start = performance.now();
      try {
        if (!late) adapter = runtime(watched);
        const started = raw.coverCatalogue(60);
        expect(started).not.toBeInstanceOf(Uint8Array);
        read = started as Pending;
        if (late) adapter = runtime(watched);
        // Polls occur only from production core-hook callbacks, never a test loop.
        expect(String(await done)).toMatch(/timed out|deadline/i);
        expect(performance.now() - start).toBeGreaterThanOrEqual(55);
        expect(wakes.some((wake) => wake.startsWith("after:"))).toBe(true);
        expect(polls).toBeGreaterThan(0);
        expect(polls).toBeLessThan(20);
      } finally {
        clearTimeout(watchdog);
        read?.cancel();
        await adapter?.close();
      }
    });
  }

  it(`${host} prepares several identities once per actual-peer schema handle without registering claims`, async () => {
    const server = await startPinnedAuthority();
    const owner = await open(host);
    const raw = owner.registerSchema(encodeSchema(schema));
    let calls = 0;
    let registrations = 0;
    const watched = observe(raw, {
      coverCatalogue(timeout, requested, claims) {
        calls++;
        expect(requested).toBeUndefined();
        expect(claims).toBeUndefined();
        return raw.coverCatalogue(timeout, requested, claims);
      },
      setIdentityClaims(identity, claims) {
        registrations++;
        raw.setIdentityClaims(identity, claims);
      },
    });
    // A stale JS author must not replace the real opened core peer.
    const adapter = runtime(watched, testAuthorBytes("catalogue-coverage:stale-js"));
    const client = await JazzClient.connectWithRuntime(adapter, {
      appId: server.appId,
      schema,
      serverUrl: server.url,
    });
    try {
      client.connectTransport(server.url, {
        admin_secret: server.adminSecret,
        bundle: server.bundle,
      });
      // Finish normal authenticated startup before deliberately registering
      // this probe's scope. Changing startup claims during admission correctly
      // refuses coverage and is a separate core regression.
      await client.query(JSON.stringify({ table: "notes" }), { tier: "remote" });
      raw.setIdentityClaims(author, { team: "registered" });
      const startupRegistrations = registrations;
      const requests = [
        { table: "notes", columns: ["title", "body", "missing"] },
        { table: "missing" },
      ];
      for (let operation = 1; operation <= 2; operation++) {
        const results = await client.prepareCatalogueIdentitiesInternal(requests, undefined, 5000);
        expect(calls).toBe(operation);
        expect(registrations).toBe(startupRegistrations);
        expect(results[0]?.identity).toMatch(/^[0-9a-f-]{36}$/);
        expect(results[0]?.columns.title).toMatch(/^[0-9a-f-]{36}$/);
        expect(results[0]?.columns.body).toMatch(/^[0-9a-f-]{36}$/);
        expect(results[0]?.columns.missing).toBeNull();
        expect(results[1]?.identity).toBeNull();
      }
    } finally {
      await client.shutdown();
      await owner.close();
      await server.stop();
    }
  }, 15000);

  it(`${host} refuses unsafe raw and SDK catalogue budgets before issuing requests`, async () => {
    const raw = await open(host);
    let calls = 0;
    const adapter = runtime(
      observe(raw, {
        coverCatalogue(timeout, requested, claims) {
          calls++;
          return raw.coverCatalogue(timeout, requested, claims);
        },
      }),
    );
    try {
      for (const timeout of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        expect(() => raw.coverCatalogue(timeout)).toThrow(/safe integer/);
        await expect(adapter.prepareCatalogueIdentities([], undefined, timeout)).rejects.toThrow(
          /safe integer/,
        );
      }
      expect(calls).toBe(0);
    } finally {
      await adapter.close();
    }
  });

  for (const stop of ["close", "discard"] as const) {
    it(`${host} ${stop} cancels a real pending catalogue operation`, async () => {
      const raw = await open(host);
      const adapter = runtime(raw);
      const transport = await adapter.connectUpstreamPeer();
      raw.setRemoteLinkHint("live");
      const result = adapter.prepareCatalogueIdentities([{ table: "notes" }], undefined, 5000).then(
        () => "answered",
        (error: unknown) => String(error),
      );
      try {
        await new Promise((resolve) => setTimeout(resolve, 0));
        await transport.tick();
        const issued = transport.recvWireFrames() as Uint8Array[];
        const requests = issued.filter((frame) => frame[0] === 0);
        expect(requests).toHaveLength(1);
        // This fresh fixture's request id is one complete postcard varint byte.
        const requestId = requests[0]![1]!;
        expect(requestId).toBeLessThan(128);
        await adapter[stop]();
        expect(await result).toMatch(/shutdown|closed/i);
        // Close performs the final tick before retiring the owner. Drain its
        // queued cancellation without asking the retired owner to progress.
        // Discard leaves storage live and still needs ordinary progression.
        if (stop === "discard") await transport.tick();
        const cleanup = transport.recvWireFrames() as Uint8Array[];
        expect(cleanup.filter((frame) => frame[0] === 0)).toHaveLength(0);
        expect(cleanup.some((frame) => frame[0] === 1)).toBe(true);
        const unsubscribes = cleanup.filter((frame) => frame[0] === 1);
        expect(unsubscribes).toHaveLength(1);
        expect(Array.from(unsubscribes[0]!)).toEqual([1, requestId]);
        if (stop === "close" && host === "WASM") {
          await expect(transport.tick()).rejects.toThrow(/storage owner is retired/i);
        }
      } finally {
        await adapter.close();
      }
    });
  }
}

// These are structural identifiers, never row content or membership evidence.
it("prepares distinct public SDK column identities and refuses retained access after shutdown", async () => {
  const publicPermissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
  });
  const server = await startTestJazz({
    app,
    permissions: publicPermissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    db = await createDb(await localAccountConfig(server.appId, server.url));
    // Complete normal authenticated startup. Metadata never installs cached
    // session claims; each getter still requests its own fresh coverage.
    await db.all(app.notes, { tier: "remote" });
    const title = await db.columnIdentity(app.notes, "title");
    const body = await db.columnIdentity(app.notes, "body");
    const table = await db.tableIdentity(app.notes);
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
    expect(title).toMatch(uuid);
    expect(body).toMatch(uuid);
    expect(table).toMatch(uuid);
    expect(title).not.toBe(body);
    expect(title).not.toBe(table);
    expect(body).not.toBe(table);
    expect(await db.columnIdentity(app.notes, "missing")).toBeNull();
    await db.shutdown();
    await expect(db.columnIdentity(app.notes, "title")).rejects.toThrow(/closed|shutdown/i);
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60000);
