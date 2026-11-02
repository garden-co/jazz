import { describe, expect, it } from "vitest";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { schema as s } from "../../index.js";
import { JazzClient, createOpenTransactionId } from "../client.js";
import { startLocalJazzServer } from "../../testing/index.js";
import { allowAll } from "../testing/allow-all.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

type PendingRead = { poll(): Uint8Array | null; cancel(): void };
type TickCallback = (first: Error | string | null, second?: string) => void;
type RawDb = {
  acceptedRead(
    query: Uint8Array,
    id: string,
    includeDeleted: boolean,
    timeoutMs: number,
    author?: Uint8Array,
    claims?: unknown,
  ): Uint8Array | PendingRead;
  beginTransaction(
    id: string,
    kind: string,
    author?: Uint8Array,
    attribution?: Uint8Array,
    acceptedEvidence?: boolean,
  ): void;
  rollbackTransaction(id: string): void;
  commitTransaction(id: string, kind: string): { wait(tier: string): Promise<unknown> };
  registerSchema(schema: Uint8Array): RawDb;
  setSessionClaims(claims: unknown): void;
  setIdentityClaims(author: Uint8Array, claims: unknown): void;
  setTickScheduler(callback: TickCallback): void;
  setRemoteLinkHint(hint: string): void;
  tick(): unknown;
  connectUpstream(): unknown;
  close(): unknown;
};
const app = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
const author = testAuthorBytes("accepted-read-host:alice");
const node = new Uint8Array(16).fill(7);
const query = queryFromTable("notes");

async function open(
  host: "NAPI" | "WASM",
  backend = false,
  schema = app.wasmSchema,
): Promise<RawDb> {
  const ctor =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  return ctor[backend ? "openMemoryAsBackend" : "openMemory"](
    encodeSchema(schema),
    openConfig(node, author, 1, false),
  );
}

function pending(read: Uint8Array | PendingRead): PendingRead {
  expect(read).not.toBeInstanceOf(Uint8Array);
  return read as PendingRead;
}

// Only the exercised boundary is observed. Every other method is bound to the
// real NAPI/WASM receiver, including its scheduler, evaluator and cancellation.
function observe(raw: RawDb, overrides: Partial<RawDb>): RawDb {
  return new Proxy(raw, {
    get(target, key) {
      if (key in overrides) return Reflect.get(overrides, key);
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
function runtime(db: RawDb): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(null, app.wasmSchema, node, author, 1, false, {
    db: db as never,
  });
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

for (const host of ["NAPI", "WASM"] as const) {
  describe(`${host} accepted host lifecycle`, () => {
    for (const installAfterRead of [false, true]) {
      it(`finishes a quiet accepted deadline with the existing SDK timer (${installAfterRead ? "late" : "early"} hook)`, async () => {
        const raw = await open(host);
        await raw.connectUpstream();
        raw.setRemoteLinkHint("live");
        const done = deferred<Uint8Array>();
        const wakes: string[] = [];
        let read: PendingRead | undefined;
        let polls = 0;
        let adapter: NativeRuntimeAdapter | undefined;
        const start = performance.now();
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
                polls += 1;
                const bytes = read.poll();
                if (bytes !== null) done.resolve(bytes);
              } catch (error) {
                done.reject(error);
              }
            }
          },
        });
        const watchdog = setTimeout(
          () => done.reject(new Error("core deadline did not wake host")),
          2000,
        );
        try {
          if (!installAfterRead) adapter = runtime(watched);
          const id = createOpenTransactionId();
          raw.beginTransaction(id, "serializable", undefined, undefined, true);
          read = pending(raw.acceptedRead(query, id, false, 70));
          if (installAfterRead) adapter = runtime(watched);
          // No transport frames, polling loop or test-driven tick. Re-polling
          // occurs only after the production adapter handles a core callback.
          await expect(done.promise).rejects.toThrow(/expired|deadline/i);
          expect(performance.now() - start).toBeGreaterThanOrEqual(65);
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

    for (const stop of ["close", "discard"] as const) {
      it(`${stop} cancels real issued and queued transaction reads before replacement`, async () => {
        const raw = await open(host);
        const reads: PendingRead[] = [];
        const cancelled = new Set<PendingRead>();
        const admitted = deferred<void>();
        const watched = observe(raw, {
          acceptedRead(bytes, id, includeDeleted, timeoutMs, author, claims) {
            const read = pending(
              raw.acceptedRead(bytes, id, includeDeleted, timeoutMs, author, claims),
            );
            reads.push(read);
            if (reads.length === 2) admitted.resolve();
            return {
              poll: () => read.poll(),
              cancel: () => {
                read.cancel();
                cancelled.add(read);
              },
            };
          },
        });
        const adapter = runtime(watched);
        const transport = await adapter.connectUpstreamPeer();
        raw.setRemoteLinkHint("live");
        const id = createOpenTransactionId();
        adapter.beginTransaction("serializable", id, undefined, "accepted-evidence");
        // Both accepted reads share B's ordinary transaction read queue.
        const outcomes = [0, 1].map(() =>
          adapter.queryAccepted(JSON.stringify({ table: "notes" }), id, undefined, 5000).then(
            () => "answered",
            (error: unknown) => String(error),
          ),
        );
        const watchdog = setTimeout(
          () => admitted.reject(new Error("reads were not admitted")),
          2000,
        );
        try {
          await admitted.promise;
          // Let both SDK continuations register in the existing cancel set.
          await new Promise((resolve) => setTimeout(resolve, 0));
          expect(reads.every((read) => read.poll() === null)).toBe(true);
          await transport.tick();
          // Read=0 and Unsubscribe=1 are pinned by sqlite_sync_wire_v1.json.
          // Exactly one request crossed the real host boundary: the second
          // read is still queued behind the first in the transaction.
          const issued = transport.recvWireFrames() as Uint8Array[];
          const requests = issued.filter((frame) => frame[0] === 0);
          expect(requests).toHaveLength(1);
          await adapter[stop]();
          expect(cancelled.size).toBe(2);
          const results = await Promise.all(outcomes);
          expect(results.every((result) => result.includes("runtime shutdown"))).toBe(true);
          for (const read of reads) expect(() => read.poll()).toThrow(/complete|cancelled/i);
          await transport.tick();
          const cleanup = transport.recvWireFrames() as Uint8Array[];
          expect(cleanup.filter((frame) => frame[0] === 0)).toHaveLength(0);
          const unsubscribes = cleanup.filter((frame) => frame[0] === 1);
          expect(unsubscribes).toHaveLength(1);
          // The fixture starts with small request ids, so this byte is their
          // complete postcard varint, in both the request and unsubscribe.
          expect(unsubscribes[0]![1]).toBe(requests[0]![1]);
          // Reusing the opaque transaction id on a replacement owner cannot
          // adopt either old read. A new accepted read reaches its own deadline.
          const replacement = runtime(await open(host));
          try {
            replacement.beginTransaction("serializable", id, undefined, "accepted-evidence");
            await expect(
              replacement.queryAccepted(JSON.stringify({ table: "notes" }), id, undefined, 30),
            ).rejects.toThrow(/expired|deadline/i);
            await replacement.rollbackTransaction(id);
          } finally {
            await replacement.close();
          }
        } finally {
          clearTimeout(watchdog);
          await adapter.close();
          await raw.close();
        }
      });
    }
  });
}

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} keeps omitted/false admission ordinary and rejects invalid opt-in without a handle`, async () => {
    const raw = await open(host);
    try {
      for (const flag of [undefined, false]) {
        const id = createOpenTransactionId();
        raw.beginTransaction(id, "serializable", undefined, undefined, flag);
        expect(() => raw.acceptedRead(query, id, false, 100)).toThrow(/bound session/);
        raw.rollbackTransaction(id);
      }
      for (const [kind, attribution] of [
        ["mergeable", undefined],
        ["serializable", author],
      ] as const) {
        const id = createOpenTransactionId();
        expect(() => raw.beginTransaction(id, kind, undefined, attribution, true)).toThrow();
        raw.beginTransaction(id, "serializable", author, undefined, true);
        const read = pending(raw.acceptedRead(query, id, false, 1000));
        read.cancel();
        raw.rollbackTransaction(id);
      }
      const id = createOpenTransactionId();
      raw.beginTransaction(id, "serializable", author);
      for (const timeout of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
        expect(() => raw.acceptedRead(query, id, false, timeout)).toThrow(
          /safe integer|range|finite|nonnegative/,
        );
      const read = pending(raw.acceptedRead(query, id, false, Number.MAX_SAFE_INTEGER));
      read.cancel();
      raw.rollbackTransaction(id);
    } finally {
      await raw.close();
    }
  });

  it(`${host} takes implicit identity and claims from the core across a schema view`, async () => {
    const raw = await open(host);
    raw.setSessionClaims({ team: "a" });
    const seen: unknown[][] = [];
    // Deliberately stale JS identity: only the real opening DbIdentity may
    // supply implicit accepted admission, including trusted-serving views.
    const watched = observe(raw, {
      beginTransaction(...args) {
        seen.push(args);
        raw.beginTransaction(...args);
      },
    });
    const adapter = new NativeRuntimeAdapter(
      null,
      app.wasmSchema,
      node,
      testAuthorBytes("stale-js-peer"),
      1,
      false,
      {
        db: watched as never,
        readAuthorizationHost: "trusted-serving",
      },
    );
    const view = adapter.registerSchemaView(app.wasmSchema);
    const id = createOpenTransactionId();
    try {
      view.beginTransaction("serializable", id, undefined, "accepted-evidence");
      expect(seen).toEqual([[id, "serializable", undefined, undefined, true]]);
      // Matching the actual peer is accepted by the shared Binding session
      // check. A stale JS guess would fail here before any read could pend.
      const probe = pending(raw.acceptedRead(query, id, false, 1000, author, { team: "a" }));
      probe.cancel();
      await adapter.rollbackTransaction(id);
      const next = createOpenTransactionId();
      view.beginTransaction("serializable", next, undefined, "accepted-evidence");
      const result = view.queryAccepted(JSON.stringify({ table: "notes" }), next, undefined, 30);
      await expect(result).rejects.toThrow(/expired|deadline/i);
      await adapter.rollbackTransaction(next);
    } finally {
      await adapter.close();
    }
  });

  it(`${host} validates SDK timeout and refuses late upgrade while ordinary reads still work`, async () => {
    const adapter = runtime(await open(host));
    const id = createOpenTransactionId();
    try {
      adapter.beginTransaction("serializable", id);
      await expect(
        adapter.queryAccepted(JSON.stringify({ table: "notes" }), id, undefined, 100),
      ).rejects.toThrow(/before transaction opening/);
      expect(
        await adapter.query(
          JSON.stringify({ table: "notes" }),
          null,
          "local-only",
          JSON.stringify({ transaction_id: id }),
        ),
      ).toEqual([]);
      await adapter.rollbackTransaction(id);
      const accepted = createOpenTransactionId();
      adapter.beginTransaction("serializable", accepted, undefined, "accepted-evidence");
      for (const timeout of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
        await expect(
          adapter.queryAccepted(JSON.stringify({ table: "notes" }), accepted, undefined, timeout),
        ).rejects.toThrow(/safe integer/);
      await expect(
        adapter.queryAccepted(JSON.stringify({ table: "notes" }), accepted, undefined, 30),
      ).rejects.toThrow(/expired|deadline/i);
      await adapter.rollbackTransaction(accepted);
    } finally {
      await adapter.close();
    }
  });
}

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} refuses implicit SYSTEM and accepted attribution before admission but keeps ordinary backend attribution`, async () => {
    const raw = await open(host, true);
    try {
      for (const attribution of [undefined, author]) {
        const id = createOpenTransactionId();
        expect(() =>
          raw.beginTransaction(id, "serializable", undefined, attribution, true),
        ).toThrow(/explicit session|attribution/);
        // The rejected id can still be opened with an explicit session.
        raw.beginTransaction(id, "serializable", author, undefined, true);
        const read = pending(raw.acceptedRead(query, id, false, 1000, author));
        read.cancel();
        raw.rollbackTransaction(id);
      }
      const id = createOpenTransactionId();
      raw.beginTransaction(id, "serializable", undefined, author, false);
      expect(() => raw.acceptedRead(query, id, false, 1000)).toThrow(/bound session/);
      raw.rollbackTransaction(id);
    } finally {
      await raw.close();
    }
  });

  it(`${host} raw premature commit consumes the handle and cannot adopt an old read on reuse`, async () => {
    const raw = await open(host);
    const adapter = runtime(raw);
    const id = createOpenTransactionId();
    try {
      raw.beginTransaction(id, "serializable", undefined, undefined, true);
      const read = pending(raw.acceptedRead(query, id, false, 1000));
      expect(() => raw.commitTransaction(id, "serializable")).toThrow(/unfinished read/);
      raw.beginTransaction(id, "serializable", undefined, undefined, true);
      expect(() => read.poll()).toThrow();
      read.cancel();
      const replacement = pending(raw.acceptedRead(query, id, false, 1000));
      expect(replacement.poll()).toBeNull();
      replacement.cancel();
      raw.rollbackTransaction(id);
    } finally {
      await adapter.close();
    }
  });

  it(`${host} decodes real paired evidence through the authority and commits a read-only explicit session`, async () => {
    const server = await startLocalJazzServer({
      inMemory: true,
      schema: app.wasmSchema,
      permissions: allowAll(app.wasmSchema),
    });
    const adapter = runtime(await open(host));
    const client = await JazzClient.connectWithRuntime(adapter, {
      appId: server.appId,
      schema: app.wasmSchema,
      serverUrl: server.url,
    });
    try {
      client.connectTransport(server.url, { admin_secret: server.adminSecret });
      const inserted = client.insert("notes", {
        body: { type: "Text", value: "paired authority row" },
      });
      await inserted.wait({ tier: "global", timeoutMs: 5000 });
      const id = createOpenTransactionId();
      const [account_id, issuer, user_id] = JSON.parse(new TextDecoder().decode(author));
      adapter.beginTransaction(
        "serializable",
        id,
        JSON.stringify({ account_id, issuer, user_id }),
        "accepted-evidence",
      );
      const result = await adapter.queryAccepted(
        JSON.stringify({ table: "notes" }),
        id,
        undefined,
        5000,
      );
      expect(result.rows).toEqual([
        { table: "notes", id: inserted.value.id, values: inserted.value.values },
      ]);
      expect(result.evidence).toHaveLength(1);
      expect(result.evidence[0]?.content.transactionId).toBe(await inserted.txId);
      expect(result.evidence[0]?.state).toEqual(result.evidence[0]?.content);
      const committed = adapter.commitTransaction(id);
      await adapter.waitForTransaction(committed, "global");
    } finally {
      await client.shutdown();
      await server.stop();
    }
  }, 15000);
}

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} rejected explicit admission leaves claims intact and failed registration abandons its batch`, async () => {
    const raw = await open(host);
    raw.setSessionClaims({ team: "original" });
    let registrations = 0;
    let rejectIdentity = true;
    let failRegistration = true;
    const watched = observe(raw, {
      beginTransaction(id, kind, explicitAuthor, attribution, flag) {
        // Exercise the real host's authentication failure before registration.
        raw.beginTransaction(
          id,
          kind,
          rejectIdentity ? new Uint8Array([0xff]) : explicitAuthor,
          attribution,
          flag,
        );
      },
      setIdentityClaims(identity, claims) {
        registrations += 1;
        if (failRegistration) throw new Error("registration unavailable");
        raw.setIdentityClaims(identity, claims);
      },
    });
    const adapter = runtime(watched);
    const [account_id, issuer, user_id] = JSON.parse(new TextDecoder().decode(author));
    const session = JSON.stringify({ account_id, issuer, user_id, claims: { team: "new" } });
    const id = createOpenTransactionId();
    const assertOriginalClaims = () => {
      raw.beginTransaction(id, "serializable", undefined, undefined, true);
      const read = pending(raw.acceptedRead(query, id, false, 1000, author, { team: "original" }));
      read.cancel();
      raw.rollbackTransaction(id);
    };
    try {
      expect(() =>
        adapter.beginTransaction("mergeable", id, session, "accepted-evidence"),
      ).toThrow();
      expect(() =>
        adapter.beginTransaction("serializable", id, session, "accepted-evidence"),
      ).toThrow();
      expect(registrations).toBe(0);
      assertOriginalClaims();
      rejectIdentity = false;
      expect(() =>
        adapter.beginTransaction("serializable", id, session, "accepted-evidence"),
      ).toThrow("registration unavailable");
      expect(registrations).toBe(1);
      assertOriginalClaims(); // same opaque ID proves the failed begin was abandoned
      failRegistration = false;
      adapter.beginTransaction("serializable", id, session, "accepted-evidence");
      expect(registrations).toBe(2);
      await adapter.rollbackTransaction(id);
    } finally {
      await adapter.close();
    }
  });

  it(`${host} never reinstalls stale explicit claims during accepted queries`, async () => {
    const server = await startLocalJazzServer({
      inMemory: true,
      schema: app.wasmSchema,
      permissions: allowAll(app.wasmSchema),
    });
    const raw = await open(host);
    let registrations = 0;
    const adapter = runtime(
      observe(raw, {
        setIdentityClaims(identity, claims) {
          registrations += 1;
          raw.setIdentityClaims(identity, claims);
        },
      }),
    );
    const client = await JazzClient.connectWithRuntime(adapter, {
      appId: server.appId,
      schema: app.wasmSchema,
      serverUrl: server.url,
    });
    const [account_id, issuer, user_id] = JSON.parse(new TextDecoder().decode(author));
    const session = JSON.stringify({ account_id, issuer, user_id, claims: { team: "before" } });
    const id = createOpenTransactionId();
    try {
      client.connectTransport(server.url, { admin_secret: server.adminSecret });
      await client
        .insert("notes", { body: { type: "Text", value: "scope admission" } })
        .wait({ tier: "global", timeoutMs: 5000 });
      adapter.beginTransaction("serializable", id, session, "accepted-evidence");
      expect(
        (await adapter.queryAccepted(JSON.stringify({ table: "notes" }), id, undefined, 5000)).rows,
      ).toHaveLength(1);
      raw.setSessionClaims({ team: "after", iss: issuer, sub: user_id });
      await expect(
        adapter.queryAccepted(JSON.stringify({ table: "notes" }), id, session, 1000),
      ).rejects.toThrow(/claims changed/);
      expect(registrations).toBe(1);
      expect(() => raw.commitTransaction(id, "serializable")).toThrow(/claims changed/);
      raw.beginTransaction(id, "serializable", undefined, undefined, true);
      const replacement = pending(
        raw.acceptedRead(query, id, false, 1000, author, {
          team: "after",
          iss: issuer,
          sub: user_id,
        }),
      );
      replacement.cancel();
      raw.rollbackTransaction(id);
    } finally {
      await client.shutdown();
      await server.stop();
    }
  }, 15000);
}

for (const host of ["NAPI", "WASM"] as const) {
  for (const operation of [
    "insert",
    "restore",
    "update",
    "upsert",
    "delete",
    "partial",
    "streaming",
  ] as const) {
    for (const mode of ["same-peer", "drift", "empty-drift"] as const) {
      it(`${host} accepted ${operation} preserves ${mode} identity and live claims`, async () => {
        const server = await startLocalJazzServer({
          inMemory: true,
          schema: app.wasmSchema,
          permissions: allowAll(app.wasmSchema),
        });
        const raw = await open(
          host,
          false,
          mergePermissionsIntoWasmSchema(app.wasmSchema, allowAll(app.wasmSchema)),
        );
        let registrations = 0;
        const tracked = observe(raw, {
          setIdentityClaims: (identity, claims) => {
            registrations++;
            raw.setIdentityClaims(identity, claims);
          },
        });
        const adapter = new NativeRuntimeAdapter(null, app.wasmSchema, node, author, 1, false, {
          db: tracked as never,
          ...(mode !== "same-peer" ? { readAuthorizationHost: "trusted-serving" as const } : {}),
        });
        const client = await JazzClient.connectWithRuntime(adapter, {
          appId: server.appId,
          schema: app.wasmSchema,
          serverUrl: server.url,
        });
        const [account_id, issuer, user_id] = JSON.parse(new TextDecoder().decode(author));
        const scope = { account_id, issuer, user_id, claims: { team: "before" } };
        const id = createOpenTransactionId();
        try {
          client.connectTransport(server.url, { admin_secret: server.adminSecret });
          const row = await client
            .insert("notes", { body: { type: "Text", value: "seed" } })
            .wait({ tier: "global", timeoutMs: 5000 });
          if (operation === "restore")
            await client.delete("notes", row.id).wait({ tier: "global", timeoutMs: 5000 });
          adapter.beginTransaction("serializable", id, JSON.stringify(scope), "accepted-evidence");
          // Hydrate ordinary write coverage under the same admitted claims.
          // Accepted evidence deliberately does not populate that cache.
          await adapter.query(JSON.stringify({ table: "notes" }), JSON.stringify(scope), "remote");
          await adapter.queryAccepted(
            JSON.stringify({ table: "notes", ...(mode === "empty-drift" ? { limit: 0 } : {}) }),
            id,
            undefined,
            5000,
          );
          if (mode === "same-peer")
            await adapter.query(
              JSON.stringify({ table: "notes" }),
              undefined,
              "local-only",
              JSON.stringify({ transaction_id: id }),
            );
          const registered = registrations;
          if (mode !== "same-peer")
            raw.setSessionClaims({ team: "after", iss: issuer, sub: user_id });
          const context = JSON.stringify({ ...scope, transaction_id: id });
          const cells = { body: { type: "Text" as const, value: "changed" } };
          // All shared mutation entry points must preserve the admitted identity
          // without reinstating claims from this stale write context.
          if (operation === "insert") adapter.insert("notes", cells, context);
          else if (operation === "restore") adapter.restore("notes", row.id, cells, context);
          else if (operation === "update") adapter.update("notes", row.id, cells, context);
          else if (operation === "upsert") adapter.upsert("notes", row.id, cells, context);
          else if (operation === "delete") adapter.delete("notes", row.id, context);
          else if (operation === "streaming") {
            // Integration carries Anselm's transaction upload API. Exercise
            // the same capability guard before that path registers claims.
            await adapter.streamingMutation(
              "update",
              "notes",
              {},
              "body",
              (async function* () {
                yield "changed";
              })(),
              context,
              row.id,
            );
          } else adapter.updateLargeValues("notes", row.id, cells, [], context);
          expect(registrations).toBe(registered);
          if (mode === "same-peer") {
            const tx = adapter.commitTransaction(id);
            await adapter.waitForTransaction(tx, "global");
          } else {
            expect(() => adapter.commitTransaction(id)).toThrow(/claims changed/);
          }
          if (operation === "streaming") {
            const rows = await client.query(JSON.stringify({ table: "notes" }), { tier: "remote" });
            expect(rows).toHaveLength(1);
            expect(rows[0]?.values.body).toEqual({
              type: "Text",
              value: mode === "same-peer" ? "changed" : "seed",
            });
          }
        } finally {
          await client.shutdown();
          await server.stop();
        }
      }, 15000);
    }
  }
}

for (const host of ["NAPI", "WASM"] as const) {
  for (const retained of [false, true]) {
    for (const explicit of [false, true]) {
      it(`${host} stable mixed reads inherit live claims (${retained ? "view" : "owner"}/${explicit ? "explicit" : "implicit"})`, async () => {
        const server = await startLocalJazzServer({
          inMemory: true,
          schema: app.wasmSchema,
          permissions: allowAll(app.wasmSchema),
        });
        const owner = await open(
          host,
          false,
          mergePermissionsIntoWasmSchema(app.wasmSchema, allowAll(app.wasmSchema)),
        );
        const raw = retained
          ? owner.registerSchema(
              encodeSchema(
                mergePermissionsIntoWasmSchema(app.wasmSchema, allowAll(app.wasmSchema)),
              ),
            )
          : owner;
        const adapter = runtime(raw);
        const client = await JazzClient.connectWithRuntime(adapter, {
          appId: server.appId,
          schema: app.wasmSchema,
          serverUrl: server.url,
        });
        const [account_id, issuer, user_id] = JSON.parse(new TextDecoder().decode(author));
        const scope = { account_id, issuer, user_id, claims: { team: "before" } };
        const id = createOpenTransactionId();
        try {
          client.connectTransport(server.url, { admin_secret: server.adminSecret });
          await client
            .insert("notes", { body: { type: "Text", value: "settle session" } })
            .wait({ tier: "global", timeoutMs: 5000 });
          raw.setSessionClaims({ team: "before", iss: issuer, sub: user_id });
          adapter.beginTransaction(
            "serializable",
            id,
            explicit ? JSON.stringify(scope) : undefined,
            "accepted-evidence",
          );
          await adapter.queryAccepted(JSON.stringify({ table: "notes" }), id, undefined, 5000);
          await expect(
            adapter.query(
              JSON.stringify({ table: "notes" }),
              undefined,
              "local-only",
              JSON.stringify({ transaction_id: id }),
            ),
          ).resolves.toBeDefined();
          raw.setSessionClaims({ team: "after", iss: issuer, sub: user_id });
          await expect(
            adapter.query(
              JSON.stringify({ table: "notes" }),
              undefined,
              "local-only",
              JSON.stringify({ transaction_id: id }),
            ),
          ).rejects.toThrow(/claims|scope/);
          expect(() => adapter.commitTransaction(id)).toThrow(/claims|scope/);
        } finally {
          await client.shutdown();
          if (retained) await owner.close();
          await server.stop();
        }
      }, 15000);
    }
  }
}
