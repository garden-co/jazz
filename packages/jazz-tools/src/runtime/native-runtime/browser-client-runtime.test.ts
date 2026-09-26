import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../../index.js";
import { createAccountDbWithRuntimeSource } from "../../accounts/context.js";
import { DefaultRuntimeSource } from "../default-runtime-source.js";
import type { Runtime } from "../client.js";
import type { Db, DbConfig } from "../db.js";
import type { WasmSchema } from "../../drivers/types.js";
import { localAccountConfig } from "../testing/account-fixtures.js";
import { translateQuery } from "../query-adapter.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { BrowserClientBindingHost } from "./browser-client-binding-host.js";
import { BrowserClientRuntime } from "./browser-client-runtime.js";
import type {
  ClientBindingEvent,
  ClientBindingRequest,
} from "./browser-client-binding-protocol.js";

// Exercise public Db/schema/query/mutation APIs over real structured-clone
// MessagePorts and a real native client. No second replica or mock data source.
class PortSource extends DefaultRuntimeSource {
  readonly natives = new Set<NativeRuntimeAdapter>();
  readonly bindings: BrowserClientBindingHost[] = [];
  readonly clients: BrowserClientRuntime[] = [];
  protected override wrapClientRuntime(
    runtime: NativeRuntimeAdapter,
    _config: DbConfig,
    schema: WasmSchema,
  ): Runtime {
    this.natives.add(runtime);
    const channel = new MessageChannel();
    this.bindings.push(new BrowserClientBindingHost(runtime, channel.port2));
    const client = new BrowserClientRuntime(schema, channel.port1);
    this.clients.push(client);
    return client;
  }
  async disposeNative(): Promise<void> {
    await Promise.all(this.bindings.map((binding) => binding.close()));
    for (const runtime of this.natives) await runtime.close();
  }
}
const app = s.defineApp({
  documents: s.table(
    { label: s.string().default("untitled"), rank: s.int(), payload: s.bytes() },
    {},
  ),
});
let source: PortSource;
let db: Db;
let config: Awaited<ReturnType<typeof localAccountConfig>>;
const dbs: Db[] = [];
beforeEach(async () => {
  source = new PortSource();
  config = await localAccountConfig(`client-port-${crypto.randomUUID()}`);
  db = await createAccountDbWithRuntimeSource(config, source);
  dbs.push(db);
  await db.all(app.documents, { tier: "local" });
});
afterEach(async () => {
  vi.restoreAllMocks();
  while (dbs.length) await dbs.pop()!.shutdown();
  await source.disposeNative();
});

it("preserves synchronous row handles, defaults, ordered values, updates, deletes and restores", async () => {
  const first = db.insert(app.documents, { rank: 2, payload: new Uint8Array([0, 255]) });
  const second = db.insert(app.documents, {
    label: "first",
    rank: 1,
    payload: new Uint8Array([8]),
  });
  expect(first).not.toBeInstanceOf(Promise);
  expect(first.value.label).toBe("untitled");
  await Promise.all([first.wait({ tier: "local" }), second.wait({ tier: "local" })]);
  expect(await db.all(app.documents.orderBy("rank", "asc"), { tier: "local" })).toEqual([
    second.value,
    first.value,
  ]);
  await db.update(app.documents, first.value.id, { label: "updated" }).wait({ tier: "local" });
  expect(
    (await db.one(app.documents.where({ id: first.value.id }), { tier: "local" }))?.label,
  ).toBe("updated");
  await db.delete(app.documents, first.value.id).wait({ tier: "local" });
  expect(await db.all(app.documents, { tier: "local" })).toEqual([second.value]);
  await db
    .restore(app.documents, first.value.id, {
      label: "restored",
      rank: 3,
      payload: new Uint8Array([7]),
    })
    .wait({ tier: "local" });
  expect(
    (await db.one(app.documents.where({ id: first.value.id }), { tier: "local" }))?.label,
  ).toBe("restored");
});

it("preserves exact ordered subscription snapshots across insert, move and delete", async () => {
  const a = db.insert(app.documents, { label: "a", rank: 1, payload: new Uint8Array([1]) });
  const b = db.insert(app.documents, { label: "b", rank: 2, payload: new Uint8Array([2]) });
  await Promise.all([a.wait({ tier: "local" }), b.wait({ tier: "local" })]);
  const snapshots: string[][] = [];
  const errors: Error[] = [];
  const stop = db.subscribe(
    app.documents.orderBy("rank", "asc"),
    {
      onUpdate: (rows) => snapshots.push(rows.map((row) => row.label)),
      onError: (error) => errors.push(error),
    },
    { tier: "local" },
  );
  try {
    await expect.poll(() => snapshots.at(-1)).toEqual(["a", "b"]);
    await db.update(app.documents, b.value.id, { rank: 0 }).wait({ tier: "local" });
    await expect.poll(() => snapshots.at(-1)).toEqual(["b", "a"]);
    await db.delete(app.documents, a.value.id).wait({ tier: "local" });
    await expect.poll(() => snapshots.at(-1)).toEqual(["b"]);
    expect(errors).toEqual([]);
  } finally {
    stop();
  }
});

it("preserves a typed partial large-value query", async () => {
  const inserted = db.insert(app.documents, { rank: 1, payload: new Uint8Array([4, 5, 6, 7]) });
  await inserted.wait({ tier: "local" });
  const rows = await db.all(
    app.documents.where({ id: inserted.value.id }).select({ payload: { from: 1, to: 3 } }),
    { tier: "local" },
  );
  expect(rows).toEqual([{ id: inserted.value.id, payload: new Uint8Array([5, 6]) }]);
});

it.each(["mergeable", "exclusive"] as const)(
  "owns staged reads and commit for %s transactions",
  async (kind) => {
    const transaction =
      kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
    const staged = transaction.insert(app.documents, {
      label: "staged",
      rank: 1,
      payload: new Uint8Array([3]),
    });
    expect(staged).not.toBeInstanceOf(Promise);
    expect(await transaction.all(app.documents, { tier: "local" })).toEqual([staged]);
    expect(await db.all(app.documents, { tier: "local" })).toEqual([]);
    await transaction.commit().wait({ tier: "local" });
    expect(await db.all(app.documents, { tier: "local" })).toEqual([staged]);
  },
);

it("closing one port rolls back its open transaction and leaves its sibling usable", async () => {
  const sibling = await createAccountDbWithRuntimeSource(config, source);
  dbs.push(sibling);
  await sibling.all(app.documents, { tier: "local" });
  const transaction = db.beginTransaction();
  transaction.insert(app.documents, { label: "private", rank: 1, payload: new Uint8Array([9]) });
  await transaction.all(app.documents, { tier: "local" });
  await db.shutdown();
  expect(await sibling.all(app.documents, { tier: "local" })).toEqual([]);
  const visible = sibling.insert(app.documents, {
    label: "public",
    rank: 2,
    payload: new Uint8Array([2]),
  });
  await visible.wait({ tier: "local" });
  expect(await sibling.all(app.documents, { tier: "local" })).toEqual([visible.value]);
});

it("a pending query does not serialize a later resident query on the same port", async () => {
  const row = db.insert(app.documents, {
    label: "resident",
    rank: 1,
    payload: new Uint8Array([1]),
  });
  await row.wait({ tier: "local" });
  // A controlled suspension is necessary to establish dispatch independence
  // deterministically; both queries still execute the real native API.
  const native = [...source.natives][0]!;
  const query = native.query.bind(native);
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  vi.spyOn(native, "query").mockImplementationOnce(async (...args) => {
    entered();
    await gate;
    return query(...args);
  });
  const pending = db.all(app.documents.where({ label: "absent" }), { tier: "local" });
  try {
    await started;
    expect(await db.all(app.documents.where({ label: "resident" }), { tier: "local" })).toEqual([
      row.value,
    ]);
  } finally {
    release();
  }
  expect(await pending).toEqual([]);
});

it("rejects another port's transaction identity at the host boundary", async () => {
  const transaction = db.beginTransaction();
  const row = transaction.insert(app.documents, { rank: 1, payload: new Uint8Array([1]) });
  await transaction.all(app.documents, { tier: "local" });
  // This boundary test deliberately bypasses the ordinary client guard: a
  // caller supplying a valid foreign handle must be rejected by the host too.
  const channel = new MessageChannel();
  const host = new BrowserClientBindingHost([...source.natives][0]!, channel.port2);
  const replies: ClientBindingEvent[] = [];
  channel.port1.addEventListener("message", (event) => replies.push(event.data));
  channel.port1.start();
  try {
    channel.port1.postMessage({
      version: 1,
      type: "client-call",
      id: 1,
      call: {
        method: "query",
        args: [
          translateQuery(app.documents._build(), app.documents._schema),
          undefined,
          "local",
          JSON.stringify({ transaction_id: transaction.openTransactionId() }),
        ],
      },
    } satisfies ClientBindingRequest);
    channel.port1.postMessage({
      version: 1,
      type: "client-call",
      id: 2,
      call: {
        method: "commitTransaction",
        args: [transaction.openTransactionId()],
      },
    } satisfies ClientBindingRequest);
    await expect.poll(() => replies.length).toBe(2);
    expect(replies.map((reply) => reply.error?.message)).toEqual([
      "Transaction does not belong to this port or is no longer open",
      "Transaction does not belong to this port or is no longer open",
    ]);
    await transaction.commit().wait({ tier: "local" });
    expect(await db.all(app.documents, { tier: "local" })).toEqual([row]);
  } finally {
    await host.close();
    channel.port1.close();
    channel.port2.close();
  }
});

it("does not commit earlier staged changes after a later staged operation fails", async () => {
  const transaction = db.beginTransaction();
  transaction.insert(app.documents, { rank: 1, payload: new Uint8Array([1]) });
  // Inject an admission failure at the real native boundary. Updating an
  // absent row is valid in Jazz and would not exercise the error path.
  vi.spyOn([...source.natives][0]!, "update").mockImplementationOnce(() => {
    throw new Error("injected staging failure");
  });
  transaction.update(app.documents, crypto.randomUUID(), { label: "missing" });
  await expect(transaction.commit().wait({ tier: "local" })).rejects.toThrow();
  expect(await db.all(app.documents, { tier: "local" })).toEqual([]);
});

it("streams a large value across the port without collecting it in the binding", async () => {
  const chunks = [new Uint8Array(70_000).fill(6), new Uint8Array([1, 2, 3])];
  let produced = 0;
  const payload = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        if (produced === chunks.length) controller.close();
        else controller.enqueue(chunks[produced++]!);
      },
    },
    { highWaterMark: 0 },
  );
  const inserted = await db.insertStreaming(app.documents, { label: "streamed", rank: 1, payload });
  await inserted.wait({ tier: "local" });
  const result = await db.all(
    app.documents
      .where({ id: inserted.value.id })
      .select({ payload: { from: 69_999, to: 70_003 } }),
    { tier: "local" },
  );
  expect(result).toEqual([{ id: inserted.value.id, payload: new Uint8Array([6, 1, 2, 3]) }]);
  expect(produced).toBe(2);
});

it("preserves non-enumerable row metadata across structured clone", async () => {
  const inserted = db.insert(app.documents, { rank: 1, payload: new Uint8Array([5]) });
  await inserted.wait({ tier: "local" });
  const query = translateQuery(app.documents._build(), app.documents._schema);
  const original = await [...source.natives][0]!.query(query, undefined, "local");
  const copied = await source.clients[0]!.query(query, undefined, "local");
  expect(Array.isArray(original) && Array.isArray(copied)).toBe(true);
  const before = (original as Array<{ valuesByColumn?: Map<string, unknown> }>)[0]!;
  const after = (copied as Array<{ valuesByColumn?: Map<string, unknown> }>)[0]!;
  expect(before.valuesByColumn).toBeInstanceOf(Map);
  expect(after.valuesByColumn).toEqual(before.valuesByColumn);
  expect(Object.keys(after)).not.toContain("valuesByColumn");
});

it("revokes a failed port without replaying a write whose response is uncertain", async () => {
  const row = db.insert(app.documents, { label: "once", rank: 1, payload: new Uint8Array([1]) });
  const client = source.clients[0]!;
  const failure = new Error("lost worker connection; write outcome unknown");
  // Failure can happen before or after physical admission. The application
  // receipt must reject, and must never be retried in either case.
  client.fail(failure);
  await expect(row.wait({ tier: "local" })).rejects.toThrow("write outcome unknown");
  const sibling = await createAccountDbWithRuntimeSource(config, source);
  dbs.push(sibling);
  const rows = await sibling.all(app.documents, { tier: "local" });
  expect(rows.length).toBeLessThanOrEqual(1);
  if (rows.length) expect(rows).toEqual([row.value]);
});
