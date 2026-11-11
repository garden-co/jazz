import { expect, it, vi } from "vitest";
import { JazzClient, createOpenTransactionId } from "../client.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import type { WasmSchema } from "../../drivers/types.js";
import type { BrowserInitializationCommand } from "./browser-worker-protocol.js";

const schema = {
  notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
} satisfies WasmSchema;
const stableId = "00000000-0000-0000-0000-000000000001";
const text = { text: { type: "Text", value: "founder" } } as const;
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
function fixture() {
  const raw = {
    setTickScheduler: vi.fn(),
    onMutationError: vi.fn(),
    setNonDurableClient: vi.fn(),
    beginTransaction: vi.fn(),
    beginClientEvidenceTransaction: vi.fn(),
    rollbackTransaction: vi.fn(),
    insertInTransaction: vi.fn(),
  };
  const runtime = new NativeRuntimeAdapter(
    { openMemory: () => raw } as never,
    schema,
    new Uint8Array(16),
    new TextEncoder().encode('["https://issuer.example","alice"]'),
    1,
    false,
  );
  runtime.setNonDurableClient();
  const commands: BrowserInitializationCommand[] = [];
  const opened = deferred<unknown>();
  const request = vi.fn(async (command: BrowserInitializationCommand): Promise<unknown> => {
    commands.push(command);
    if (command.operation === "begin") return opened.promise;
    if (command.operation === "seal") return "reservation";
    if (command.operation === "commit" || command.operation === "publish")
      return "actual-original-tx";
    if (command.operation === "query") return [{ id: stableId, values: [text.text] }];
    return undefined;
  });
  const detach = runtime.setInitializationCommandOwner(request);
  const client = JazzClient.connectWithRuntime(runtime, { appId: "durable-route", schema });
  const begin = () =>
    client.openTransaction(
      "serializable",
      undefined,
      undefined,
      undefined,
      undefined,
      "client-accepted-evidence",
    );
  return { runtime, raw, commands, opened, request, detach, client, begin };
}

it("opens and executes on the worker from before the first callback operation", async () => {
  const f = fixture();
  const id = f.begin();
  const preparation = f.client.prepareTransaction(id, async (io) => {
    io.insertInternal("notes", text, { id: stableId });
    expect(await io.queryInternal('{"table":"notes"}', { tier: "local-only" })).toEqual([
      { id: stableId, values: [text.text] },
    ]);
  });
  await vi.waitFor(() => expect(f.commands.map((c) => c.operation)).toEqual(["begin"]));
  expect(f.raw.beginTransaction).not.toHaveBeenCalled();
  expect(f.raw.beginClientEvidenceTransaction).not.toHaveBeenCalled();
  expect(f.raw.insertInTransaction).not.toHaveBeenCalled();
  f.opened.resolve("owner-handle");
  await preparation;
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "insert", "query"]);
  const mutation = f.commands[1];
  const query = f.commands[2];
  if (mutation?.operation !== "insert" || query?.operation !== "query")
    throw new Error("missing bound operations");
  expect(mutation.rowId).toBe(stableId);
  expect(JSON.parse(mutation.context).transaction_id).toBe(id);
  expect(JSON.parse(query.options).transaction_id).toBe(id);
  expect(mutation.handle).toBe("owner-handle");
  expect(query.handle).toBe("owner-handle");
  await f.client.rollbackTransaction(id);
});

it("rolls back the actual opening worker batch while its admission reply is pending", async () => {
  const f = fixture();
  const id = f.begin();
  const cancelled = f.client.rollbackTransaction(id);
  f.opened.resolve("original-owner-handle");
  expect(await cancelled).toBe(true);
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "cancel"]);
  expect(f.commands[1]).toMatchObject({ handle: "original-owner-handle" });
  expect(f.raw.rollbackTransaction).not.toHaveBeenCalled();
});

it("keeps ordinary no-capability transactions on their original offline foreground route", async () => {
  const f = fixture();
  const id = f.client.beginTransaction("serializable");
  expect(f.raw.beginTransaction).toHaveBeenCalledWith(id, "serializable", undefined, undefined);
  expect(f.commands).toEqual([]);
  await f.client.rollbackTransaction(id);
  expect(f.raw.rollbackTransaction).toHaveBeenCalledExactlyOnceWith(id);
});

it("carries an immediate public wait into the real worker commit and wait", async () => {
  const f = fixture();
  const id = f.begin();
  const handle = f.client.commitTransaction(id);
  const waiting = handle.wait({ tier: "global", timeoutMs: 1000 });
  f.opened.resolve("owner-handle");
  await waiting;
  const commit = f.commands.find((c) => c.operation === "commit");
  const wait = f.commands.find((c) => c.operation === "wait");
  if (commit?.operation !== "commit" || wait?.operation !== "wait")
    throw new Error("missing actual owner wait");
  expect(commit.waitIntents).toEqual([{ waitId: wait.waitId, tier: "global" }]);
  expect(wait.transactionId).toBe("actual-original-tx");
  expect(f.raw.beginTransaction).not.toHaveBeenCalled();
});

it("refuses further staged operations when the actual follower connection is detached", async () => {
  const f = fixture();
  const id = f.begin();
  f.opened.resolve("handle");
  await f.client.transactionPreparation(id);
  f.detach();
  expect(() =>
    f.runtime.insert("notes", text, JSON.stringify({ transaction_id: id }), stableId),
  ).toThrow(/retired/);
  await f.runtime.rollbackTransaction(id);
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "cancel"]);
});

it("requires the genuine worker-open admission provider for a non-durable foreground", async () => {
  const f = fixture();
  await expect(f.runtime.waitForStorageOwner()).rejects.toThrow(/No admitted durable worker/);
  const ready = deferred<void>();
  const release = f.runtime.setStorageOwnerAdmission(() => ready.promise);
  const admitted = f.runtime.waitForStorageOwner();
  release();
  ready.resolve();
  await expect(admitted).rejects.toThrow(/retired/);
});
