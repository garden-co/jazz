import { expect, it, vi } from "vitest";
import { RemoteDurableTransaction } from "./durable-initialization-remote.js";
import type { BrowserInitializationCommand } from "./browser-worker-protocol.js";
import type { OpenTransactionId } from "../client.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
function fixture() {
  const commands: BrowserInitializationCommand[] = [];
  const admission = deferred<unknown>();
  let current = true;
  const request = vi.fn(async (command: BrowserInitializationCommand): Promise<unknown> => {
    commands.push(command);
    if (command.operation === "begin") return admission.promise;
    if (command.operation === "seal") return "original-reservation";
    if (command.operation === "publish" || command.operation === "commit")
      return "actual-original-tx";
    return [];
  });
  const id = crypto.randomUUID() as OpenTransactionId;
  const remote = new RemoteDurableTransaction(
    id,
    {
      operation: "begin",
      openTransactionId: id,
      schema: {
        notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
      },
      readCapability: "client-accepted-evidence",
    },
    request,
    () => {
      if (!current) throw new Error("owner changed");
    },
  );
  return {
    remote,
    request,
    commands,
    admission,
    retire: () => {
      current = false;
    },
  };
}

it("awaits real before-begin admission and copies queued mutation inputs", async () => {
  const f = fixture();
  const values = { bytes: { type: "Bytea" as const, value: new Uint8Array([1, 2]) } };
  f.remote.enqueue({
    operation: "insert",
    table: "notes",
    rowId: "stable-id",
    values,
    context: "exact-context",
  });
  values.bytes.value[0] = 9;
  await Promise.resolve();
  expect(f.commands.map((c) => c.operation)).toEqual(["begin"]);
  f.admission.resolve("private-handle");
  const reading = f.remote.run({ operation: "query", query: "query", options: "options" });
  await reading;
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "insert", "query"]);
  expect(f.commands[1]).toMatchObject({
    handle: "private-handle",
    rowId: "stable-id",
    values: { bytes: { type: "Bytea", value: new Uint8Array([1, 2]) } },
  });
  await f.remote.cancel();
});

it("cancels an opening on its actual returned handle without staging later commands", async () => {
  const f = fixture();
  const cancelled = f.remote.cancel();
  f.admission.resolve("original-handle");
  await cancelled;
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "cancel"]);
  expect(f.commands[1]).toMatchObject({ handle: "original-handle" });
  expect(() =>
    f.remote.enqueue({ operation: "prepare-insert", table: "notes", rowId: "id" }),
  ).toThrow(/no longer/);
});

it("cancels a pending request without waiting behind it", async () => {
  const f = fixture();
  f.admission.resolve("handle");
  await f.remote.ready;
  const release = deferred<unknown>();
  f.request.mockImplementation(async (command) => {
    f.commands.push(command);
    return command.operation === "query" ? release.promise : undefined;
  });
  const reading = f.remote
    .run({ operation: "query", query: "query", options: "options" })
    .catch((error) => error);
  await vi.waitFor(() => expect(f.commands.at(-1)?.operation).toBe("query"));
  await f.remote.cancel();
  expect(f.commands.at(-1)?.operation).toBe("cancel");
  release.resolve(["late rows"]);
  expect(await reading).toBeInstanceOf(Error);
});

it("never publishes after a failed queued prefix, including thrown undefined", async () => {
  const f = fixture();
  f.admission.resolve("handle");
  await f.remote.ready;
  f.request.mockImplementation(async (command) => {
    f.commands.push(command);
    if (command.operation === "insert") throw undefined;
    return "unexpected";
  });
  f.remote.enqueue({
    operation: "insert",
    table: "notes",
    rowId: "id",
    values: {},
    context: "context",
  });
  await expect(f.remote.seal()).rejects.toBeUndefined();
  expect(f.commands.map((c) => c.operation)).toEqual(["begin", "insert"]);
  await f.remote.cancel();
});

it("carries only an actual live caller wait into publication and never transfers a native token", async () => {
  const f = fixture();
  f.admission.resolve("handle");
  await f.remote.ready;
  const live = new AbortController();
  const expired = new AbortController();
  const liveId = crypto.randomUUID();
  const expiredId = crypto.randomUUID();
  expect(f.remote.registerWait(liveId, "global", live.signal)).toBe(true);
  expect(f.remote.registerWait(expiredId, "global", expired.signal)).toBe(true);
  expired.abort();
  const seal = await f.remote.seal();
  expect(Object.keys(seal)).toEqual(["reservedTxId"]);
  expect(await f.remote.publish()).toBe("actual-original-tx");
  expect(f.commands.at(-1)).toEqual({
    operation: "publish",
    handle: "handle",
    waitIntents: [{ waitId: liveId, tier: "global" }],
  });
  expect(f.remote.waitWasSubmitted(liveId)).toBe(true);
  expect(f.remote.waitWasSubmitted(expiredId)).toBe(false);
});

it("refuses stale-owner results and a second terminal operation", async () => {
  const f = fixture();
  f.admission.resolve("handle");
  await f.remote.ready;
  f.retire();
  await expect(f.remote.commit()).rejects.toThrow("owner changed");
  expect(f.commands.map((c) => c.operation)).toEqual(["begin"]);
  await f.remote.cancel();
});

it("can clean up the original worker batch after publication is refused before commit", async () => {
  const f = fixture();
  f.admission.resolve("original-handle");
  await f.remote.ready;
  f.request.mockImplementation(async (command) => {
    f.commands.push(command);
    if (command.operation === "commit") throw new Error("worker refused before native commit");
    return undefined;
  });
  await expect(f.remote.commit()).rejects.toThrow("worker refused before native commit");
  await f.remote.cancel();
  expect(f.commands.at(-1)).toEqual({ operation: "cancel", handle: "original-handle" });
});
