import { expect, it, vi } from "vitest";
import type { NativeForegroundCommand, NativeForegroundResponse } from "jazz-rn/relay";
import { servePrivateStore, type PrivateStore } from "../encryption/private-store.js";
import { NativeForegroundDb } from "./native-foreground-db.js";

function fixture(
  respond: (command: NativeForegroundCommand) => NativeForegroundResponse,
  tick: () => void = () => {},
  isClosed: () => boolean = () => false,
) {
  const commands: NativeForegroundCommand[] = [];
  let response: NativeForegroundResponse;
  const db = new NativeForegroundDb(
    {
      execute(bytes) {
        response = respond(commands[bytes[0]!]!);
        return bytes;
      },
      tick,
      isClosed,
      close: () => true,
    },
    {
      encodeNativeForegroundCommand(command) {
        const at = commands.length;
        commands.push(command);
        return Uint8Array.of(at);
      },
      decodeNativeForegroundResponse: () => response,
      installNativeForegroundRuntime() {
        throw new Error("fixture uses an already open foreground");
      },
    },
  );
  return { db, commands };
}

it.each(["answer", "recoveryRestore", "keysRotate"])(
  "cancels a pending %s call if its own poll throws and preserves the first error",
  async (op) => {
    const failure = new Error("poll failed");
    const { db, commands } = fixture((command) => {
      if (typeof command === "object" && command.type === "encryptionCall") {
        return { type: "pending", operation: 11 };
      }
      if (typeof command === "object" && command.type === "poll") throw failure;
      throw new Error("cancel also failed");
    });

    await expect(db.encryptionCall(op)).rejects.toBe(failure);
    expect(commands.slice(1)).toEqual([
      { type: "poll", operation: 11 },
      { type: "cancel", operation: 11 },
    ]);
  },
);

it.each(["answer", "recoveryRestore"])(
  "keeps a pending %s call to its own result when an unrelated tick throws",
  async (op) => {
    const failure = new Error("core tick failed");
    const tick = vi.fn<() => void>().mockImplementationOnce(() => {
      throw failure;
    });
    let polls = 0;
    const { db, commands } = fixture((command) => {
      if (typeof command === "object" && command.type === "encryptionCall") {
        return { type: "pending", operation: 14 };
      }
      if (typeof command === "object" && command.type === "poll") {
        polls += 1;
        return polls < 3
          ? { type: "pending", operation: 14 }
          : { type: "encryptionResult", json: '"done"' };
      }
      throw new Error(`unexpected command ${JSON.stringify(command)}`);
    }, tick);

    await expect(db.encryptionCall(op)).resolves.toBe('"done"');
    expect(commands.slice(1)).toEqual([
      { type: "poll", operation: 14 },
      { type: "poll", operation: 14 },
      { type: "poll", operation: 14 },
    ]);
    // The core kept being driven, and its failure reaches the core's own
    // driver once, at its next tick, instead of being lost.
    expect(tick).toHaveBeenCalledTimes(3);
    expect(() => db.tick()).toThrow(failure);
    expect(tick).toHaveBeenCalledTimes(3);
    db.tick();
    expect(tick).toHaveBeenCalledTimes(4);
  },
);

it.each([
  ["close", false],
  ["stopping the private store then close", true],
] as const)("%s rejects a pending answer and sends no further poll", async (_name, stopStore) => {
  let polledAfterClose = false;
  let closed = false;
  const storeRequests = [JSON.stringify({ id: "r1", op: "list", scope: "device" })];
  const { db, commands } = fixture((command) => {
    if (command === "close") {
      closed = true;
      return { type: "closed", closed: true };
    }
    if (typeof command !== "object") throw new Error(`unexpected command ${command}`);
    if (command.type === "encryptionCall") return { type: "pending", operation: 15 };
    if (command.type === "poll") {
      if (closed) polledAfterClose = true;
      return { type: "pending", operation: 15 };
    }
    if (command.type === "takeStoreRequests") {
      return { type: "storeRequests", requests: storeRequests.splice(0) };
    }
    throw new Error(`unexpected command ${JSON.stringify(command)}`);
  });
  // A store whose answer never arrives: the shutdown path must not wait on it.
  const listed = vi.fn(() => new Promise<string[]>(() => {}));
  const serving = stopStore
    ? servePrivateStore(db, { list: listed } as unknown as PrivateStore, 1)
    : undefined;

  const call = db.encryptionCall("answer");
  const outcome = call.then(
    () => "resolved",
    (error: Error) => error.message,
  );
  await vi.waitFor(() => {
    expect(
      commands.filter((c) => typeof c === "object" && c.type === "poll").length,
    ).toBeGreaterThanOrEqual(2);
    if (serving) expect(listed).toHaveBeenCalled();
  });
  serving?.stop({ cancelPending: true });
  expect(db.close()).toBe(true);

  await expect(outcome).resolves.toBe("React Native native foreground runtime is closed");
  expect(polledAfterClose).toBe(false);
  // Closing ends the operation natively; nothing else is sent for it.
  expect(commands.filter((c) => typeof c === "object" && c.type === "cancel")).toEqual([]);
  expect(commands.filter((c) => typeof c === "object" && c.type === "replyStore")).toEqual([]);
});

it("skips cancelling after the native foreground has closed", async () => {
  const failure = new Error("foreground closed during poll");
  const { db, commands } = fixture(
    (command) => {
      if (typeof command === "object" && command.type === "poll") throw failure;
      return { type: "pending", operation: 12 };
    },
    () => {},
    () => true,
  );

  await expect(db.encryptionCall("keysRotate")).rejects.toBe(failure);
  expect(commands.slice(1)).toEqual([{ type: "poll", operation: 12 }]);
});

it("skips cancelling when polling returned a terminal operation error", async () => {
  const { db, commands } = fixture((command) =>
    typeof command === "object" && command.type === "encryptionCall"
      ? { type: "pending", operation: 12 }
      : { type: "operationError", reason: "store: keychain unavailable" },
  );

  await expect(db.encryptionCall("keysRotate")).rejects.toThrow("store: keychain unavailable");
  expect(commands.slice(1)).toEqual([{ type: "poll", operation: 12 }]);
});

it("polls a pending readiness call without a synchronous tick", async () => {
  const tick = vi.fn(() => {
    throw new Error("relay owner is busy");
  });
  const { db, commands } = fixture(
    (command) =>
      typeof command === "object" && command.type === "encryptionCall"
        ? { type: "pending", operation: 13 }
        : { type: "encryptionResult", json: '"active"' },
    tick,
  );

  await expect(db.encryptionCall("ready")).resolves.toBe('"active"');
  expect(tick).not.toHaveBeenCalled();
  expect(commands.slice(1)).toEqual([{ type: "poll", operation: 13 }]);
});
