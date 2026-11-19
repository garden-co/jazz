import { expect, it, vi } from "vitest";
import type { NativeForegroundCommand, NativeForegroundResponse } from "jazz-rn/relay";
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

it.each(["tick", "poll"] as const)(
  "cancels an admitted encryption call if %s throws and preserves the first error",
  async (failedStep) => {
    const failure = new Error(`${failedStep} failed`);
    const { db, commands } = fixture(
      (command) => {
        if (typeof command === "object" && command.type === "encryptionCall") {
          return { type: "pending", operation: 11 };
        }
        if (typeof command === "object" && command.type === "poll") throw failure;
        throw new Error("cancel also failed");
      },
      () => {
        if (failedStep === "tick") throw failure;
      },
    );

    await expect(db.encryptionCall("keysRotate")).rejects.toBe(failure);
    expect(commands.slice(1)).toEqual([
      ...(failedStep === "poll" ? [{ type: "poll", operation: 11 }] : []),
      { type: "cancel", operation: 11 },
    ]);
  },
);

it("skips cancelling after the native foreground has closed", async () => {
  const failure = new Error("foreground closed during tick");
  const { db, commands } = fixture(
    () => ({ type: "pending", operation: 12 }),
    () => {
      throw failure;
    },
    () => true,
  );

  await expect(db.encryptionCall("keysRotate")).rejects.toBe(failure);
  expect(commands).toHaveLength(1);
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
