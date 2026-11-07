import { expect, it } from "vitest";
import { NativeForegroundDb } from "./native-foreground-db.js";

it("cancels a pending foreground read only once", () => {
  const commands: unknown[] = [];
  const db = new NativeForegroundDb(
    { execute: (bytes: Uint8Array) => bytes, tick: () => {}, close: () => true },
    {
      encodeNativeForegroundCommand(command: { type: string } | "close") {
        commands.push(command);
        if (command === "close") return Uint8Array.of(3);
        return Uint8Array.of(command.type === "all" ? 1 : 2);
      },
      decodeNativeForegroundResponse(bytes: Uint8Array) {
        if (bytes[0] === 3) return { type: "closed", closed: true };
        return bytes[0] === 1
          ? { type: "pending", operation: 11 }
          : { type: "cancelled", cancelled: true };
      },
    } as never,
  );
  const read = db.all(Uint8Array.of(1), { tier: "local-first" });
  expect("cancel" in read).toBe(true);
  if (!("cancel" in read) || typeof read.cancel !== "function")
    throw new Error("Pending reads must expose cancellation");
  read.cancel();
  read.cancel();
  expect(commands.slice(1)).toEqual([{ type: "cancel", operation: 11 }]);
  db.close();
});

it("exposes a pending waitForPendingWrites as a cancellable read", () => {
  const commands: unknown[] = [];
  const db = new NativeForegroundDb(
    { execute: (bytes: Uint8Array) => bytes, tick: () => {}, close: () => true },
    {
      encodeNativeForegroundCommand(command: { type: string } | "close") {
        commands.push(command);
        if (command === "close") return Uint8Array.of(3);
        if (command.type === "cancel") return Uint8Array.of(2);
        return Uint8Array.of(1);
      },
      decodeNativeForegroundResponse(bytes: Uint8Array) {
        if (bytes[0] === 3) return { type: "closed", closed: true };
        return bytes[0] === 1
          ? { type: "pending", operation: 12 }
          : { type: "cancelled", cancelled: true };
      },
    } as never,
  );
  const wait = db.waitForPendingWrites("global");
  if (wait instanceof Uint8Array) throw new Error("A pending wait must be pollable");
  expect(wait.poll()).toBeNull();
  wait.cancel();
  wait.cancel();
  expect(commands.slice(1)).toEqual([
    { type: "poll", operation: 12 },
    { type: "cancel", operation: 12 },
  ]);
  db.close();
});

it("disables the pending-upload count only when the host rejects the command", () => {
  let failure = new Error("Jazz native foreground runtime is busy during execute; retry");
  const db = new NativeForegroundDb(
    {
      execute: () => {
        throw failure;
      },
      tick: () => {},
      close: () => true,
    },
    {
      encodeNativeForegroundCommand: () => Uint8Array.of(0),
      decodeNativeForegroundResponse: () => ({ type: "pendingUploadCount", count: 0 }),
    } as never,
  );
  expect(db.pendingUploadCount()).toBe(0);
  expect(
    (db as unknown as { pendingUploadCountUnsupported: boolean }).pendingUploadCountUnsupported,
  ).toBe(false);
  failure = new Error("Jazz native foreground runtime failed during execute");
  expect(db.pendingUploadCount()).toBe(0);
  expect(
    (db as unknown as { pendingUploadCountUnsupported: boolean }).pendingUploadCountUnsupported,
  ).toBe(true);
});
