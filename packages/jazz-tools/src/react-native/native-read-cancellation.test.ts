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
  const read = db.all(Uint8Array.of(1), { tier: "local" });
  expect("cancel" in read).toBe(true);
  if (!("cancel" in read) || typeof read.cancel !== "function")
    throw new Error("Pending reads must expose cancellation");
  read.cancel();
  read.cancel();
  expect(commands.slice(1)).toEqual([{ type: "cancel", operation: 11 }]);
  db.close();
});
