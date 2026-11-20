import { expect, it } from "vitest";
import { Encryption, type EncryptionNative } from "./index.js";
import { formatSecret } from "../local-first-auth/secret.js";

// A device account awaiting approval recovers by itself with its account
// secret. The runtime answers `null` when there is nothing to recover from
// yet; every other failure must reach the caller.

const secret = new Uint8Array(32).fill(7);

function awaitingDevice(restore: () => Promise<string>) {
  const calls: { op: string; args: Record<string, unknown> }[] = [];
  const native = {
    encryptionCall: async (op: string, args: Record<string, unknown>) => {
      calls.push({ op, args });
      if (op === "ready") return JSON.stringify("awaiting-approval");
      if (op === "recoveryRestore") return restore();
      throw new Error(`unexpected op ${op}`);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "awaiting-approval" }),
  } as unknown as EncryptionNative;
  const encryption = new Encryption(native, { accountSecret: () => formatSecret(secret) });
  return { encryption, calls };
}

it("asks for the automatic recovery and stays awaiting approval when there is nothing to recover from", async () => {
  const { encryption, calls } = awaitingDevice(async () => "null");

  await expect(encryption.ready()).resolves.toBe("awaiting-approval");
  expect(calls[1]).toEqual({
    op: "recoveryRestore",
    args: { secret: "07".repeat(32), automatic: true },
  });
});

it("returns the status the automatic recovery reached", async () => {
  const { encryption } = awaitingDevice(async () => JSON.stringify("active"));

  await expect(encryption.ready()).resolves.toBe("active");
});

it.each([
  ["store", "store: keychain unavailable"],
  ["database", "database: the transaction failed"],
  ["protocol", "protocol: E2EE recovery failed"],
])("surfaces a %s failure of the automatic recovery", async (code, message) => {
  const { encryption } = awaitingDevice(async () => {
    throw new Error(message);
  });

  await expect(encryption.ready()).rejects.toMatchObject({
    name: "EncryptionError",
    code,
    message: message.slice(code.length + 2),
  });
});
