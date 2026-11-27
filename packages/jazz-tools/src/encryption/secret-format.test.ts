import { expect, it } from "vitest";
import { Encryption, type EncryptionNative } from "./index.js";
import { formatSecret } from "../local-first-auth/secret.js";
import {
  explainingSecretFormat,
  SECRET_FORMAT_MISMATCH,
  secretFormatExplained,
} from "./secret-format.js";

// A native build older than this package takes the device account secret as
// 64 hexadecimal characters and refuses `jazz-auth-v1:` text: the refusal
// must say that the versions differ, not only that the secret is invalid.

const olderBuild = new Error("a recovery secret is 64 hexadecimal characters");

it("explains an older build's refusal and passes other errors through", () => {
  const explained = secretFormatExplained(olderBuild) as Error;
  expect(explained.message).toBe(SECRET_FORMAT_MISMATCH);
  expect(explained.message).toContain("jazz-auth-v1:");
  expect(explained.message).toContain("matching versions");
  expect(explained.cause).toBe(olderBuild);
  expect(secretFormatExplained(new Error("a recovery secret is hexadecimal"))).not.toBe(olderBuild);
  // That message needs 64 characters, which `jazz-auth-v1:` text never has.
  const hex = new Error("a recovery secret is hexadecimal");
  expect(secretFormatExplained(hex)).toBe(hex);

  const other = new Error("invalid device account secret");
  expect(secretFormatExplained(other)).toBe(other);
  expect(() =>
    explainingSecretFormat(() => {
      throw olderBuild;
    }),
  ).toThrow(SECRET_FORMAT_MISMATCH);
  expect(explainingSecretFormat(() => 7)).toBe(7);
});

it("gives the automatic recovery refused by an older build its own code", async () => {
  const native = {
    encryptionCall: async (op: string) => {
      if (op === "ready") return JSON.stringify("awaiting-approval");
      if (op === "recoveryRestore") throw olderBuild;
      if (op === "reportsForListener") return JSON.stringify({ active: [], queued: [] });
      throw new Error(`unexpected op ${op}`);
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "awaiting-approval" }),
  } as unknown as EncryptionNative;
  const encryption = new Encryption(native, {
    accountSecret: () => formatSecret(new Uint8Array(32).fill(7)),
  });
  await expect(encryption.ready()).rejects.toMatchObject({
    code: "secret-format-mismatch",
    message: SECRET_FORMAT_MISMATCH,
  });
});

it("leaves the same text from other calls as it is", async () => {
  const native = {
    encryptionCall: async () => {
      throw olderBuild;
    },
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status: "active" }),
  } as unknown as EncryptionNative;
  await expect(new Encryption(native).devices.list()).rejects.toMatchObject({
    code: "error",
    message: olderBuild.message,
  });
});
