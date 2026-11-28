import { expect, it, vi } from "vitest";
import { Encryption, EncryptionError, type EncryptionNative } from "./index.js";

/** A native layer whose verification never answers: the server is out of reach. */
function waitingNative(status: string | null): EncryptionNative {
  return {
    takeEncryptionReports: () => [],
    encryptionState: () => JSON.stringify({ status, error: null, stage: "verify" }),
    encryptionCall: (op: string) => {
      expect(op).toBe("ready");
      return new Promise<string>(() => {});
    },
  };
}

async function closedPromptly(encryption: Encryption) {
  const waiting = encryption.ready({ timeoutMs: 60_000 }).catch((error: unknown) => error);
  await new Promise((resolve) => setTimeout(resolve, 20));
  const closing = Date.now();
  encryption.stop();
  const error = await waiting;
  expect(Date.now() - closing).toBeLessThan(1_000);
  expect(error).toBeInstanceOf(EncryptionError);
  expect((error as EncryptionError).code).toBe("closed");
}

it("ends a native host's wait for readiness when the database closes", async () => {
  await closedPromptly(new Encryption(waitingNative(null), { nativeReadiness: true }));
});

it("ends a verified device's wait for readiness when the database closes", async () => {
  await closedPromptly(new Encryption(waitingNative("active")));
});

it("unsubscribes from host state notifications and wakes readability waiters on close", async () => {
  const unsubscribe = vi.fn();
  const encryption = new Encryption(waitingNative(null), { stateChanges: () => unsubscribe });
  const readable = encryption.readable();

  encryption.stop();

  await expect(readable).rejects.toMatchObject({ code: "closed" });
  expect(unsubscribe).toHaveBeenCalledOnce();
});
