import { describe, it, expect } from "vitest";
import { generateAuthSecret } from "./auth-secret-store.js";
import { formatAuthSecret } from "./auth-secret-codec.js";
import { RecoveryPhrase } from "./recovery-phrase.js";
import { createDb } from "./testing/create-internal-db.js";

describe("RecoveryPhrase integration — device keys", () => {
  it("round-trips a device key through backup + restore", () => {
    const original = generateAuthSecret();
    const restored = RecoveryPhrase.toSecret(RecoveryPhrase.fromSecret(original));
    expect(restored).toBe(original);
    const other = formatAuthSecret(new Uint8Array(32).fill(7));
    expect(RecoveryPhrase.fromSecret(other)).not.toBe(RecoveryPhrase.fromSecret(original));
  });
});

describe("RecoveryPhrase integration — identity continuity", () => {
  it("two createDb calls with secrets derived from the same phrase yield the same user_id", async () => {
    const originalSecret = generateAuthSecret();

    const phrase = RecoveryPhrase.fromSecret(originalSecret);
    const restoredSecret = RecoveryPhrase.toSecret(phrase);

    const dbA = await createDb({
      appId: "recovery-phrase-test",
      secret: originalSecret,
    });
    const dbB = await createDb({
      appId: "recovery-phrase-test",
      secret: restoredSecret,
    });

    try {
      const idA = dbA.getAuthState().session?.identity ?? null;
      const idB = dbB.getAuthState().session?.identity ?? null;
      expect(idA).not.toBeNull();
      expect(idB).toStrictEqual(idA);
    } finally {
      await dbA.shutdown();
      await dbB.shutdown();
    }
  });
});
