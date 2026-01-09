import { assert, describe, expect, it } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto";
import { LocalNode } from "../exports";
import { agentAndSessionIDFromSecret } from "./testUtils";
import { Transaction } from "../coValueCore/verifiedState";

const wasmCrypto = await WasmCrypto.create();

const agentSecret =
  "sealerSecret_zE3Nr7YFr1KkVbJSx4JDCzYn4ApYdm8kJ5ghNBxREHQya/signerSecret_z9fEu4eNG1eXHMak3YSzY7uLdoG8HESSJ8YW4xWdNNDSP";

function createTestNode() {
  const [agent, session] = agentAndSessionIDFromSecret(agentSecret);
  return {
    agent,
    session,
    node: new LocalNode(agent.agentSecret, session, wasmCrypto),
  };
}

describe("SessionLog WASM", () => {
  it("it works", () => {
    const [agent, sessionId] = agentAndSessionIDFromSecret(agentSecret);

    const session = wasmCrypto.createSessionLog(
      "co_test1" as any,
      sessionId,
      agent.currentSignerID(),
    );

    expect(session).toBeDefined();
  });

  it("test_add_from_example_json", () => {
    const { agent, session, node } = createTestNode();

    const group = node.createGroup();
    const sessionContent =
      group.core.newContentSince(undefined)?.[0]?.new[session];
    assert(sessionContent);

    let log = wasmCrypto.createSessionLog(
      group.id,
      session,
      agent.currentSignerID(),
    );

    log.tryAdd(
      sessionContent.newTransactions,
      sessionContent.lastSignature,
      false,
    );
  });

  it("test_add_new_transaction", () => {
    const { agent, session, node } = createTestNode();

    const group = node.createGroup();
    const sessionContent =
      group.core.newContentSince(undefined)?.[0]?.new[session];
    assert(sessionContent);

    let log = wasmCrypto.createSessionLog(
      group.id,
      session,
      agent.currentSignerID(),
    );

    const changesJson = [
      { after: "start", op: "app", value: "co_zMphsnYN6GU8nn2HDY5suvyGufY" },
    ];
    const key = group.getCurrentReadKey();
    assert(key);
    assert(key.secret);

    const { signature, transaction } = log.addNewPrivateTransaction(
      agent,
      changesJson,
      key.id,
      key.secret,
      0,
      undefined,
    );

    expect(signature).toMatch(/^signature_z[a-zA-Z0-9]+$/);
    expect(transaction).toEqual({
      encryptedChanges: expect.stringMatching(/^encrypted_U/),
      keyUsed: expect.stringMatching(/^key_z/),
      madeAt: 0,
      privacy: "private",
    });

    const decrypted = log.decryptNextTransactionChangesJson(0, key.secret);

    expect(decrypted).toEqual(
      '[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]',
    );
  });

  it("test_decrypt + clone", () => {
    const [agent] = agentAndSessionIDFromSecret(agentSecret);
    const fixtures = {
      id: "co_zWwrEiushQLvbkWd6Z3L8WxTU1r",
      signature:
        "signature_z3ktW7wxMnW7VYExCGZv4Ug2UJSW3ag6zLDiP8GpZThzif6veJt7JipYpUgshhuGbgHtLcWywWSWysV7hChxFypDt",
      decrypted:
        '[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]',
      key: {
        secret: "keySecret_z3dU66SsyQkkGKpNCJW6NX74MnfVGHUyY7r85b4M8X88L",
        id: "key_z5XUAHyoqUV9zXWvMK",
      },
      transaction: {
        privacy: "private",
        madeAt: 0,
        encryptedChanges:
          "encrypted_UNAxqdUSGRZ2rzuLU99AFPKCe2C0HwsTzMWQreXZqLr6RpWrSMa-5lwgwIev7xPHTgZFq5UyUgMFrO9zlHJHJGgjJcDzFihY=" as any,
        keyUsed: "key_z5XUAHyoqUV9zXWvMK",
      },
      session:
        "sealer_z5yhsCCe2XwLTZC4254mUoMASshm3Diq49JrefPpjTktp/signer_z7gVGDpNz9qUtsRxAkHMuu4DYdtVVCG4XELTKPYdoYLPr_session_z9mDP8FoonSA",
    } as const;

    let log = wasmCrypto.createSessionLog(
      fixtures.id,
      fixtures.session,
      agent.currentSignerID(),
    );

    log.tryAdd([fixtures.transaction], fixtures.signature, true);

    const decrypted = log
      .clone()
      .decryptNextTransactionChangesJson(0, fixtures.key.secret);

    expect(decrypted).toEqual(fixtures.decrypted);
  });

  function shuffleObjectKeys<T extends object>(obj: T): T {
    const keys = Object.keys(obj);
    // Fisher-Yates shuffle
    for (let i = keys.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [keys[i], keys[j]] = [keys[j]!, keys[i]!];
    }
    const result = {} as T;
    for (const key of keys) {
      (result as any)[key] = (obj as any)[key];
    }
    return result;
  }

  function shuffleTransactions(transactions: Transaction[]): Transaction[] {
    return transactions.map((t) => shuffleObjectKeys(t) as Transaction);
  }

  describe("Signature validation after shuffling transaction keys", () => {
    it("trusting transactions with 100 k/v entries", () => {
      const { agent, session, node } = createTestNode();

      const group = node.createGroup();
      const map = group.createMap();

      // Create 100 trusting transactions with explicit test data
      for (let i = 0; i < 100; i++) {
        map.core.makeTransaction(
          [{ op: "set", key: `key${i}`, value: `value${i}` }],
          "trusting",
          undefined,
          i * 1000,
        );
      }

      const sessionContent =
        map.core.newContentSince(undefined)?.[0]?.new[session];
      assert(sessionContent);
      expect(sessionContent.newTransactions.length).toBe(100);

      const log = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );
      const logShuffled = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );

      const shuffledTransactions = shuffleTransactions(
        sessionContent.newTransactions,
      );

      expect(() =>
        log.tryAdd(
          sessionContent.newTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();

      expect(() =>
        logShuffled.tryAdd(
          shuffledTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();
    });

    it("private transactions with 100 k/v entries", () => {
      const { agent, session, node } = createTestNode();

      const group = node.createGroup();
      const map = group.createMap();

      // Create 100 private transactions with explicit test data
      for (let i = 0; i < 100; i++) {
        map.core.makeTransaction(
          [{ op: "set", key: `secretKey${i}`, value: `secretValue${i}` }],
          "private",
          undefined,
          i * 1000,
        );
      }

      const sessionContent =
        map.core.newContentSince(undefined)?.[0]?.new[session];
      assert(sessionContent);
      expect(sessionContent.newTransactions.length).toBe(100);

      // Verify transactions are actually private (encrypted)
      const firstTx = sessionContent.newTransactions[0];
      assert(firstTx);
      expect(firstTx.privacy).toBe("private");
      expect("encryptedChanges" in firstTx).toBe(true);

      const log = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );
      const logShuffled = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );

      const shuffledTransactions = shuffleTransactions(
        sessionContent.newTransactions,
      );

      expect(() =>
        log.tryAdd(
          sessionContent.newTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();

      expect(() =>
        logShuffled.tryAdd(
          shuffledTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();
    });

    it("trusting transactions with metas", () => {
      const { agent, session, node } = createTestNode();

      const group = node.createGroup();
      const map = group.createMap();

      // Create transactions with metas
      for (let i = 0; i < 50; i++) {
        map.core.makeTransaction(
          [{ op: "set", key: `key${i}`, value: `value${i}` }],
          "trusting",
          { index: i, timestamp: i * 1000, nested: { data: `meta${i}` } },
          i * 1000,
        );
      }

      const sessionContent =
        map.core.newContentSince(undefined)?.[0]?.new[session];
      assert(sessionContent);
      expect(sessionContent.newTransactions.length).toBe(50);

      // Verify metas are present
      const firstTx = sessionContent.newTransactions[0];
      assert(firstTx);
      expect(firstTx.meta).toBeDefined();

      const log = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );
      const logShuffled = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );

      const shuffledTransactions = shuffleTransactions(
        sessionContent.newTransactions,
      );

      expect(() =>
        log.tryAdd(
          sessionContent.newTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();

      expect(() =>
        logShuffled.tryAdd(
          shuffledTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();
    });

    it("private transactions with metas", () => {
      const { agent, session, node } = createTestNode();

      const group = node.createGroup();
      const map = group.createMap();

      // Create private transactions with encrypted metas
      for (let i = 0; i < 50; i++) {
        map.core.makeTransaction(
          [{ op: "set", key: `secretKey${i}`, value: `secretValue${i}` }],
          "private",
          { index: i, secret: `confidential${i}` },
          i * 1000,
        );
      }

      const sessionContent =
        map.core.newContentSince(undefined)?.[0]?.new[session];
      assert(sessionContent);
      expect(sessionContent.newTransactions.length).toBe(50);

      // Verify transactions are private with encrypted metas
      const firstTx = sessionContent.newTransactions[0];
      assert(firstTx);
      expect(firstTx.privacy).toBe("private");
      expect("encryptedChanges" in firstTx).toBe(true);
      expect(firstTx.meta).toBeDefined();

      const log = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );
      const logShuffled = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );

      const shuffledTransactions = shuffleTransactions(
        sessionContent.newTransactions,
      );

      expect(() =>
        log.tryAdd(
          sessionContent.newTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();

      expect(() =>
        logShuffled.tryAdd(
          shuffledTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();
    });

    it("mixed trusting and private transactions across multiple sessions", () => {
      const { agent, session, node } = createTestNode();

      const group = node.createGroup();
      const map = group.createMap();

      // Create alternating trusting and private transactions
      for (let i = 0; i < 50; i++) {
        const privacy = i % 2 === 0 ? "trusting" : "private";
        const hasMeta = i % 3 === 0;
        map.core.makeTransaction(
          [{ op: "set", key: `mixedKey${i}`, value: `mixedValue${i}` }],
          privacy,
          hasMeta ? { iteration: i, type: privacy } : undefined,
          i * 1000,
        );
      }

      const sessionContent =
        map.core.newContentSince(undefined)?.[0]?.new[session];
      assert(sessionContent);
      expect(sessionContent.newTransactions.length).toBe(50);

      // Verify we have a mix of trusting and private transactions
      const trustingCount = sessionContent.newTransactions.filter(
        (t) => t.privacy === "trusting",
      ).length;
      const privateCount = sessionContent.newTransactions.filter(
        (t) => t.privacy === "private",
      ).length;
      expect(trustingCount).toBe(25);
      expect(privateCount).toBe(25);

      const log = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );
      const logShuffled = wasmCrypto.createSessionLog(
        map.id,
        session,
        agent.currentSignerID(),
      );

      const shuffledTransactions = shuffleTransactions(
        sessionContent.newTransactions,
      );

      expect(() =>
        log.tryAdd(
          sessionContent.newTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();

      expect(() =>
        logShuffled.tryAdd(
          shuffledTransactions,
          sessionContent.lastSignature,
          false,
        ),
      ).not.toThrow();
    });
  });
});
