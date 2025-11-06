import { assert, describe, expect, it } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto";
import { JsonValue, LocalNode, SessionID } from "../exports";
import {
  agentAndSessionIDFromSecret,
  randomAgentAndSessionID,
} from "./testUtils";
import { PureJSCrypto } from "../crypto/PureJSCrypto";
import { Encrypted } from "../crypto/crypto";
import { PrivateTransaction } from "../coValueCore/verifiedState";

const wasmCrypto = await WasmCrypto.create();
const jsCrypto = await PureJSCrypto.create();

const agentSecret =
  "sealerSecret_zE3Nr7YFr1KkVbJSx4JDCzYn4ApYdm8kJ5ghNBxREHQya/signerSecret_z9fEu4eNG1eXHMak3YSzY7uLdoG8HESSJ8YW4xWdNNDSP";

function createTestNode() {
  const [agent, session] = agentAndSessionIDFromSecret(agentSecret);
  return {
    agent,
    session,
    node: new LocalNode(agent.agentSecret, session, jsCrypto),
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
});
