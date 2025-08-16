import * as fs from "fs";
import * as path from "path";
import { ed25519 } from "@noble/curves/ed25519";
import { base58 } from "@scure/base";
import { SessionLog, initialize } from "cojson-core-wasm";
import { beforeAll, describe, expect, it } from "vitest";
import { Transaction } from "../coValueCore/verifiedState";
import { PureJSCrypto } from "../crypto/PureJSCrypto";

function decodeZ(value: string): Uint8Array {
  const prefixEnd = value.indexOf("_z");
  if (prefixEnd === -1) {
    throw new Error("Invalid prefix");
  }
  return base58.decode(value.substring(prefixEnd + 2));
}

await initialize();
const jsCrypto = new PureJSCrypto();

describe("SessionLog WASM", () => {
  let exampleSessions: any;

  beforeAll(() => {
    const dataPath = path.resolve(
      __dirname,
      "../../../../crates/cojson-core/data/singleTxSession.json",
    );
    const data = fs.readFileSync(dataPath, "utf-8");
    exampleSessions = JSON.parse(data);
  });

  it("it works", () => {
    const session = new SessionLog(
      "co_test1",
      "session_test1",
      jsCrypto.getSignerID(jsCrypto.newRandomSigner()),
    );

    expect(session).toBeDefined();
  });

  it("decodeZ works", () => {
    const encoded = "z6Mkeq44t4iEbfY23e42n1g8G2yJ2g4g4g4g4g4g4g4g4g4g4";
    const decoded = decodeZ("signerID_z" + encoded);
    const scureDecoded = base58.decode(encoded);
    expect(decoded).toEqual(scureDecoded);
  });

  it("test_add_from_example_json", () => {
    const [sessionIdStr, example] = Object.entries(
      exampleSessions.exampleBase,
    )[0] as [string, any];
    const coIdStr = sessionIdStr.split("_session_")[0]!;

    let session = new SessionLog(
      coIdStr,
      sessionIdStr,
      exampleSessions.signerID,
    );

    try {
      session.tryAdd(
        [JSON.stringify(example.transactions[0])],
        example.lastSignature,
        false,
      );
    } catch (e) {
      console.error(e);
      throw e;
    }
  });

  it("test_add_from_multi_tx_example_json", () => {
    const dataPath = path.resolve(
      __dirname,
      "../../../../crates/cojson-core/data/multiTxSession.json",
    );
    const data = fs.readFileSync(dataPath, "utf-8");
    const multiTxExampleSessions = JSON.parse(data);

    const [sessionIdStr, example] = Object.entries(
      multiTxExampleSessions.exampleBase,
    )[0] as [string, any];
    const coIdStr = sessionIdStr.split("_session_")[0]!;

    let session = new SessionLog(
      coIdStr,
      sessionIdStr,
      multiTxExampleSessions.signerID,
    );

    try {
      session.tryAdd(
        example.transactions.map((tx: Transaction) => JSON.stringify(tx)),
        example.lastSignature,
        false,
      );
    } catch (e) {
      console.error(e);
      throw e;
    }
  });

  it("test_add_new_transaction", () => {
    const sessionData =
      exampleSessions.exampleBase[
        "co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR"
      ];
    const txFromExample = sessionData.transactions[0];
    const knownKey = exampleSessions.knownKeys[0];

    const signerSecret = jsCrypto.newRandomSigner();
    const publicKey = jsCrypto.getSignerID(signerSecret);

    let session = new SessionLog(
      exampleSessions.coID,
      "co_zkNajJ1BhLzR962jpzvXxx917ZB_session_zXzrQLTtp8rR",
      publicKey,
    );

    const changesJson =
      '[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]';
    const keyId = knownKey.id;
    const madeAt = txFromExample.madeAt;

    const signatureAndTxJson = session.addNewPrivateTransaction(
      changesJson,
      signerSecret,
      knownKey.secret,
      keyId,
      madeAt,
    );

    const { signature, transaction } = JSON.parse(signatureAndTxJson);

    expect(signature).toMatch(/^signature_z[a-zA-Z0-9]+$/);
    expect(transaction).toEqual(txFromExample);
  });

  it("test_decrypt_from_example_json", () => {
    const [sessionIdStr, example] = Object.entries(
      exampleSessions.exampleBase,
    )[0] as [string, any];
    let session = new SessionLog(
      exampleSessions.coID,
      sessionIdStr,
      exampleSessions.signerID,
    );

    session.tryAdd(
      example.transactions.map((tx: any) => JSON.stringify(tx)),
      example.lastSignature,
      true,
    );

    const keySecret = decodeZ(exampleSessions.knownKeys[0].secret);

    const decrypted = session.decryptNextTransactionChangesJson(0, keySecret);

    expect(decrypted).toEqual(
      '[{"after":"start","op":"app","value":"co_zMphsnYN6GU8nn2HDY5suvyGufY"}]',
    );
  });
});
