// React Native FFI Transaction Tests
// Note: These tests require the native module to be built and linked.
// They are designed to be run in a React Native environment.

import { describe, expect, it, beforeAll } from '@jest/globals';

// These tests are placeholders for RN environment testing.
// The actual uniffi bindings need to be built and the RN app needs to be running.

describe('cojson-core-rn FFI Transactions', () => {
  describe('createTransactionFfi', () => {
    it.todo('creates private transaction with all fields');
    it.todo('creates trusting transaction without keyUsed');
    it.todo('madeAt accepts bigint timestamps');
  });

  describe('tryAddFfi', () => {
    it.todo('accepts trusting transaction via tryAddFfi');
    it.todo('accepts private transaction via tryAddFfi');
    it.todo('tryAddFfi and tryAdd produce equivalent results');
    it.todo('rejects private transaction without keyUsed');
    it.todo('rejects invalid privacy type');
    it.todo('handles batch of multiple transactions');
  });
});

// Integration test example (requires RN runtime):
/*
import {
  SessionLog,
  createTransactionFfi,
  newEd25519SigningKey,
  ed25519VerifyingKey,
} from 'cojson-core-rn';
import { base58 } from '@scure/base';

function makeSignerKeyPair() {
  const signingKey = new Uint8Array(newEd25519SigningKey());
  const secret = "signerSecret_z" + base58.encode(signingKey);
  const verifyingKey = new Uint8Array(ed25519VerifyingKey(signingKey.buffer));
  const signerId = "signer_z" + base58.encode(verifyingKey);
  return { signingKey, secret, signerId };
}

describe('cojson-core-rn FFI Transactions (Integration)', () => {
  it('accepts trusting transaction via tryAddFfi', () => {
    const { secret, signerId } = makeSignerKeyPair();
    const coId = "co_z" + base58.encode(new Uint8Array(32));
    const sessionId = coId + "_session_z" + base58.encode(new Uint8Array(32));

    const sourceLog = new SessionLog(coId, sessionId, signerId);
    const madeAt = Date.now();
    const changesJson = JSON.stringify([{ op: "set", path: "/foo", value: "bar" }]);

    const signature = sourceLog.addNewTrustingTransaction(
      changesJson,
      secret,
      madeAt,
      undefined
    );

    const destLog = new SessionLog(coId, sessionId, signerId);
    const ffiTx = createTransactionFfi(
      "trusting",
      changesJson,
      undefined,
      BigInt(madeAt),
      undefined
    );

    expect(() => {
      destLog.tryAddFfi([ffiTx], signature, false);
    }).not.toThrow();
  });
});
*/
