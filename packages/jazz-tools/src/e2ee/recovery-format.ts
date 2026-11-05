import { encodeCryptoContext } from "./context.js";
import { encodeEnvelope } from "./envelope.js";
import { frameCryptoRecord } from "./record-frame.js";

/** Key-free recovery authority, independently generated from every device key. */
export type RecoveryRoot = {
  id: string;
  accountId: string;
  epochId: string;
  signerId: string;
  signingMechanism: string;
  signingVersion: number;
  signingPublicKey: Uint8Array;
  mechanism: string;
  version: number;
  publicKey: Uint8Array;
  signature: Uint8Array;
};

/** Signing bytes only; authority acceptance and key validation belong to replay. */
export function recoveryRootBytes(
  application: string,
  root: Omit<RecoveryRoot, "signature">,
): Uint8Array {
  return frameCryptoRecord([
    encodeCryptoContext({
      application,
      policy: "jazz.e2ee.recovery.v1",
      scope: "account",
      identifier: root.accountId,
      table: "__e2ee_recovery_roots",
      row: root.id,
      column: JSON.stringify(["root", root.signerId]),
      epoch: root.epochId,
      recipient: "",
    }),
    encodeEnvelope(
      { id: root.signingMechanism, version: root.signingVersion },
      root.signingPublicKey,
    ),
    encodeEnvelope({ id: root.mechanism, version: root.version }, root.publicKey),
  ]);
}
