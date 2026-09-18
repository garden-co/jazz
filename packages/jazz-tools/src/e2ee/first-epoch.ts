import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db } from "../runtime/db.js";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { encodeCryptoContext } from "./context.js";
import { deviceRequestApp } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import type { LocalDevice } from "./local-device.js";
import type { KeyEnvelope } from "./types.js";

export function accountEpochContext(
  application: string,
  accountId: string,
  epoch: string,
  recipient: string,
  column = "envelope",
): Uint8Array {
  return encodeCryptoContext({
    application,
    policy: "jazz.e2ee.account-identity.v1",
    scope: "account",
    identifier: accountId,
    table: "__e2ee_account_identities",
    row: accountId,
    column,
    epoch,
    recipient,
  });
}

export async function confirmAccountEpoch(
  keys: KeyEnvelope,
  application: string,
  accountId: string,
  identity: { epochId: string; verification: Uint8Array },
  secret: Uint8Array,
): Promise<void> {
  if (secret.length !== 32) throw new Error("Invalid E2EE account epoch");
  const confirmation = await keys.unwrap(
    secret,
    accountEpochContext(application, accountId, identity.epochId, "", "verification"),
    identity.verification,
  );
  try {
    if (confirmation.length !== 32 || confirmation.some((byte) => byte !== 0))
      throw new Error("E2EE account epoch key confirmation failed");
  } finally {
    confirmation.fill(0);
  }
}

/** The account ID is the immutable identity row ID, so creation has one winner. */
export async function firstAccountEpoch(
  db: Db,
  accountId: string,
  application: string,
  device: LocalDevice,
  keys: KeyEnvelope,
  assertOpen: () => void,
  app: DeviceTables = deviceRequestApp,
): Promise<string> {
  const identities = app.__e2ee_account_identities;
  const context = (epoch: string, recipient: string, column = "envelope") =>
    accountEpochContext(application, accountId, epoch, recipient, column);
  for (let attempt = 0; ; attempt++) {
    assertOpen();
    try {
      const proposal = await exclusiveE2eeTransaction(db, async (tx) => {
        const existing = await tx.one(identities.where({ id: accountId }), { tier: "local" });
        assertOpen();
        if (existing) return existing;
        const epochId = crypto.randomUUID();
        const secret = runtimeRandomBytes(32);
        try {
          const envelope = await keys.seal(device.publicKey, context(epochId, device.id), secret);
          const verification = await keys.wrap(
            secret,
            context(epochId, "", "verification"),
            new Uint8Array(32),
          );
          assertOpen();
          return tx.insert(
            identities,
            { deviceId: device.id, epochId, envelope, verification, ledgerVersion: 1 },
            { id: accountId },
          );
        } finally {
          secret.fill(0);
        }
      });
      // Even an existing identity is validated by a read-only exclusive commit:
      // local/optimistic observations alone must not select the active device.
      const accepted = await proposal.wait({ tier: "global" });
      assertOpen();
      if (accepted.ledgerVersion !== 1)
        throw new Error("E2EE account requires public ledger migration");
      // The insert policy validates against the already accepted private identity.
      // This projection never selects or replaces that identity.
      const roots = app.__e2ee_account_roots;
      if (!(await db.one(roots.where({ accountId }), { tier: "edge" }))) {
        assertOpen();
        // Concurrent publications may duplicate the same policy-checked binding.
        await db
          .insert(roots, {
            accountId,
            deviceId: accepted.deviceId,
            epochId: accepted.epochId,
            ledgerVersion: accepted.ledgerVersion,
          })
          .wait({ tier: "global" });
      }
      assertOpen();
      if (accepted.deviceId === device.id) {
        const secret = await keys.open(
          device,
          context(accepted.epochId, device.id),
          accepted.envelope,
        );
        try {
          assertOpen();
          await confirmAccountEpoch(keys, application, accountId, accepted, secret);
          assertOpen();
        } finally {
          secret.fill(0);
        }
      }
      return accepted.deviceId;
    } catch (error) {
      if (
        attempt >= 2 ||
        !(error instanceof PersistedWriteRejectedError) ||
        (error.code !== "transaction_conflict" && error.code !== "permission_denied")
      )
        throw error;
      if (!(await db.one(identities.where({ id: accountId }), { tier: "edge" }))) throw error;
      // Re-read the winner after a competing initialiser is accepted. Never
      // replace an existing identity, even if this device cannot open its key.
    }
  }
}
