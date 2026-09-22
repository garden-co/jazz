import { expect, it } from "vitest";
import { createNativeCrypto } from "./native.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import { replayAccountMembership } from "./public-membership.js";
import { recoveryRootBytes } from "./recovery-format.js";

const snapshot = <T extends { id: string }>(rows: T[], position: number) => ({
  rows,
  settlements: rows.map((row) => ({
    rowId: row.id,
    transactionId: crypto.randomUUID(),
    position: String(position),
  })),
});

it("requires initial authority before grants and recovery registration", async () => {
  const adapters = await createNativeCrypto();
  const signer = await adapters.deviceSigner.createKeyPair();
  const recipient = await adapters.deviceSigner.createKeyPair();
  const accountId = crypto.randomUUID();
  const application = "initial-epoch-ordering";
  const deviceId = crypto.randomUUID();
  const recipientId = crypto.randomUUID();
  const epochId = crypto.randomUUID();
  try {
    const root = { id: accountId, accountId, deviceId, epochId, ledgerVersion: 2 };
    const keys = (
      [
        [deviceId, signer.publicKey],
        [recipientId, recipient.publicKey],
      ] as const
    ).map(([id, publicKey]) => ({
      id: crypto.randomUUID(),
      deviceId: id,
      signingPublicKey: publicKey,
      signingMechanism: adapters.deviceSigner.mechanism.id,
      signingVersion: adapters.deviceSigner.mechanism.version,
      publicKey,
      mechanism: adapters.keyEnvelope.mechanism.id,
      version: adapters.keyEnvelope.mechanism.version,
    }));
    const approval = {
      id: crypto.randomUUID(),
      accountId,
      epochId,
      deviceId: recipientId,
      signerId: deviceId,
    };
    const signedApproval = {
      ...approval,
      signature: await adapters.deviceSigner.sign(
        signer.privateKey,
        publicDeviceApprovalBytes(application, approval),
      ),
    };
    const recovery = {
      id: crypto.randomUUID(),
      accountId,
      epochId,
      signerId: deviceId,
      signingPublicKey: recipient.publicKey,
      signingMechanism: adapters.deviceSigner.mechanism.id,
      signingVersion: adapters.deviceSigner.mechanism.version,
      publicKey: recipient.publicKey,
      mechanism: adapters.keyEnvelope.mechanism.id,
      version: adapters.keyEnvelope.mechanism.version,
    };
    const signedRecovery = {
      ...recovery,
      signature: await adapters.deviceSigner.sign(
        signer.privateKey,
        recoveryRootBytes(application, recovery),
      ),
    };
    for (const position of [9, 10, 11]) {
      const history = {
        roots: snapshot([root], 10),
        keys: snapshot(keys, 8),
        approvals: snapshot([signedApproval], position),
        successors: snapshot([], 0),
        recovery: snapshot([signedRecovery], position),
      };
      if (position === 10) {
        const transactionId = history.roots.settlements[0]!.transactionId;
        history.approvals.settlements[0]!.transactionId = transactionId;
        history.recovery.settlements[0]!.transactionId = transactionId;
      }
      const membership = await replayAccountMembership(history, application, adapters.deviceSigner);
      expect(membership.active.has(deviceId)).toBe(true);
      expect(membership.active.has(recipientId)).toBe(position > 10);
      expect(membership.approvalIds.has(approval.id)).toBe(position > 10);
      expect(membership.recoveryRoots.map((row) => row.id)).toEqual(
        position > 10 ? [recovery.id] : [],
      );
      // A later matching projection cannot move the activation cutoff.
      history.roots.rows.push({ ...root, id: crypto.randomUUID() });
      history.roots.settlements.push({
        rowId: history.roots.rows[1]!.id,
        transactionId: crypto.randomUUID(),
        position: "12",
      });
      const duplicate = await replayAccountMembership(history, application, adapters.deviceSigner);
      expect(duplicate.active).toEqual(membership.active);
      expect(duplicate.recoveryRoots).toEqual(membership.recoveryRoots);
    }
  } finally {
    signer.privateKey.fill(0);
    recipient.privateKey.fill(0);
  }
});
