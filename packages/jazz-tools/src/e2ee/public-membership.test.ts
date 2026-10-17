import { expect, it } from "vitest";
import type { RowSettlement } from "../runtime/client.js";
import {
  encodeEpochIds,
  encodePublicApprovalRevision,
  publicSuccessorSigningBytes,
  type PublicAccountSuccessor,
} from "./account-successor.js";
import { createNativeCrypto } from "./native.js";
import { publicDeviceApprovalBytes, type PublicDeviceApproval } from "./public-device-approval.js";
import { replayAccountMembership } from "./public-membership.js";
import { recoveryRootBytes, type RecoveryRoot } from "./recovery-format.js";
import type { DeviceKeyPair } from "./types.js";

// Accepted-history seam: positions are authority transactions, not insertion order.
// Real signatures distinguish temporal ineligibility from malformed candidates.
function accepted<T extends { id: string }>(...entries: [T, number][]) {
  return {
    rows: entries.map(([row]) => row),
    settlements: entries.map(
      ([row, position]): RowSettlement => ({
        rowId: row.id,
        transactionId: `authority-${position}`,
        position: String(position),
      }),
    ),
  };
}

async function fixture() {
  const adapters = await createNativeCrypto();
  const signer = adapters.deviceSigner;
  const creator = await signer.createKeyPair();
  const recipient = await signer.createKeyPair();
  const recovery = await signer.createKeyPair();
  const encryption = await adapters.keyEnvelope.createKeyPair();
  const application = "initial-root-authority";
  const accountId = crypto.randomUUID();
  const epochId = crypto.randomUUID();
  const creatorId = crypto.randomUUID();
  const recipientId = crypto.randomUUID();
  const root = {
    id: crypto.randomUUID(),
    accountId,
    epochId,
    deviceId: creatorId,
    ledgerVersion: 1,
  };
  const devices: [string, DeviceKeyPair][] = [
    [creatorId, creator],
    [recipientId, recipient],
  ];
  const publicKeys = devices.map(([deviceId, pair]) => ({
    id: crypto.randomUUID(),
    deviceId,
    signingPublicKey: pair.publicKey,
    signingMechanism: signer.mechanism.id,
    signingVersion: signer.mechanism.version,
    publicKey: encryption.publicKey,
    mechanism: adapters.keyEnvelope.mechanism.id,
    version: adapters.keyEnvelope.mechanism.version,
  }));
  const history = {
    roots: accepted([root, 10]),
    keys: accepted(...publicKeys.map((row): [typeof row, number] => [row, 1])),
    approvals: accepted<
      PublicDeviceApproval & {
        signature: Uint8Array;
        recoveryRootId: string | null;
        recoverySignature: Uint8Array | null;
      }
    >(),
    successors: accepted<PublicAccountSuccessor & { signature: Uint8Array }>(),
    recovery: accepted<RecoveryRoot>(),
  };
  const approval = async () => {
    const row = {
      id: crypto.randomUUID(),
      accountId,
      epochId,
      deviceId: recipientId,
      signerId: creatorId,
    };
    return {
      ...row,
      signature: await signer.sign(creator.privateKey, publicDeviceApprovalBytes(application, row)),
      recoveryRootId: null,
      recoverySignature: null,
    };
  };
  const successor = async (revision: string[] = []) => {
    const row = {
      id: crypto.randomUUID(),
      accountId,
      predecessor: epochId,
      epochId: crypto.randomUUID(),
      signerId: creatorId,
      removedDeviceId: creatorId,
      membership: encodeEpochIds([]),
      revision: encodePublicApprovalRevision(revision),
    };
    return {
      ...row,
      signature: await signer.sign(
        creator.privateKey,
        publicSuccessorSigningBytes(application, row),
      ),
    };
  };
  const recoveryRoot = async (signerId = creatorId, privateKey = creator.privateKey) => {
    const row = {
      id: crypto.randomUUID(),
      accountId,
      epochId,
      signerId,
      signingMechanism: signer.mechanism.id,
      signingVersion: signer.mechanism.version,
      signingPublicKey: recovery.publicKey,
      mechanism: adapters.keyEnvelope.mechanism.id,
      version: adapters.keyEnvelope.mechanism.version,
      publicKey: encryption.publicKey,
    };
    return {
      ...row,
      signature: await signer.sign(privateKey, recoveryRootBytes(application, row)),
    };
  };
  return {
    history,
    root,
    creatorId,
    recipientId,
    recipient,
    recovery,
    application,
    signer,
    approval,
    successor,
    recoveryRoot,
    replay: () => replayAccountMembership(history, application, signer),
    close() {
      for (const pair of [creator, recipient, recovery, encryption]) pair.privateKey.fill(0);
    },
  };
}

it.each([9, 10, 11])(
  "only grants membership strictly after initial public root (position %i)",
  async (position) => {
    const f = await fixture();
    try {
      const grant = await f.approval();
      f.history.approvals = accepted([grant, position]);
      // A later identical publication must not delay the canonical activation.
      f.history.roots = accepted([{ ...f.root, id: crypto.randomUUID() }, 20], [f.root, 10]);
      const state = await f.replay();
      expect(state.active).toEqual(
        new Set(position > 10 ? [f.creatorId, f.recipientId] : [f.creatorId]),
      );
      expect(state.approvalIds).toEqual(new Set(position > 10 ? [grant.id] : []));
    } finally {
      f.close();
    }
  },
);

it.each([9, 10, 11])(
  "only advances epochs strictly after initial public root (position %i)",
  async (position) => {
    const f = await fixture();
    try {
      const successor = await f.successor();
      f.history.successors = accepted([successor, position]);
      const state = await f.replay();
      expect(state.epochId).toBe(position > 10 ? successor.epochId : f.root.epochId);
      expect(state.active).toEqual(new Set(position > 10 ? [] : [f.creatorId]));
      expect(state.successorIds).toEqual(new Set(position > 10 ? [successor.id] : []));
      expect(state.revoked).toEqual(new Set(position > 10 ? [f.creatorId] : []));
    } finally {
      f.close();
    }
  },
);

it("retains preactivation candidates in the raw public revision without granting membership", async () => {
  const f = await fixture();
  try {
    const earlyGrant = await f.approval();
    f.history.approvals = accepted([earlyGrant, 9]);
    const incomplete = await f.successor();
    const complete = await f.successor([earlyGrant.id]);
    f.history.successors = accepted([incomplete, 11], [complete, 12]);
    const state = await f.replay();
    expect(state.epochId).toBe(complete.epochId);
    expect(state.successorIds).toEqual(new Set([complete.id]));
    expect(state.approvalIds).toEqual(new Set());
    expect(state.active).toEqual(new Set());
  } finally {
    f.close();
  }
});

it.each([9, 10, 11])(
  "only registers recovery authority strictly after initial public root (position %i)",
  async (position) => {
    const f = await fixture();
    try {
      const recoveryRoot = await f.recoveryRoot();
      f.history.recovery = accepted([recoveryRoot, position]);
      f.history.roots = accepted([{ ...f.root, id: crypto.randomUUID() }, 20], [f.root, 10]);
      // Key projections remain immutable evidence, even if published later.
      f.history.keys = accepted(
        ...f.history.keys.rows.map((row): [typeof row, number] => [row, 30]),
      );
      const row = {
        id: crypto.randomUUID(),
        accountId: f.root.accountId,
        epochId: f.root.epochId,
        deviceId: f.recipientId,
        signerId: f.recipientId,
        recoveryRootId: recoveryRoot.id,
      };
      const bytes = publicDeviceApprovalBytes(f.application, row);
      const grant = {
        ...row,
        signature: await f.signer.sign(f.recipient.privateKey, bytes),
        recoverySignature: await f.signer.sign(f.recovery.privateKey, bytes),
      };
      f.history.approvals = accepted([grant, 12]);
      const state = await f.replay();
      expect(state.recoveryRoots.map((root) => root.id)).toEqual(
        position > 10 ? [recoveryRoot.id] : [],
      );
      expect(state.active).toEqual(
        new Set(position > 10 ? [f.creatorId, f.recipientId] : [f.creatorId]),
      );
      expect(state.approvalIds).toEqual(new Set(position > 10 ? [grant.id] : []));
      if (position > 10) {
        const descendant = await f.recoveryRoot(f.recipientId, f.recipient.privateKey);
        f.history.recovery = accepted([recoveryRoot, position], [descendant, 13]);
        expect((await f.replay()).recoveryRoots.map((root) => root.id)).toEqual([
          recoveryRoot.id,
          descendant.id,
        ]);
      }
    } finally {
      f.close();
    }
  },
);

it.each([
  "conflicting account",
  "conflicting device",
  "conflicting epoch",
  "unsupported version",
  "missing coverage",
])("fails closed on an inconsistent initial root: %s", async (kind) => {
  const f = await fixture();
  try {
    const duplicate = { ...f.root, id: crypto.randomUUID() };
    if (kind === "conflicting account") duplicate.accountId = crypto.randomUUID();
    if (kind === "conflicting device") duplicate.deviceId = f.recipientId;
    if (kind === "conflicting epoch") duplicate.epochId = crypto.randomUUID();
    if (kind === "unsupported version") duplicate.ledgerVersion = 0;
    f.history.roots = accepted([f.root, 10], [duplicate, 20]);
    if (kind === "missing coverage") f.history.roots.settlements.pop();
    await expect(f.replay()).rejects.toThrow(/Conflicting|migration|coverage/);
  } finally {
    f.close();
  }
});
