import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db } from "../runtime/db.js";
import {
  observeE2eeHistory,
  E2eeHistoryUnavailable,
  type E2eeHistoryReader,
} from "./history-reader.js";
import type { RowSettlement } from "../runtime/client.js";
import type { DeviceSigner } from "./types.js";
import { sameSnapshotValue } from "./public-snapshot.js";
import { deviceRequestApp as app } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import { recoveryRootBytes, type RecoveryRoot } from "./recovery-format.js";
import {
  decodeEpochIds,
  encodePublicApprovalRevision,
  publicSuccessorSigningBytes,
} from "./account-successor.js";

/** Internal recipient discovery. Never reads another account's private handshake. */
export async function readAccountMembership(
  db: Db,
  accountId: string,
  application: string,
  signer: DeviceSigner,
  tables: DeviceTables = app,
) {
  if (await db.e2eeIsExplicitlyOffline())
    return observeE2eeHistory(db, async (reader) =>
      replayAccountMembership(
        await readPublicMembershipHistory(reader, accountId, tables),
        application,
        signer,
      ),
    );
  await prefetchPublicMembershipHistory(db, accountId, tables);
  const read = await exclusiveE2eeTransaction(db, (tx) =>
    readPublicMembershipHistory(tx, accountId, tables),
  );
  // Refuse unavailable coverage; a partial history must never imply active membership.
  return replayAccountMembership(await read.wait({ tier: "global" }), application, signer);
}

/** Fetch candidate references without treating them as accepted recipients. */
export async function prefetchPublicMembershipHistory(
  db: Db,
  accountId: string,
  tables: DeviceTables = app,
): Promise<void> {
  const rootsQuery = tables.__e2ee_account_roots.where({ accountId });
  const keysQuery = tables.__e2ee_device_keys.where({ "$createdBy.account": accountId });
  const approvalsQuery = tables.__e2ee_public_device_approvals.where({ accountId });
  const successorsQuery = tables.__e2ee_public_account_successors.where({ accountId });
  const recoveryQuery = tables.__e2ee_recovery_roots.where({ accountId });
  await Promise.all([
    db.all(rootsQuery, { tier: "edge" }),
    db.all(keysQuery, { tier: "edge" }),
    db.all(approvalsQuery, { tier: "edge" }),
    db.all(successorsQuery, { tier: "edge" }),
    db.all(recoveryQuery, { tier: "edge" }),
  ]);
}

/** Strict authority cutoff: a grant cannot authorise another grant in the same transaction. */
export function historyBefore(
  history: Awaited<ReturnType<typeof readPublicMembershipHistory>>,
  cut: bigint,
): Awaited<ReturnType<typeof readPublicMembershipHistory>> {
  const before = <T extends { id: string }>(snapshot: {
    rows: T[];
    settlements: RowSettlement[];
  }) => {
    const order = positions(snapshot);
    const rows = snapshot.rows.filter((row) => order.get(row.id)! < cut);
    const ids = new Set(rows.map((row) => row.id));
    return { rows, settlements: snapshot.settlements.filter((entry) => ids.has(entry.rowId)) };
  };
  return {
    roots: before(history.roots),
    keys: before(history.keys),
    approvals: before(history.approvals),
    successors: before(history.successors),
    recovery: before(history.recovery),
  };
}

/** Shared accepted-history reader; authority transactions still require their global wait. */
export async function readPublicMembershipHistory(
  tx: E2eeHistoryReader,
  accountId: string,
  tables: DeviceTables = app,
) {
  const [roots, keys, approvals, successors, recovery] = await Promise.all([
    tx.allSettledForE2ee(tables.__e2ee_account_roots.where({ accountId })),
    tx.allSettledForE2ee(tables.__e2ee_device_keys.where({ "$createdBy.account": accountId })),
    tx.allSettledForE2ee(tables.__e2ee_public_device_approvals.where({ accountId })),
    tx.allSettledForE2ee(tables.__e2ee_public_account_successors.where({ accountId })),
    tx.allSettledForE2ee(tables.__e2ee_recovery_roots.where({ accountId })),
  ]);
  return { roots, keys, approvals, successors, recovery };
}

type AccountMembership = {
  epochId: string;
  active: Set<string>;
  revoked: Set<string>;
  approvalIds: Set<string>;
  successorIds: Set<string>;
  recoveryRoots: RecoveryRoot[];
};

type History = Awaited<ReturnType<typeof readPublicMembershipHistory>>;
type ValidatedAccount = {
  history: History;
  application: string;
  mechanism: DeviceSigner["mechanism"];
  verify: DeviceSigner["verify"];
  state: AccountMembership;
};
// Keep a small working set per adapter, shared by its account/group/space readers.
// ponytail: eight full public histories; index validated prefixes if this working set thrashes.
const validatedAccounts = new WeakMap<DeviceSigner, ValidatedAccount[]>();

export async function replayAccountMembership(
  history: History,
  application: string,
  signer: DeviceSigner,
  recoveryMemo?: Map<string, boolean>,
): Promise<AccountMembership> {
  // Recovery recursion owns a separate ancestry memo and always follows the
  // original replay path. No failed or in-flight result enters the shared cache.
  if (recoveryMemo) return replay(history, application, signer, recoveryMemo);
  const cached = validatedAccounts
    .get(signer)
    ?.find(
      (entry) =>
        entry.application === application &&
        entry.verify === signer.verify &&
        sameSnapshotValue(entry.mechanism, signer.mechanism) &&
        sameSnapshotValue(entry.history, history),
    );
  if (cached) return structuredClone(cached.state);
  const state = await replay(history, application, signer);
  const entries = validatedAccounts.get(signer) ?? [];
  entries.push({
    ...structuredClone({ history, application, mechanism: signer.mechanism, state }),
    verify: signer.verify,
  });
  if (entries.length > 8) entries.shift();
  validatedAccounts.set(signer, entries);
  return state;
}

async function replay(
  history: Awaited<ReturnType<typeof readPublicMembershipHistory>>,
  application: string,
  signer: DeviceSigner,
  recoveryMemo = new Map<string, boolean>(),
): Promise<AccountMembership> {
  const recoveryPositions = positions(history.recovery);
  // Each recursive cutoff strictly precedes its root. Cache roots to avoid
  // re-verifying the same recovery ancestry for every subsequent approval.
  const validRecovery = async (root: RecoveryRoot): Promise<boolean> => {
    const cached = recoveryMemo.get(root.id);
    if (cached !== undefined) return cached;
    const prior = historyBefore(history, recoveryPositions.get(root.id)!);
    // Immutable projections can be published after the binding they establish.
    prior.roots = history.roots;
    prior.keys = history.keys;
    const atRegistration = await replayAccountMembership(prior, application, signer, recoveryMemo);
    let valid = false;
    if (root.epochId === atRegistration.epochId && atRegistration.active.has(root.signerId)) {
      const key = prior.keys.rows.find((row) => row.deviceId === root.signerId);
      if (!key) throw new E2eeHistoryUnavailable("Missing accepted E2EE recovery signer keys");
      if (
        key.signingMechanism !== signer.mechanism.id ||
        key.signingVersion !== signer.mechanism.version
      )
        throw new Error("Unsupported E2EE recovery signing mechanism");
      let bytes: Uint8Array | undefined;
      try {
        bytes = recoveryRootBytes(application, root);
      } catch {
        /* Invalid candidate. */
      }
      if (bytes) valid = await signer.verify(key.signingPublicKey, bytes, root.signature);
    }
    recoveryMemo.set(root.id, valid);
    return valid;
  };
  const rootPositions = positions(history.roots);
  positions(history.keys);
  const approvalPositions = positions(history.approvals);
  const successorPositions = positions(history.successors);
  const roots = [...history.roots.rows].sort((a, b) =>
    compare(rootPositions.get(a.id)!, rootPositions.get(b.id)!),
  );
  const root = roots[0];
  if (!root) throw new E2eeHistoryUnavailable("Missing accepted E2EE account root");
  if (roots.some((row) => row.ledgerVersion !== 1))
    throw new Error("E2EE account requires public ledger migration");
  if (roots.some((row) => row.deviceId !== root.deviceId || row.epochId !== root.epochId))
    throw new Error("Conflicting E2EE account roots");
  let epochId = root.epochId;
  // Root publication can be delayed. Insert policies require the private identity
  // and immutable requests to exist before any public approval or successor.
  let epochPosition = -1n;
  let base = new Set([root.deviceId]);
  const revoked = new Set<string>();
  const approvalIds = new Set<string>();
  const successorIds = new Set<string>();
  const visited = new Set([epochId]);
  const keyFor = (deviceId: string) => history.keys.rows.find((key) => key.deviceId === deviceId);
  if (!keyFor(root.deviceId)) throw new E2eeHistoryUnavailable("Missing accepted E2EE root keys");
  const verify = async (deviceId: string, bytes: Uint8Array, signature: Uint8Array) => {
    const key = keyFor(deviceId);
    if (!key) throw new E2eeHistoryUnavailable("Missing accepted E2EE signer keys");
    if (
      key.signingMechanism !== signer.mechanism.id ||
      key.signingVersion !== signer.mechanism.version
    )
      throw new Error("Unsupported E2EE membership signing mechanism");
    return signer.verify(key.signingPublicKey, bytes, signature);
  };
  const approvals = [...history.approvals.rows].sort((a, b) =>
    compare(approvalPositions.get(a.id)!, approvalPositions.get(b.id)!),
  );
  // ponytail: replay the account history per epoch; index epochs if histories grow large.
  const membersBefore = async (cut?: bigint) => {
    const active = new Set(base);
    let before = new Set(active);
    let batch: bigint | undefined;
    for (const approval of approvals) {
      const position = approvalPositions.get(approval.id)!;
      if (
        approval.epochId !== epochId ||
        position <= epochPosition ||
        (cut !== undefined && position >= cut)
      )
        continue;
      if (position !== batch) {
        batch = position;
        before = new Set(active);
      }
      if (revoked.has(approval.deviceId)) continue;
      const recovery = approval.recoveryRootId != null;
      if (!recovery && (!before.has(approval.signerId) || approval.recoverySignature != null))
        continue;
      let bytes: Uint8Array;
      try {
        bytes = publicDeviceApprovalBytes(application, approval);
      } catch {
        continue;
      }
      // Adapter failures propagate; failure to verify is not evidence of invalidity.
      if (recovery) {
        const root = history.recovery.rows.find((row) => row.id === approval.recoveryRootId);
        if (
          !root ||
          recoveryPositions.get(root.id)! >= position ||
          !approval.recoverySignature ||
          !(await validRecovery(root))
        )
          continue;
        if (
          root.signingMechanism !== signer.mechanism.id ||
          root.signingVersion !== signer.mechanism.version
        )
          throw new Error("Unsupported E2EE recovery signing mechanism");
        if (!(await signer.verify(root.signingPublicKey, bytes, approval.recoverySignature)))
          continue;
      }
      if (await verify(approval.signerId, bytes, approval.signature)) {
        if (!keyFor(approval.deviceId))
          throw new E2eeHistoryUnavailable("Missing accepted E2EE recipient keys");
        active.add(approval.deviceId);
        approvalIds.add(approval.id);
      }
    }
    return active;
  };
  const successors = [...history.successors.rows].sort((a, b) =>
    compare(successorPositions.get(a.id)!, successorPositions.get(b.id)!),
  );
  for (const successor of successors) {
    const position = successorPositions.get(successor.id)!;
    if (
      successor.predecessor !== epochId ||
      position <= epochPosition ||
      visited.has(successor.epochId)
    )
      continue;
    if (
      successors.some(
        (other) =>
          other.id !== successor.id &&
          other.predecessor === epochId &&
          successorPositions.get(other.id) === position,
      )
    )
      continue;
    let bytes: Uint8Array;
    let recorded: string[];
    try {
      bytes = publicSuccessorSigningBytes(application, successor);
      recorded = decodeEpochIds(successor.membership);
    } catch {
      continue;
    }
    const revision = encodePublicApprovalRevision(
      approvals
        .filter(
          (approval) =>
            approval.epochId === epochId && approvalPositions.get(approval.id)! < position,
        )
        .map((approval) => approval.id),
    );
    if (
      revision.length !== successor.revision.length ||
      !revision.every((byte, i) => byte === successor.revision[i])
    )
      continue;
    const members = await membersBefore(position);
    if (!members.has(successor.signerId) || !members.has(successor.removedDeviceId)) continue;
    if (!(await verify(successor.signerId, bytes, successor.signature))) continue;
    members.delete(successor.removedDeviceId);
    if (recorded.length !== members.size || recorded.some((id) => !members.has(id))) continue;
    epochId = successor.epochId;
    epochPosition = position;
    base = members;
    revoked.add(successor.removedDeviceId);
    successorIds.add(successor.id);
    visited.add(epochId);
  }
  const active = await membersBefore();
  const recoveryRoots: RecoveryRoot[] = [];
  for (const root of history.recovery.rows) if (await validRecovery(root)) recoveryRoots.push(root);
  return { epochId, active, revoked, approvalIds, successorIds, recoveryRoots };
}

function positions(snapshot: { rows: { id: string }[]; settlements: RowSettlement[] }) {
  const result = new Map(
    snapshot.settlements.map((entry) => [entry.rowId, BigInt(entry.position)]),
  );
  if (snapshot.rows.some((row) => !result.has(row.id)))
    throw new Error("Incomplete E2EE authority coverage");
  return result;
}
function compare(a: bigint, b: bigint) {
  return a < b ? -1 : a > b ? 1 : 0;
}
