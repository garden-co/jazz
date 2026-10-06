import { sha256 } from "@noble/hashes/sha2.js";
import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db, TableProxy, E2eeTransactionScope } from "../runtime/db.js";
import type { RowSettlement } from "../runtime/client.js";
import type { AccountStore } from "../accounts/persistence.js";
import { sameSnapshotValue } from "./public-snapshot.js";
import { decodeRecoveryMaterial, type DecodedRecoveryMaterial } from "./recovery-format.js";
import { E2eeRecoveryError, RecoveryCandidateError } from "./recovery-error.js";
import { spaceRecoveryContext, spaceRecoveryBytes } from "./space-recovery-format.js";
import { loadRecoveredSpaceKey, retainRecoveredSpaceKey } from "./local-space-keys.js";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import { encodePublicApprovalRevision } from "./account-successor.js";
import { encodeGroupMembership } from "./group-successor.js";
import { spaceSuccessorBytes, spaceSuccessorContext } from "./space-successor.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import type { GroupMembershipSnapshot, Groups } from "./group-lifecycle.js";
import type { DeviceTables } from "./device-requests.js";
import type { DeviceKeyLifetime, LocalDevice } from "./local-device.js";
import type { DeviceSigner, KeyEnvelope } from "./types.js";
import type {
  SpaceTables,
  SpaceRoot,
  SpaceGrant,
  SpaceDelivery,
  SpaceSuccessor,
  SpaceRecoveryDelivery,
} from "./spaces.js";
import {
  historyBefore,
  prefetchPublicMembershipHistory,
  readPublicMembershipHistory,
  replayAccountMembership,
  type PublicMembershipHistory,
} from "./public-membership.js";
import {
  spaceContext,
  spaceRootBytes,
  spaceGrantBytes,
  spaceDeliveryContext,
  spaceDeliveryBytes,
} from "./space-format.js";

type DeviceState = {
  active: Set<string>;
  epochId: string;
  publicHistory: PublicMembershipHistory;
};
export type SpaceDataPreparation = (
  key: Uint8Array,
  root: SpaceRoot,
  tx: E2eeTransactionScope,
) => Promise<void>;
type Settled<T> = { rows: T[]; settlements: RowSettlement[] };
type Address = { scopeId: string; identifier: string };
// An identity mismatch, malformed transcript or checked signature mismatch invalidates a root.
// Missing authority/history and operational failures must not be silently discarded.
class InvalidSpaceRoot extends Error {}
class UnavailableSpaceKey extends Error {}

function unavailableSpaceKey(cause: unknown): never {
  throw new UnavailableSpaceKey("Unable to confirm E2EE space key", { cause });
}

const recoveryIdBatchSize = 128;

function isTransactionConflict(error: unknown): boolean {
  return (
    (error instanceof PersistedWriteRejectedError &&
      (error.code === "exclusive_conflict" || error.code === "transaction_conflict")) ||
    (error instanceof Error && error.message.startsWith("(transaction_conflict):"))
  );
}

function spaceRootId(address: Address): string {
  const digest = sha256(
    new TextEncoder().encode(
      JSON.stringify(["jazz.e2ee.space-id.v1", address.scopeId, address.identifier]),
    ),
  ).slice(0, 16);
  digest[6] = (digest[6]! & 0x0f) | 0x80;
  digest[8] = (digest[8]! & 0x3f) | 0x80;
  const hex = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

type Snapshot = {
  roots: Settled<SpaceRoot>;
  grants: Settled<SpaceGrant>;
  deliveries: Settled<SpaceDelivery>;
  successors: Settled<SpaceSuccessor>;
  histories: Map<string, PublicMembershipHistory>;
  group?: GroupMembershipSnapshot;
};
type AuthoritySnapshot = Omit<Snapshot, "deliveries">;
type ReplayInput = {
  snapshot: AuthoritySnapshot;
  address: Address;
  contexts: Map<string, string>;
  mechanism: DeviceSigner["mechanism"];
};
export type SpaceRecoveryPath = Address & { spaceId: string; epochId: string } & (
    | { validation: "validated" }
    | {
        validation: "unavailable";
        reason: "missing-recovery-delivery" | "unusable-recovery-delivery" | "maintenance-required";
      }
  );
type SpaceState = {
  state: "ready" | "refused" | "unavailable" | "maintenance-required";
  reason?: string;
};
// Settlements are internal read metadata, never passed to crypto adapters.
// Each table wrapper owns its index, even when row IDs overlap across tables.
const positions = new WeakMap<Settled<unknown>, Map<string, string>>();
function positionOf<T>(snapshot: Settled<T>, id: string): string | undefined {
  let index = positions.get(snapshot);
  if (!index) {
    index = new Map();
    for (const entry of snapshot.settlements) {
      if (!index.has(entry.rowId)) index.set(entry.rowId, entry.position);
    }
    positions.set(snapshot, index);
  }
  return index.get(id);
}

// Initialisation knows the first secret without conferring recipient membership.
// This authority belongs to that exact author/device/account epoch, never a successor.
const isInitialAuthor = (
  root: SpaceRoot,
  epochId: string,
  accountId: string,
  deviceId: string,
  accountEpochId: string,
) =>
  epochId === root.epochId &&
  accountId === root.accountId &&
  deviceId === root.deviceId &&
  accountEpochId === root.accountEpochId;

export function initialRecipientIds(ids: readonly string[]): string[] {
  if (
    !Array.isArray(ids) ||
    ids.length === 0 ||
    ids.some(
      (id) =>
        typeof id !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id),
    )
  )
    throw new Error("Initial E2EE recipients must be a non-empty set of IDs");
  return [...new Set(ids)];
}

/** Scoped roots and accepted-only device delivery. Jazz policies own every write. */
export class Spaces {
  private validated?: {
    input: ReplayInput;
    verify: DeviceSigner["verify"];
    states: Map<bigint | undefined, Awaited<ReturnType<Spaces["replay"]>>>;
  };
  private readonly replayContexts = new WeakMap<Snapshot, NonNullable<Spaces["validated"]>>();

  constructor(
    private readonly db: Db,
    private readonly accountId: string,
    private readonly tables: DeviceTables & SpaceTables,
    private readonly keys: KeyEnvelope,
    private readonly signer: DeviceSigner,
    private readonly accountContext: (accountId: string) => string,
    private readonly assertOpen: () => void,
    private readonly device?: {
      readonly id: string;
      store: AccountStore;
      isKnownRevoked(): boolean;
      load(): Promise<LocalDevice>;
      release(device: LocalDevice): void;
      readonly keyLifetime: DeviceKeyLifetime;
      states(tx?: E2eeTransactionScope): Promise<DeviceState>;
    },
    private readonly groups?: Groups,
  ) {}

  private requireDevice() {
    if (!this.device) throw new Error("Device enrolment is required for this space operation");
    return this.device;
  }

  private async loadDevice(): Promise<LocalDevice> {
    const provider = this.requireDevice();
    const device = await provider.load();
    try {
      this.assertOpen();
      return device;
    } catch (error) {
      provider.release(device);
      throw error;
    }
  }

  private async address<T, Init>(scope: TableProxy<T, Init>, identifier: string): Promise<Address> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(identifier))
      throw new Error("Invalid E2EE space identifier");
    const scopeId = await this.db.tableIdentity(scope);
    if (!scopeId) throw new Error("E2EE scope is unavailable");
    this.assertOpen();
    const address = { scopeId, identifier };
    return address;
  }

  private async discoverRecoveryRoots(): Promise<{
    own: PublicMembershipHistory;
    roots: SpaceRoot[];
  }> {
    await prefetchPublicMembershipHistory(this.db, this.accountId, this.tables);
    await this.groups?.warmMembership({ accountId: this.accountId });
    const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
      const own = await readPublicMembershipHistory(tx, this.accountId, this.tables);
      // Deleted grants seed discovery only; live replay still proves membership.
      const direct = await tx.allSettledForE2ee(
        this.tables.__e2ee_space_grants.includeDeleted().where({
          recipientKind: "account",
          recipientId: this.accountId,
        }),
      );
      const spaceIds = new Set(direct.rows.map((row) => row.spaceId));
      if (this.groups) {
        const membership = await this.groups.readMembership(tx, { accountId: this.accountId }, own);
        const graph = await this.groups.acceptedGraph(membership);
        const groupIds = [...graph]
          .filter(([, state]) => state.members.has(this.accountId))
          .map(([id]) => id);
        // Keep the empty predicate covered too; never widen an empty group scope.
        let start = 0;
        do {
          const grants = await tx.allSettledForE2ee(
            this.tables.__e2ee_space_grants.includeDeleted().where({
              recipientKind: "group",
              recipientId: { in: groupIds.slice(start, start + recoveryIdBatchSize) },
            }),
          );
          for (const row of grants.rows) spaceIds.add(row.spaceId);
          start += recoveryIdBatchSize;
        } while (start < groupIds.length);
      }
      const ids = [...spaceIds];
      const roots: SpaceRoot[] = [];
      for (let start = 0; start < ids.length; start += recoveryIdBatchSize) {
        const found = await tx.allSettledForE2ee(
          this.tables.__e2ee_spaces.where({
            id: { in: ids.slice(start, start + recoveryIdBatchSize) },
          }),
        );
        roots.push(...found.rows);
      }
      return { own, roots };
    });
    const { own, roots } = await read.wait({ tier: "global" });
    this.assertOpen();
    // Discovery is a candidate superset. Fresh per-path replay establishes entitlement.
    return { own, roots: roots.filter((root) => root.id === spaceRootId(root)) };
  }

  /** Read-only coverage: no device state, private store, delivery or maintenance writes. */
  async inspectRecovery(value: string, accountEpochId: string): Promise<SpaceRecoveryPath[]> {
    const material = await decodeRecoveryMaterial(
      value,
      this.accountContext(this.accountId),
      this.keys,
      this.signer,
    );
    try {
      const discovery = await this.discoverRecoveryRoots();
      if ((await this.checkRecoveryAuthority(material, discovery.own)).epochId !== accountEpochId)
        throw new E2eeRecoveryError("recovery-state-changed");
      const paths: SpaceRecoveryPath[] = [];
      for (const observed of discovery.roots) {
        await this.warm(observed);
        const query = this.tables.__e2ee_space_recovery_deliveries.where({
          spaceId: observed.id,
          recipientAccountId: this.accountId,
          recoveryRootId: material.rootId,
        });
        await this.db.all(query, { tier: "global" });
        const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
          const own = await readPublicMembershipHistory(tx, this.accountId, this.tables);
          return {
            own,
            snapshot: await this.readRecords(tx, observed, observed.id, own),
            recovery: await tx.allSettledForE2ee(query),
          };
        });
        const { own, snapshot, recovery } = await read.wait({ tier: "global" });
        this.assertOpen();
        if ((await this.checkRecoveryAuthority(material, own)).epochId !== accountEpochId)
          throw new E2eeRecoveryError("recovery-state-changed");
        const state = await this.recoveryState(snapshot, observed);
        if (!state || state.sealed || !state.members.has(this.accountId)) continue;
        const path = {
          scopeId: state.root.scopeId,
          identifier: state.root.identifier,
          spaceId: state.root.id,
          epochId: state.keyRoot.epochId,
        };
        if (state.rotationRequired || !state.groupsReady) {
          paths.push({ ...path, validation: "unavailable", reason: "maintenance-required" });
          continue;
        }
        const secret = await this.openRecovery(
          snapshot,
          state.root,
          state.position,
          state.keyRoot.epochId,
          recovery,
          material,
        );
        if (secret) {
          secret.fill(0);
          paths.push({ ...path, validation: "validated" });
        } else {
          const present = recovery.rows.some((row) => row.epochId === path.epochId);
          paths.push({
            ...path,
            validation: "unavailable",
            reason: present ? "unusable-recovery-delivery" : "missing-recovery-delivery",
          });
        }
      }
      this.assertOpen();
      return paths.sort((a, b) => a.spaceId.localeCompare(b.spaceId));
    } finally {
      material.recipient.privateKey.fill(0);
      material.signing.privateKey.fill(0);
    }
  }

  private async checkRecoveryAuthority(
    material: DecodedRecoveryMaterial,
    history: PublicMembershipHistory,
  ) {
    const own = await replayAccountMembership(
      history,
      this.accountContext(this.accountId),
      this.signer,
    );
    const authority = own.recoveryRoots.find((root) => root.id === material.rootId);
    if (
      !authority ||
      authority.mechanism !== this.keys.mechanism.id ||
      authority.version !== this.keys.mechanism.version ||
      authority.signingMechanism !== this.signer.mechanism.id ||
      authority.signingVersion !== this.signer.mechanism.version ||
      !sameBytes(authority.publicKey, material.recipient.publicKey) ||
      !sameBytes(authority.signingPublicKey, material.signing.publicKey)
    )
      throw new E2eeRecoveryError("recovery-root-mismatch");
    return own;
  }

  private async openRecovery(
    snapshot: Snapshot,
    root: SpaceRoot,
    position: string,
    epochId: string,
    recovery: Settled<SpaceRecoveryDelivery>,
    material: DecodedRecoveryMaterial,
  ): Promise<Uint8Array | undefined> {
    for (const row of recovery.rows) {
      if (
        row.epochId !== epochId ||
        row.recipientAccountId !== this.accountId ||
        row.recoveryRootId !== material.rootId ||
        !(await this.acceptedDelivery(snapshot, root, position, row, positionOf(recovery, row.id)))
      )
        continue;
      const context = spaceRecoveryContext(
        this.accountContext(root.accountId),
        { ...root, epochId },
        row,
      );
      let secret: Uint8Array | undefined;
      try {
        try {
          secret = await this.keys.open(material.recipient, context, row.envelope);
        } catch (error) {
          unavailableSpaceKey(error);
        }
        await this.confirmHistory(snapshot, root, secret);
        this.assertOpen();
        return secret;
      } catch (error) {
        secret?.fill(0);
        this.assertOpen();
        if (!(error instanceof UnavailableSpaceKey)) throw error;
      }
    }
    return undefined;
  }

  /** Backfill existing memberships before reporting recovery material as usable. */
  async protectRecovery(material: string): Promise<void> {
    const provider = this.requireDevice();
    await prefetchPublicMembershipHistory(this.db, this.accountId, this.tables);
    const initial = await exclusiveE2eeTransaction(this.db, (tx) => provider.states(tx));
    const own = await initial.wait({ tier: "global" });
    this.assertOpen();
    if (!own.active.has(provider.id)) throw new Error("Space recovery requires an active device");
    const { roots } = await this.discoverRecoveryRoots();
    const required = new Set<string>();
    for (const observed of roots) {
      await this.warm(observed);
      const read = await exclusiveE2eeTransaction(this.db, (tx) =>
        this.readSnapshot(tx, observed, observed.id),
      );
      const snapshot = await read.wait({ tier: "global" });
      this.assertOpen();
      const state = await this.recoveryState(snapshot, observed);
      if (!state || state.sealed || !state.members.has(this.accountId)) continue;
      if ((await this.explainAddress(observed)).state !== "ready")
        throw new Error("Space key unavailable while creating recovery");
      required.add(observed.id);
    }
    // Ready creators need proof of recovery coverage, not local recovery-key
    // restaging or another delivery/maintenance pass.
    const paths = await this.inspectRecovery(material, own.epochId);
    let maintenance = false;
    let unavailableDelivery = false;
    for (const path of paths) {
      required.delete(path.spaceId);
      if (path.validation === "unavailable") {
        if (path.reason === "maintenance-required") maintenance = true;
        else unavailableDelivery = true;
      }
    }
    if (required.size) throw new Error("Recovery space is unavailable");
    if (maintenance) throw new Error("Space key unavailable while creating recovery");
    if (unavailableDelivery)
      throw new RecoveryCandidateError("recovery-space-delivery-unavailable");
  }

  /** Recovery material restores keys, never removed membership or revoked devices. */
  async restoreRecovery(value: string): Promise<void> {
    const material = await decodeRecoveryMaterial(
      value,
      this.accountContext(this.accountId),
      this.keys,
      this.signer,
    );
    let device: LocalDevice | undefined;
    try {
      device = await this.loadDevice();
      const { roots } = await this.discoverRecoveryRoots();
      for (const observed of roots) {
        await this.warm(observed);
        const query = this.tables.__e2ee_space_recovery_deliveries.where({
          spaceId: observed.id,
          recipientAccountId: this.accountId,
          recoveryRootId: material.rootId,
        });
        await this.db.all(query, { tier: "global" });
        const read = await exclusiveE2eeTransaction(this.db, async (tx) => ({
          snapshot: await this.readSnapshot(tx, observed, observed.id),
          recovery: await tx.allSettledForE2ee(query),
        }));
        const { snapshot, recovery } = await read.wait({ tier: "global" });
        this.assertOpen();
        if (!snapshot.state.active.has(device.id))
          throw new Error("Space recovery requires an active device");
        await this.checkRecoveryAuthority(material, snapshot.state.publicHistory);
        const state = await this.recoveryState(snapshot, observed);
        if (!state || state.sealed || !state.members.has(this.accountId)) continue;
        const secret = await this.openRecovery(
          snapshot,
          state.root,
          state.position,
          state.keyRoot.epochId,
          recovery,
          material,
        );
        try {
          if (!secret) throw new RecoveryCandidateError("recovery-space-delivery-unavailable");
          await retainRecoveredSpaceKey(
            this.requireDevice().store,
            this.accountContext(this.accountId),
            state.root.id,
            state.keyRoot.epochId,
            secret,
            this.assertOpen,
            this.requireDevice().keyLifetime,
          );
        } finally {
          secret?.fill(0);
        }
        // Re-read current eligibility before delivering or rotating from the recovered key.
        const result = await this.explainAddress(observed);
        if (result.state !== "ready" && result.state !== "refused")
          throw new Error("Recovered space requires maintenance");
      }
    } finally {
      material.recipient.privateKey.fill(0);
      material.signing.privateKey.fill(0);
      if (device) this.requireDevice().release(device);
    }
  }

  async grant<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    recipientId: string,
  ): Promise<void> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(recipientId))
      throw new Error("Invalid E2EE space recipient");
    const address = await this.address(scope, identifier);
    const roots = this.tables.__e2ee_spaces.where({ id: spaceRootId(address) });
    const observed = await this.db.one(roots, { tier: "global" });
    if (observed) {
      await this.changeRecipient(address, observed, recipientId, "add");
      await this.explain(scope, identifier);
      return;
    }
    // A reopened client may not have the existing scope row locally yet.
    await this.db.one(
      new TypedTableQueryBuilder(scope._table, scope._schema)
        .where({ id: identifier })
        .select("id"),
      { tier: "global" },
    );
    await this.warmInitialRecipients([recipientId]);
    const device = await this.loadDevice();
    try {
      const secret = runtimeRandomBytes(32);
      try {
        const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
          return this.stageInitial(tx, scope, identifier, [recipientId], address, device, secret);
        });
        const accepted = await proposal.wait({ tier: "global" });
        this.assertOpen();
        await this.deliver(address, accepted, secret, device);
        await this.explain(scope, identifier);
      } finally {
        secret.fill(0);
      }
    } finally {
      this.requireDevice().release(device);
    }
  }

  async warmInitialRecipients(recipientIds: readonly string[] = [this.accountId]): Promise<void> {
    const ids = initialRecipientIds(recipientIds);
    for (const recipientId of ids)
      if (recipientId !== this.accountId)
        await prefetchPublicMembershipHistory(this.db, recipientId, this.tables);
    await this.groups?.warmMembership({ groupIds: ids });
  }

  /** Internal preparation only: the caller owns the enclosing transaction and acceptance. */
  async prepareInitial<T, Init>(
    tx: E2eeTransactionScope,
    scope: TableProxy<T, Init>,
    identifier: string,
    prepareData: SpaceDataPreparation,
    recipientIds: readonly string[] = [this.accountId],
  ): Promise<void> {
    if (tx.kind !== "exclusive")
      throw new Error("E2EE initialisation requires an exclusive transaction");
    const address = await this.address(scope, identifier);
    const device = await this.loadDevice();
    try {
      const secret = runtimeRandomBytes(32);
      try {
        const root = await this.stageInitial(
          tx,
          scope,
          identifier,
          recipientIds,
          address,
          device,
          secret,
        );
        await prepareData(secret, root, tx);
        this.assertOpen();
      } finally {
        secret.fill(0);
      }
    } finally {
      this.requireDevice().release(device);
    }
  }

  private async stageInitial<T, Init>(
    tx: E2eeTransactionScope,
    scope: TableProxy<T, Init>,
    identifier: string,
    recipientIds: readonly string[],
    address: Address,
    device: LocalDevice,
    secret: Uint8Array,
  ): Promise<SpaceRoot> {
    const rootId = spaceRootId(address);
    const roots = this.tables.__e2ee_spaces.where({ id: rootId });
    if (
      await tx.one(this.tables.__e2ee_spaces.includeDeleted().where({ id: rootId }), {
        tier: "global",
      })
    )
      throw new Error("E2EE space already exists");
    const state = await this.requireDevice().states(tx);
    if (!state.active.has(device.id))
      throw new Error("An active approved device must initialise a space");
    const ids = initialRecipientIds(recipientIds);
    const groupSnapshot = await this.groups?.readMembership(
      tx,
      { groupIds: ids },
      state.publicHistory,
    );
    const graph = groupSnapshot && (await this.groups!.acceptedGraph(groupSnapshot));
    const recipients: Pick<SpaceGrant, "recipientId" | "recipientKind" | "recipientEpochId">[] = [];
    for (const recipientId of ids) {
      const recipientHistory =
        recipientId === this.accountId
          ? state.publicHistory
          : await readPublicMembershipHistory(tx, recipientId, this.tables);
      const group = graph?.get(recipientId);
      if (group && recipientHistory.roots.rows.length)
        throw new Error("Ambiguous E2EE space recipient ID");
      if (group) {
        if (group.sealed || group.rotationRequired)
          throw new Error("E2EE initial recipient group requires reconciliation");
        recipients.push({
          recipientId,
          recipientKind: "group",
          recipientEpochId: group.keyRoot.epochId,
        });
      } else {
        if (!recipientHistory.roots.rows.length)
          throw new Error("E2EE initial recipient account is unavailable");
        const recipient = await replayAccountMembership(
          recipientHistory,
          this.accountContext(recipientId),
          this.signer,
        );
        recipients.push({
          recipientId,
          recipientKind: "account",
          recipientEpochId: recipient.epochId,
        });
      }
    }
    const scopeRow = new TypedTableQueryBuilder(scope._table, scope._schema)
      .where({ id: identifier })
      .select("id");
    if (!(await tx.one(scopeRow, { tier: "local" })))
      throw new Error("Explicit space initialisation requires an existing scope row");
    if (await tx.one(roots, { tier: "local" })) throw new Error("E2EE space already exists");
    const root = {
      id: rootId,
      ...address,
      accountId: this.accountId,
      deviceId: device.id,
      accountEpochId: state.epochId,
      epochId: crypto.randomUUID(),
      initialGrantId: crypto.randomUUID(),
      mechanism: this.keys.mechanism.id,
      version: this.keys.mechanism.version,
    };
    const application = this.accountContext(root.accountId);
    const verification = await this.keys.wrap(
      secret,
      spaceContext(application, root, "verification"),
      new Uint8Array(32),
    );
    const authorContext = spaceContext(application, root, "author", device.id);
    const authorEnvelope = await this.keys.seal(device.publicKey, authorContext, secret);
    const completeRoot = { ...root, verification, authorEnvelope };
    await this.confirm(completeRoot, secret);
    const opened = await this.keys.open(device, authorContext, authorEnvelope);
    try {
      if (opened.length !== secret.length || opened.some((byte, i) => byte !== secret[i]))
        throw new Error("Invalid generated E2EE space envelope");
    } finally {
      opened.fill(0);
    }
    const signature = await this.sign(device, spaceRootBytes(application, completeRoot));
    this.assertOpen();
    const initialGrants: SpaceGrant[] = [];
    for (const [index, recipient] of recipients.entries()) {
      const grant = {
        id: index === 0 ? root.initialGrantId : crypto.randomUUID(),
        spaceId: root.id,
        epochId: root.epochId,
        authorAccountId: this.accountId,
        authorDeviceId: device.id,
        authorEpochId: state.epochId,
        operation: "add",
        ...recipient,
      };
      const grantSignature = await this.sign(device, spaceGrantBytes(application, root, grant));
      this.assertOpen();
      initialGrants.push({ ...grant, signature: grantSignature });
    }
    const signedRoot = { ...completeRoot, signature };
    this.assertOpen();
    const { id, ...values } = signedRoot;
    // The settled lookup above rejects visible roots. Exclusive upsert records
    // an exact-row precondition, so hidden roots and concurrent creators cannot
    // turn a filtered empty query into a second accepted space.
    tx.upsert(this.tables.__e2ee_spaces, id, values);
    for (const { id: grantId, ...grantValues } of initialGrants)
      tx.insert(this.tables.__e2ee_space_grants, grantValues, { id: grantId });
    return signedRoot;
  }

  async revoke<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    recipientId: string,
  ): Promise<void> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(recipientId))
      throw new Error("Invalid E2EE space recipient");
    const address = await this.address(scope, identifier);
    const root = await this.db.one(this.tables.__e2ee_spaces.where({ id: spaceRootId(address) }), {
      tier: "global",
    });
    if (!root) throw new Error("E2EE space not found");
    await this.changeRecipient(address, root, recipientId, "remove");
    for (let attempt = 0; ; attempt++) {
      try {
        await this.explain(scope, identifier);
        return;
      } catch (error) {
        // The removal is already accepted. Another reader may win follow-up
        // maintenance; revalidate its result without publishing removal again.
        if (!isTransactionConflict(error) || attempt >= 2) throw error;
      }
    }
  }

  private async readSnapshot(
    tx: E2eeTransactionScope,
    address: Address,
    id: string,
    extraAccounts: string[] = [],
    includeDeliveries = true,
  ): Promise<Snapshot & { state: DeviceState }> {
    const state = await this.requireDevice().states(tx);
    return {
      state,
      ...(await this.readRecords(
        tx,
        address,
        id,
        state.publicHistory,
        extraAccounts,
        includeDeliveries,
      )),
    };
  }

  private async readRecords(
    tx: E2eeTransactionScope,
    address: Address,
    id: string,
    ownHistory: PublicMembershipHistory,
    extraAccounts: string[] = [],
    includeDeliveries = true,
  ): Promise<Snapshot> {
    const [roots, grants, deliveries, successors] = await Promise.all([
      tx.allSettledForE2ee(this.tables.__e2ee_spaces.where({ id: spaceRootId(address) })),
      tx.allSettledForE2ee(this.tables.__e2ee_space_grants.where({ spaceId: id })),
      includeDeliveries
        ? tx.allSettledForE2ee(this.tables.__e2ee_space_deliveries.where({ spaceId: id }))
        : { rows: [], settlements: [] },
      tx.allSettledForE2ee(this.tables.__e2ee_space_successors.where({ spaceId: id })),
    ]);
    const histories = new Map([[this.accountId, ownHistory]]);
    const groupIds = this.groupHistoryIds(grants.rows, extraAccounts);
    const group =
      this.groups && groupIds !== undefined
        ? await this.groups.readMembership(tx, { groupIds }, ownHistory)
        : undefined;
    for (const [accountId, history] of group?.histories ?? []) histories.set(accountId, history);
    for (const accountId of this.accounts(roots.rows, grants.rows, [
      ...extraAccounts,
      ...successors.rows.map((row) => row.authorAccountId),
    ])) {
      if (!histories.has(accountId))
        histories.set(accountId, await readPublicMembershipHistory(tx, accountId, this.tables));
    }
    return { roots, grants, deliveries, successors, histories, group };
  }

  private accounts(roots: SpaceRoot[], grants: SpaceGrant[], extra: string[] = []): Set<string> {
    return new Set(
      [
        this.accountId,
        ...extra,
        ...roots.map((row) => row.accountId),
        ...grants.flatMap((row) => [
          row.authorAccountId,
          ...(row.recipientKind === "account" ? [row.recipientId] : []),
        ]),
      ].filter((id) => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)),
    );
  }

  private groupHistoryIds(grants: SpaceGrant[], extra: string[]): string[] | undefined {
    const candidates = [
      ...extra,
      ...grants.flatMap((row) => (row.recipientKind === "group" ? [row.recipientId] : [])),
    ];
    // Raw grants still require group replay even when every query operand is invalid.
    if (candidates.length === 0) return undefined;
    return [
      ...new Set(
        candidates.filter((id) =>
          /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id),
        ),
      ),
    ];
  }

  private async warm(observed: SpaceRoot, extraAccounts: string[] = []): Promise<void> {
    const [grants, successors] = await Promise.all([
      this.db.all(this.tables.__e2ee_space_grants.where({ spaceId: observed.id }), {
        tier: "global",
      }),
      this.db.all(this.tables.__e2ee_space_successors.where({ spaceId: observed.id }), {
        tier: "global",
      }),
      this.db.all(this.tables.__e2ee_space_deliveries.where({ spaceId: observed.id }), {
        tier: "global",
      }),
    ]);
    const groupIds = this.groupHistoryIds(grants, extraAccounts);
    if (groupIds !== undefined) await this.groups?.warmMembership({ groupIds });
    await Promise.all(
      [
        ...this.accounts([observed], grants, [
          ...extraAccounts,
          ...successors.map((row) => row.authorAccountId),
        ]),
      ].map((id) => prefetchPublicMembershipHistory(this.db, id, this.tables)),
    );
  }

  private async changeRecipient(
    address: Address,
    observed: SpaceRoot,
    recipientId: string,
    operation: "add" | "remove",
  ): Promise<void> {
    await this.warm(observed, [recipientId]);
    const device = await this.loadDevice();
    try {
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        // Envelopes do not establish membership authority. Reading them here
        // would make background delivery unnecessarily conflict with a grant.
        const snapshot = await this.readSnapshot(tx, address, observed.id, [recipientId], false);
        const { root, keyRoot, direct, groupRecipients, graph, rotationRequired, sealed } =
          await this.validate(snapshot, address);
        if (root.id !== observed.id || (operation === "add" && (rotationRequired || sealed)))
          throw new Error("E2EE space requires reconciliation");
        if (!snapshot.state.active.has(device.id))
          throw new Error("An active device must author a space grant");
        const history = snapshot.histories.get(recipientId);
        const recipientGroup = graph.get(recipientId);
        if (recipientGroup && history?.roots.rows.length)
          throw new Error("Ambiguous E2EE space recipient ID");
        let recipientKind: "account" | "group";
        let recipientEpochId: string;
        if (recipientGroup) {
          if (operation === "add" && (recipientGroup.sealed || recipientGroup.rotationRequired))
            throw new Error("E2EE recipient group requires reconciliation");
          recipientKind = "group";
          recipientEpochId = recipientGroup.keyRoot.epochId;
          if (
            operation === "add"
              ? groupRecipients.get(recipientId) === recipientEpochId
              : !groupRecipients.has(recipientId)
          )
            return;
        } else {
          if (!history?.roots.rows.length) throw new Error("E2EE recipient account is unavailable");
          const recipient = await replayAccountMembership(
            history,
            this.accountContext(recipientId),
            this.signer,
          );
          recipientKind = "account";
          recipientEpochId = recipient.epochId;
          if (
            operation === "add"
              ? direct.get(recipientId) === recipientEpochId
              : !direct.has(recipientId)
          )
            return;
        }
        const record = {
          id: crypto.randomUUID(),
          spaceId: root.id,
          epochId: keyRoot.epochId,
          authorAccountId: this.accountId,
          authorDeviceId: device.id,
          authorEpochId: snapshot.state.epochId,
          operation,
          recipientKind,
          recipientId,
          recipientEpochId,
        };
        const signature = await this.sign(
          device,
          spaceGrantBytes(this.accountContext(root.accountId), keyRoot, record),
        );
        this.assertOpen();
        const { id, ...values } = record;
        tx.insert(this.tables.__e2ee_space_grants, { ...values, signature }, { id });
      });
      // Administration is decided by Jazz, not by possession of the space key.
      await proposal.wait({ tier: "global" });
      this.assertOpen();
    } finally {
      this.requireDevice().release(device);
    }
  }

  private async recoveryState(snapshot: Snapshot, address: Address) {
    try {
      return await this.validate(snapshot, address);
    } catch (error) {
      if (!(error instanceof InvalidSpaceRoot)) throw error;
      this.assertOpen();
      return undefined;
    }
  }

  private sameReplayAdapter(cache: NonNullable<Spaces["validated"]>): boolean {
    const { mechanism, verify } = this.signer;
    return (
      cache.verify === verify &&
      cache.input.mechanism.id === mechanism.id &&
      cache.input.mechanism.version === mechanism.version
    );
  }

  private replayContext(snapshot: Snapshot, address: Address) {
    const previous = this.replayContexts.get(snapshot);
    if (
      previous &&
      previous.input.address.scopeId === address.scopeId &&
      previous.input.address.identifier === address.identifier &&
      this.sameReplayAdapter(previous)
    )
      return previous;

    // Capture authority once per covered read, not once per delivery candidate.
    // Delivery rows are authenticated separately and cannot change membership replay.
    const authority: AuthoritySnapshot = previous?.input.snapshot ?? {
      roots: snapshot.roots,
      grants: snapshot.grants,
      successors: snapshot.successors,
      histories: snapshot.histories,
      group: snapshot.group,
    };
    const contexts =
      previous?.input.contexts ??
      new Map(
        [
          ...new Set([
            ...authority.roots.rows.map((root) => root.accountId),
            ...authority.histories.keys(),
            ...(authority.group?.histories.keys() ?? []),
          ]),
        ].map((id) => [id, this.accountContext(id)]),
      );
    const input: ReplayInput = {
      snapshot: authority,
      address: { scopeId: address.scopeId, identifier: address.identifier },
      contexts,
      mechanism: { ...this.signer.mechanism },
    };
    if (
      !this.validated ||
      !this.sameReplayAdapter(this.validated) ||
      !sameSnapshotValue(input, this.validated.input)
    ) {
      this.validated = {
        input: structuredClone(input),
        verify: this.signer.verify,
        states: new Map(),
      };
    }
    const cache = this.validated;
    this.replayContexts.set(snapshot, cache);
    return cache;
  }

  private async validate(snapshot: Snapshot, address: Address, before?: bigint) {
    const cache = this.replayContext(snapshot, address);
    const cached = cache.states.get(before);
    if (cached) return structuredClone(cached);
    // The immutable basis never reaches adapters. Clone shared histories together
    // only on a genuine cutoff miss, preserving their aliases within this replay.
    const working = structuredClone(cache.input);
    const state = await this.replay(working.snapshot, working.address, working.contexts, before);
    // Completed, unmodified proofs only; coverage, device eligibility and keys stay live.
    if (this.sameReplayAdapter(cache) && sameSnapshotValue(working, cache.input)) {
      cache.states.set(before, structuredClone(state));
      if (cache.states.size > 8) cache.states.delete(cache.states.keys().next().value);
    }
    return state;
  }

  private async replay(
    snapshot: AuthoritySnapshot,
    address: Address,
    contexts: ReadonlyMap<string, string>,
    before?: bigint,
  ) {
    const accountContext = (id: string): string => {
      const context = contexts.get(id);
      if (context === undefined) throw new Error("Missing E2EE space account context");
      return context;
    };
    const root = snapshot.roots.rows[0];
    if (!root || snapshot.roots.rows.length !== 1)
      throw new Error("Invalid or unsupported E2EE space membership");
    let rootBytes: Uint8Array;
    try {
      rootBytes = spaceRootBytes(accountContext(root.accountId), root);
    } catch {
      throw new InvalidSpaceRoot("Malformed E2EE space root transcript");
    }
    if (root.id !== spaceRootId(root))
      throw new InvalidSpaceRoot("Invalid E2EE space root identity");
    const history = snapshot.histories.get(root.accountId);
    if (!history) throw new Error("Missing E2EE space creator history");
    const position = positionOf(snapshot.roots, root.id);
    if (!position) throw new Error("E2EE space root lacks authority coverage");
    const initialHistory = historyBefore(history, BigInt(position));
    if (!initialHistory.roots.rows.length)
      throw new InvalidSpaceRoot("Ineligible E2EE space creator");
    const initial = await replayAccountMembership(
      initialHistory,
      accountContext(root.accountId),
      this.signer,
    );
    if (!initial.active.has(root.deviceId) || initial.epochId !== root.accountEpochId)
      throw new InvalidSpaceRoot("Unauthorised E2EE space creator");
    const signingKey = initialHistory.keys.rows.find((row) => row.deviceId === root.deviceId);
    if (
      !signingKey ||
      signingKey.signingMechanism !== this.signer.mechanism.id ||
      signingKey.signingVersion !== this.signer.mechanism.version
    )
      throw new Error("Missing or unsupported E2EE space creator verification key");
    if (!(await this.signer.verify(signingKey.signingPublicKey, rootBytes, root.signature)))
      throw new InvalidSpaceRoot("Invalid E2EE space signature");
    const grant = snapshot.grants.rows.find((row) => row.id === root.initialGrantId);
    if (!grant) throw new Error("Invalid or unsupported E2EE space membership");
    if (positionOf(snapshot.grants, grant.id) !== position)
      throw new Error("E2EE space root and initial grant lack atomic authority coverage");
    if (
      root.scopeId !== address.scopeId ||
      root.identifier !== address.identifier ||
      grant.id !== root.initialGrantId ||
      grant.operation !== "add" ||
      (grant.recipientKind !== "account" && grant.recipientKind !== "group") ||
      grant.authorAccountId !== root.accountId ||
      grant.authorDeviceId !== root.deviceId ||
      grant.authorEpochId !== root.accountEpochId
    )
      throw new Error("Invalid E2EE initial space grant");
    if (
      !(await this.verify(
        history,
        grant.authorDeviceId,
        spaceGrantBytes(accountContext(root.accountId), root, grant),
        grant.signature,
      ))
    )
      throw new Error("Invalid E2EE space signature");
    const direct = new Map<string, string>();
    const groupRecipients = new Map<string, string>();
    const graphs = new Map<string, Promise<Awaited<ReturnType<Groups["acceptedGraph"]>>>>();
    const groupGraph = (cut?: bigint) => {
      const key = cut?.toString() ?? "current";
      let result = graphs.get(key);
      if (!result) {
        result =
          snapshot.group && this.groups
            ? this.groups.acceptedGraph(snapshot.group, cut)
            : Promise.resolve(new Map());
        graphs.set(key, result);
      }
      return result;
    };
    if (grant.recipientKind === "group") {
      const recipient = (await groupGraph(BigInt(position))).get(grant.recipientId);
      if (
        !recipient ||
        recipient.sealed ||
        recipient.rotationRequired ||
        recipient.keyRoot.epochId !== grant.recipientEpochId
      )
        throw new Error("Invalid E2EE initial recipient group");
      groupRecipients.set(grant.recipientId, grant.recipientEpochId);
    } else {
      const recipientHistory = snapshot.histories.get(grant.recipientId);
      if (!recipientHistory) throw new Error("Missing E2EE initial recipient history");
      const recipient =
        grant.recipientId === root.accountId
          ? initial
          : await replayAccountMembership(
              historyBefore(recipientHistory, BigInt(position)),
              accountContext(grant.recipientId),
              this.signer,
            );
      if (recipient.epochId !== grant.recipientEpochId)
        throw new Error("Invalid E2EE initial recipient epoch");
      direct.set(grant.recipientId, grant.recipientEpochId);
    }
    const accounts = new Map<
      string,
      Promise<Awaited<ReturnType<typeof replayAccountMembership>> | undefined>
    >();
    const account = (id: string, cut?: bigint) => {
      const key = JSON.stringify([id, cut?.toString()]);
      let result = accounts.get(key);
      if (!result) {
        result = (async () => {
          const known = snapshot.histories.get(id);
          if (!known) return undefined;
          const history = cut === undefined ? known : historyBefore(known, cut);
          if (!history.roots.rows.length) return undefined;
          return replayAccountMembership(history, accountContext(id), this.signer);
        })();
        accounts.set(key, result);
      }
      return result;
    };
    let keyRoot = root;
    let successor: SpaceSuccessor | undefined;
    let epochPosition = BigInt(position);
    let rotationRequired = false;
    let sealed = false;
    const usedEpochs = new Set([root.epochId]);
    const revisionIds = new Set(["space-grant:" + grant.id]);
    const effective = async (cut?: bigint) => {
      const graph = await groupGraph(cut);
      const members = new Map(direct);
      const revision = [...revisionIds];
      let stale = false;
      let groupsReady = true;
      for (const [id, epoch] of groupRecipients) {
        const recipient = graph.get(id);
        if (!recipient) {
          stale = true;
          groupsReady = false;
          continue;
        }
        revision.push(
          "group:" +
            JSON.stringify([
              id,
              recipient.keyRoot.epochId,
              recipient.sealed,
              JSON.parse(new TextDecoder().decode(recipient.revision)),
            ]),
        );
        if (recipient.sealed) {
          stale = true;
          continue;
        }
        if (recipient.keyRoot.epochId !== epoch || recipient.rotationRequired) stale = true;
        if (recipient.rotationRequired) groupsReady = false;
        for (const [accountId, accountEpochId] of recipient.members)
          if (!members.has(accountId)) members.set(accountId, accountEpochId);
      }
      for (const [id, epoch] of members)
        if ((await account(id, cut))?.epochId !== epoch) stale = true;
      return {
        graph,
        members,
        groupsReady,
        stale,
        revision: encodePublicApprovalRevision(revision),
      };
    };
    const events = [
      ...snapshot.grants.rows
        .filter((row) => row.id !== grant.id)
        .map((row) => {
          const at = positionOf(snapshot.grants, row.id);
          if (at === undefined) throw new Error("Missing E2EE space grant authority coverage");
          return { kind: "grant" as const, row, at: BigInt(at) };
        }),
      ...snapshot.successors.rows.map((row) => {
        const at = positionOf(snapshot.successors, row.id);
        if (at === undefined) throw new Error("Missing E2EE space successor authority coverage");
        return { kind: "successor" as const, row, at: BigInt(at) };
      }),
    ].sort((a, b) =>
      a.at !== b.at
        ? a.at < b.at
          ? -1
          : 1
        : a.kind !== b.kind
          ? a.kind === "successor"
            ? -1
            : 1
          : a.row.id < b.row.id
            ? -1
            : a.row.id > b.row.id
              ? 1
              : 0,
    );
    let previousPosition = BigInt(position);
    for (const event of events) {
      const { row, at } = event;
      if (
        at < BigInt(position) ||
        (before !== undefined && at >= before) ||
        row.spaceId !== root.id
      )
        continue;
      // Additional initial grants share the root's accepted transaction. They
      // cannot use another author, remove a recipient or introduce a successor.
      if (
        at === BigInt(position) &&
        (event.kind !== "grant" ||
          event.row.operation !== "add" ||
          event.row.epochId !== root.epochId ||
          event.row.authorAccountId !== root.accountId ||
          event.row.authorDeviceId !== root.deviceId ||
          event.row.authorEpochId !== root.accountEpochId)
      )
        continue;
      if (previousPosition !== at && (await effective(at)).members.size === 0) sealed = true;
      previousPosition = at;
      if (sealed) continue;
      if (event.kind === "successor") {
        const row = event.row;
        if (
          row.predecessor !== keyRoot.epochId ||
          usedEpochs.has(row.epochId) ||
          at <= epochPosition
        )
          continue;
        const state = await effective(at);
        if (
          !(rotationRequired || state.stale) ||
          !state.groupsReady ||
          !state.members.has(row.authorAccountId)
        )
          continue;
        const author = await account(row.authorAccountId, at);
        if (!author?.active.has(row.authorDeviceId) || author.epochId !== row.authorEpochId)
          continue;
        const next = new Map<string, string>();
        for (const id of state.members.keys()) {
          const recipient = await account(id, at);
          if (!recipient) throw new Error("Missing E2EE space recipient history");
          next.set(id, recipient.epochId);
        }
        if (
          !sameBytes(state.revision, row.revision) ||
          !sameBytes(encodeGroupMembership(next), row.membership)
        )
          continue;
        let bytes: Uint8Array;
        try {
          bytes = spaceSuccessorBytes(accountContext(root.accountId), root, row);
        } catch {
          continue;
        }
        if (
          !(await this.verify(
            snapshot.histories.get(row.authorAccountId)!,
            row.authorDeviceId,
            bytes,
            row.signature,
          ))
        )
          continue;
        for (const id of direct.keys()) direct.set(id, next.get(id)!);
        for (const id of groupRecipients.keys()) {
          const group = state.graph.get(id)!;
          if (group.sealed) groupRecipients.delete(id);
          else groupRecipients.set(id, group.keyRoot.epochId);
        }
        keyRoot = { ...root, epochId: row.epochId, verification: row.verification };
        successor = row;
        epochPosition = at;
        usedEpochs.add(row.epochId);
        rotationRequired = false;
        continue;
      }
      const record = event.row;
      if (record.epochId !== keyRoot.epochId) continue;
      const author = await account(record.authorAccountId, at);
      if (!author?.active.has(record.authorDeviceId) || author.epochId !== record.authorEpochId)
        continue;
      let bytes: Uint8Array;
      try {
        bytes = spaceGrantBytes(accountContext(root.accountId), keyRoot, record);
      } catch {
        continue;
      }
      if (
        !(await this.verify(
          snapshot.histories.get(record.authorAccountId)!,
          record.authorDeviceId,
          bytes,
          record.signature,
        ))
      )
        continue;
      if (record.operation !== "add" && record.operation !== "remove") continue;
      let recipients: Map<string, string>;
      if (record.recipientKind === "account") {
        const recipient = await account(record.recipientId, at);
        if (!recipient || recipient.epochId !== record.recipientEpochId) continue;
        recipients = direct;
      } else if (record.recipientKind === "group") {
        if (!snapshot.group) throw new Error("E2EE space group history is unavailable");
        const recipient = (await groupGraph(at)).get(record.recipientId);
        if (
          !recipient ||
          recipient.keyRoot.epochId !== record.recipientEpochId ||
          (record.operation === "add" && (recipient.sealed || recipient.rotationRequired))
        )
          continue;
        recipients = groupRecipients;
      } else continue;
      revisionIds.add("space-grant:" + record.id);
      if (record.operation === "remove") {
        if (recipients.delete(record.recipientId)) rotationRequired = true;
      } else {
        const previous = recipients.get(record.recipientId);
        if (previous !== undefined && previous !== record.recipientEpochId) rotationRequired = true;
        recipients.set(record.recipientId, record.recipientEpochId);
      }
    }
    const state = await effective(before);
    sealed ||= state.members.size === 0;
    return {
      root,
      grant,
      position,
      keyRoot,
      successor,
      epochPosition,
      direct,
      groupRecipients,
      ...state,
      rotationRequired: rotationRequired || state.stale,
      sealed,
    };
  }

  async explain<T, Init>(scope: TableProxy<T, Init>, identifier: string): Promise<SpaceState> {
    const address = await this.address(scope, identifier);
    return this.explainAddress(address);
  }

  /** Package-internal key access, always backed by a completed authority read. */
  async withKeys<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    use: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
    includeHistory = false,
  ): Promise<SpaceState> {
    const address = await this.address(scope, identifier);
    return this.explainAddress(address, use, includeHistory);
  }

  private async explainAddress(
    address: Address,
    use?: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
    includeHistory = false,
    conflicts = 0,
  ): Promise<SpaceState> {
    this.assertOpen();
    if (this.device?.isKnownRevoked()) return { state: "refused", reason: "device-not-active" };
    const observed = await this.db.one(
      this.tables.__e2ee_spaces.where({ id: spaceRootId(address) }),
      { tier: "global" },
    );
    if (!observed) return { state: "unavailable", reason: "space-not-found" };
    await this.warm(observed);
    const device = await this.loadDevice();
    let secret: Uint8Array | undefined;
    try {
      let snapshot = await this.readAcceptedSnapshot(address, observed.id);
      this.assertOpen();
      if (!snapshot.state.active.has(device.id))
        return { state: "refused", reason: "device-not-active" };
      if (!snapshot.roots.rows.length)
        return { state: "unavailable", reason: "space-not-accepted" };
      let state = await this.validate(snapshot, address);
      if (!state.groupsReady && !state.sealed && state.members.has(this.accountId) && this.groups) {
        for (const id of state.groupRecipients.keys()) {
          const group = state.graph.get(id);
          if (group?.rotationRequired && !group.sealed && group.members.has(this.accountId))
            await this.groups.explain(id);
        }
        // Group maintenance is an ordinary accepted write. Re-read authority
        // coverage and eligibility before opening or distributing any space key.
        await this.warm(observed);
        snapshot = await this.readAcceptedSnapshot(address, observed.id);
        this.assertOpen();
        if (!snapshot.state.active.has(device.id))
          return { state: "refused", reason: "device-not-active" };
        state = await this.validate(snapshot, address);
      }
      const { root, keyRoot, successor, position } = state;
      if (state.sealed) return { state: "refused", reason: "space-sealed" };
      const recipient = state.members.has(this.accountId);
      // The exact initial author may finish an interrupted first-epoch handoff,
      // but this does not grant membership or authority over a successor.
      const initialHandoff =
        !recipient &&
        !state.rotationRequired &&
        state.groupsReady &&
        isInitialAuthor(root, keyRoot.epochId, this.accountId, device.id, snapshot.state.epochId);
      if (!recipient && !initialHandoff)
        return { state: "refused", reason: "not-a-space-recipient" };
      if (!state.groupsReady) return { state: "maintenance-required", reason: "group-epoch-stale" };
      if (!successor && root.accountId === this.accountId && root.deviceId === device.id) {
        secret = await this.keys.open(
          device,
          spaceContext(this.accountContext(root.accountId), root, "author", device.id),
          root.authorEnvelope,
        );
        await this.confirmHistory(snapshot, root, secret);
      } else if (
        successor?.authorAccountId === this.accountId &&
        successor.authorDeviceId === device.id
      ) {
        secret = await this.keys.open(
          device,
          spaceSuccessorContext(
            this.accountContext(root.accountId),
            root,
            successor,
            "author-envelope",
            device.id,
          ),
          successor.authorEnvelope,
        );
        await this.confirmHistory(snapshot, root, secret);
      } else {
        for (const delivery of snapshot.deliveries.rows) {
          if (
            delivery.recipientAccountId !== this.accountId ||
            delivery.recipientDeviceId !== device.id ||
            delivery.epochId !== keyRoot.epochId ||
            !(await this.acceptedDelivery(
              snapshot,
              root,
              position,
              delivery,
              positionOf(snapshot.deliveries, delivery.id),
            ))
          )
            continue;
          const context = spaceDeliveryContext(
            this.accountContext(root.accountId),
            keyRoot,
            delivery,
          );
          let opened: Uint8Array | undefined;
          try {
            try {
              opened = await this.keys.open(device, context, delivery.envelope);
            } catch (error) {
              unavailableSpaceKey(error);
            }
            await this.confirmHistory(snapshot, root, opened);
            secret = opened;
            opened = undefined;
            break;
          } catch (error) {
            this.assertOpen();
            if (!(error instanceof UnavailableSpaceKey)) throw error;
            /* Another independently authenticated delivery may be usable. */
          } finally {
            opened?.fill(0);
          }
        }
      }
      if (!secret) {
        secret = await loadRecoveredSpaceKey(
          this.requireDevice().store,
          this.accountContext(this.accountId),
          root.id,
          keyRoot.epochId,
          this.assertOpen,
          this.requireDevice().keyLifetime,
        );
        if (secret) await this.confirmHistory(snapshot, root, secret);
      }
      if (!secret) return { state: "unavailable", reason: "space-key-not-delivered" };
      try {
        if (state.rotationRequired) {
          await this.rotate(address, keyRoot, secret, device);
          return this.explainAddress(address, use, includeHistory, conflicts);
        }
        // A verified key read need not wait for envelopes for other devices.
        // Explicit maintenance still delivers and revalidates its changed history.
        if (!use) {
          await this.deliver(address, keyRoot, secret, device);
          // Key delivery can add records after the snapshot used to open the key.
          const refreshed = await this.readAcceptedSnapshot(address, observed.id);
          if (!sameSnapshotValue(snapshot, refreshed)) {
            if (conflicts >= 2) return { state: "unavailable", reason: "history-keeps-changing" };
            return this.explainAddress(address, use, includeHistory, conflicts + 1);
          }
        }
      } catch (error) {
        // Key preparation may follow another client's completed maintenance.
        // Delivery may race another maintainer. Explicit rotation still reports
        // its losing contender; never retry use() itself.
        if (isTransactionConflict(error) && (use || !state.rotationRequired) && conflicts < 2)
          return this.explainAddress(address, use, includeHistory, conflicts + 1);
        if (!(error instanceof PersistedWriteRejectedError) || error.code !== "permission_denied")
          throw error;
        if (state.rotationRequired)
          return { state: "maintenance-required", reason: "recipient-removed" };
      }
      if (!recipient) return { state: "refused", reason: "not-a-space-recipient" };
      this.assertOpen();
      if (includeHistory) await this.confirmHistory(snapshot, root, secret, use);
      else await use?.(secret, keyRoot);
      return { state: "ready" };
    } finally {
      secret?.fill(0);
      this.requireDevice().release(device);
    }
  }

  private async readAcceptedSnapshot(address: Address, id: string) {
    for (let attempt = 0; ; attempt++) {
      try {
        const read = await exclusiveE2eeTransaction(this.db, (tx) =>
          this.readSnapshot(tx, address, id),
        );
        const snapshot = await read.wait({ tier: "global" });
        this.assertOpen();
        return snapshot;
      } catch (error) {
        if (!isTransactionConflict(error) || attempt >= 2) throw error;
      }
    }
  }

  private async rotate(
    address: Address,
    expected: SpaceRoot,
    previousSecret: Uint8Array,
    device: LocalDevice,
  ) {
    const secret = runtimeRandomBytes(32);
    try {
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const snapshot = await this.readSnapshot(tx, address, expected.id);
        const state = await this.validate(snapshot, address);
        if (state.keyRoot.epochId !== expected.epochId || state.root.id !== expected.id)
          throw new Error("E2EE space predecessor changed during rotation");
        if (
          !state.rotationRequired ||
          state.sealed ||
          !state.groupsReady ||
          !state.members.has(this.accountId) ||
          !snapshot.state.active.has(device.id)
        )
          throw new Error("E2EE space rotation requires an eligible remaining member");
        await this.confirmHistory(snapshot, state.root, previousSecret);
        const members = new Map<string, string>();
        for (const id of state.members.keys()) {
          const account = await replayAccountMembership(
            snapshot.histories.get(id)!,
            this.accountContext(id),
            this.signer,
          );
          members.set(id, account.epochId);
        }
        const coordinates = {
          id: crypto.randomUUID(),
          spaceId: state.root.id,
          predecessor: state.keyRoot.epochId,
          epochId: crypto.randomUUID(),
          authorAccountId: this.accountId,
          authorDeviceId: device.id,
          authorEpochId: snapshot.state.epochId,
        };
        const application = this.accountContext(state.root.accountId);
        const context = spaceSuccessorContext(
          application,
          state.root,
          coordinates,
          "author-envelope",
          device.id,
        );
        const authorEnvelope = await this.keys.seal(device.publicKey, context, secret);
        const opened = await this.keys.open(device, context, authorEnvelope);
        try {
          if (!sameBytes(secret, opened))
            throw new Error("Invalid generated E2EE space successor self-envelope");
        } finally {
          opened.fill(0);
        }
        const verification = await this.keys.wrap(
          secret,
          spaceContext(
            application,
            { ...state.keyRoot, epochId: coordinates.epochId },
            "verification",
          ),
          new Uint8Array(32),
        );
        await this.confirm(
          { ...state.keyRoot, epochId: coordinates.epochId, verification },
          secret,
        );
        const historyContext = spaceSuccessorContext(
          application,
          state.root,
          coordinates,
          "history",
        );
        const history = await this.keys.wrap(secret, historyContext, previousSecret);
        const previous = await this.keys.unwrap(secret, historyContext, history);
        try {
          if (!sameBytes(previous, previousSecret))
            throw new Error("Invalid generated E2EE space successor history");
        } finally {
          previous.fill(0);
        }
        const record = {
          ...coordinates,
          revision: state.revision,
          membership: encodeGroupMembership(members),
          verification,
          history,
          authorEnvelope,
        };
        const signature = await this.sign(
          device,
          spaceSuccessorBytes(application, state.root, record),
        );
        this.assertOpen();
        const { id, ...values } = record;
        return tx.insert(this.tables.__e2ee_space_successors, { ...values, signature }, { id });
      });
      const accepted = await proposal.wait({ tier: "global" });
      this.assertOpen();
      await this.deliver(
        address,
        { ...expected, epochId: accepted.epochId, verification: accepted.verification },
        secret,
        device,
      );
    } finally {
      secret.fill(0);
    }
  }

  private async confirmHistory(
    snapshot: Snapshot,
    root: SpaceRoot,
    payload: Uint8Array,
    use?: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
  ) {
    let state = await this.validate(snapshot, root);
    // Only the candidate's current key can be unavailable. Once it is confirmed,
    // predecessor processing and historical replay failures belong to the operation.
    const currentContext = spaceContext(
      this.accountContext(root.accountId),
      state.keyRoot,
      "verification",
    );
    await this.confirm(state.keyRoot, payload, currentContext).catch(unavailableSpaceKey);
    let secret = payload;
    let owned: Uint8Array | undefined;
    try {
      for (;;) {
        await use?.(secret, state.keyRoot);
        if (!state.successor) return;
        const successor = state.successor;
        const previous = await this.keys.unwrap(
          secret,
          spaceSuccessorContext(this.accountContext(root.accountId), root, successor, "history"),
          successor.history,
        );
        owned?.fill(0);
        owned = previous;
        secret = previous;
        state = await this.validate(snapshot, root, state.epochPosition);
        if (state.keyRoot.epochId !== successor.predecessor)
          throw new Error("Invalid E2EE space predecessor history");
        await this.confirm(state.keyRoot, secret);
      }
    } finally {
      owned?.fill(0);
    }
  }

  private async acceptedDelivery(
    snapshot: Snapshot,
    root: SpaceRoot,
    rootPosition: string,
    row: SpaceDelivery | SpaceRecoveryDelivery,
    position: string | undefined,
  ): Promise<boolean> {
    if (!position || BigInt(position) <= BigInt(rootPosition) || row.spaceId !== root.id)
      return false;
    const cut = BigInt(position);
    const { members, keyRoot, rotationRequired } = await this.validate(snapshot, root, cut);
    if (
      rotationRequired ||
      row.epochId !== keyRoot.epochId ||
      (members.get(row.senderAccountId) !== row.senderEpochId &&
        !isInitialAuthor(
          root,
          keyRoot.epochId,
          row.senderAccountId,
          row.senderDeviceId,
          row.senderEpochId,
        )) ||
      members.get(row.recipientAccountId) !== row.recipientEpochId
    )
      return false;
    const senderHistory = snapshot.histories.get(row.senderAccountId);
    const recipientHistory = snapshot.histories.get(row.recipientAccountId);
    if (!senderHistory || !recipientHistory) return false;
    const history = historyBefore(senderHistory, cut);
    const sender = await replayAccountMembership(
      history,
      this.accountContext(row.senderAccountId),
      this.signer,
    );
    const recipient = await replayAccountMembership(
      historyBefore(recipientHistory, cut),
      this.accountContext(row.recipientAccountId),
      this.signer,
    );
    if (
      sender.epochId !== row.senderEpochId ||
      recipient.epochId !== row.recipientEpochId ||
      !sender.active.has(row.senderDeviceId)
    )
      return false;
    if ("recoveryRootId" in row) {
      const recovery = recipient.recoveryRoots.find((root) => root.id === row.recoveryRootId);
      if (
        !recovery ||
        recovery.mechanism !== this.keys.mechanism.id ||
        recovery.version !== this.keys.mechanism.version
      )
        return false;
    } else if (!recipient.active.has(row.recipientDeviceId)) return false;
    let bytes: Uint8Array;
    try {
      bytes =
        "recoveryRootId" in row
          ? spaceRecoveryBytes(this.accountContext(root.accountId), keyRoot, row)
          : spaceDeliveryBytes(this.accountContext(root.accountId), keyRoot, row);
    } catch {
      return false;
    }
    return this.verify(history, row.senderDeviceId, bytes, row.signature);
  }

  private async deliver(
    address: Address,
    expected: SpaceRoot,
    secret: Uint8Array,
    device: LocalDevice,
  ) {
    const delivery = await exclusiveE2eeTransaction(this.db, async (tx) => {
      const snapshot = await this.readSnapshot(tx, address, expected.id);
      const { root, keyRoot, members, rotationRequired, position } = await this.validate(
        snapshot,
        address,
      );
      if (
        root.id !== expected.id ||
        keyRoot.epochId !== expected.epochId ||
        !snapshot.state.active.has(device.id) ||
        rotationRequired ||
        (snapshot.state.epochId !== members.get(this.accountId) &&
          !isInitialAuthor(
            root,
            keyRoot.epochId,
            this.accountId,
            device.id,
            snapshot.state.epochId,
          ))
      )
        throw new Error("E2EE space membership requires reconciliation");
      await this.confirmHistory(snapshot, root, secret);
      const delivered = new Set<string>();
      for (const row of snapshot.deliveries.rows) {
        if (
          row.epochId === keyRoot.epochId &&
          (await this.acceptedDelivery(
            snapshot,
            root,
            position,
            row,
            positionOf(snapshot.deliveries, row.id),
          ))
        )
          delivered.add(JSON.stringify([row.recipientAccountId, row.recipientDeviceId]));
      }
      for (const [recipientAccountId, recipientEpochId] of members) {
        const history = snapshot.histories.get(recipientAccountId)!;
        const recipients = await replayAccountMembership(
          history,
          this.accountContext(recipientAccountId),
          this.signer,
        );
        if (recipients.epochId !== recipientEpochId)
          throw new Error("E2EE space recipient epoch changed");
        if (recipients.recoveryRoots.length) {
          const recovery = await tx.allSettledForE2ee(
            this.tables.__e2ee_space_recovery_deliveries.where({
              spaceId: root.id,
              recipientAccountId,
            }),
          );
          for (const recoveryRoot of recipients.recoveryRoots) {
            if (
              recoveryRoot.mechanism !== this.keys.mechanism.id ||
              recoveryRoot.version !== this.keys.mechanism.version
            )
              throw new Error("Unsupported E2EE space recovery recipient");
            let present = false;
            for (const row of recovery.rows) {
              if (
                row.epochId === keyRoot.epochId &&
                row.recoveryRootId === recoveryRoot.id &&
                (await this.acceptedDelivery(
                  snapshot,
                  root,
                  position,
                  row,
                  positionOf(recovery, row.id),
                ))
              ) {
                present = true;
                break;
              }
            }
            if (present) continue;
            const row = {
              id: crypto.randomUUID(),
              spaceId: root.id,
              epochId: keyRoot.epochId,
              senderAccountId: this.accountId,
              senderDeviceId: device.id,
              senderEpochId: snapshot.state.epochId,
              recipientAccountId,
              recipientEpochId,
              recoveryRootId: recoveryRoot.id,
            };
            const envelope = await this.keys.seal(
              recoveryRoot.publicKey,
              spaceRecoveryContext(this.accountContext(root.accountId), keyRoot, row),
              secret,
            );
            const signature = await this.sign(
              device,
              spaceRecoveryBytes(this.accountContext(root.accountId), keyRoot, {
                ...row,
                envelope,
              }),
            );
            this.assertOpen();
            const { id, ...values } = row;
            tx.insert(
              this.tables.__e2ee_space_recovery_deliveries,
              { ...values, envelope, signature },
              { id },
            );
          }
        }
        for (const recipientDeviceId of recipients.active) {
          if (delivered.has(JSON.stringify([recipientAccountId, recipientDeviceId]))) continue;
          const key = history.keys.rows.find((row) => row.deviceId === recipientDeviceId);
          if (
            !key ||
            key.mechanism !== this.keys.mechanism.id ||
            key.version !== this.keys.mechanism.version
          )
            throw new Error("Unsupported E2EE space recipient key");
          const row = {
            id: crypto.randomUUID(),
            spaceId: root.id,
            epochId: keyRoot.epochId,
            senderAccountId: this.accountId,
            senderDeviceId: device.id,
            senderEpochId: snapshot.state.epochId,
            recipientAccountId,
            recipientDeviceId,
            recipientEpochId: recipients.epochId,
          };
          const envelope = await this.keys.seal(
            key.publicKey,
            spaceDeliveryContext(this.accountContext(root.accountId), keyRoot, row),
            secret,
          );
          const signature = await this.sign(
            device,
            spaceDeliveryBytes(this.accountContext(root.accountId), keyRoot, { ...row, envelope }),
          );
          this.assertOpen();
          const { id, ...values } = row;
          tx.insert(
            this.tables.__e2ee_space_deliveries,
            { ...values, envelope, signature },
            { id },
          );
        }
      }
    });
    await delivery.wait({ tier: "global" });
    this.assertOpen();
  }

  private async confirm(
    root: Pick<
      SpaceRoot,
      | "id"
      | "scopeId"
      | "identifier"
      | "epochId"
      | "accountId"
      | "mechanism"
      | "version"
      | "verification"
    >,
    secret: Uint8Array,
    context?: Uint8Array,
  ) {
    if (
      secret.length !== 32 ||
      root.mechanism !== this.keys.mechanism.id ||
      root.version !== this.keys.mechanism.version
    )
      throw new Error("Invalid or unsupported E2EE space key");
    const opened = await this.keys.unwrap(
      secret,
      context ?? spaceContext(this.accountContext(root.accountId), root, "verification"),
      root.verification,
    );
    try {
      if (opened.length !== 32 || opened.some((byte) => byte !== 0))
        throw new Error("Invalid E2EE space key confirmation");
    } finally {
      opened.fill(0);
    }
  }
  private async sign(device: LocalDevice, bytes: Uint8Array): Promise<Uint8Array> {
    const signature = await this.signer.sign(device.signing.privateKey, bytes);
    if (!(await this.signer.verify(device.signing.publicKey, bytes, signature)))
      throw new Error("Invalid generated E2EE space signature");
    return signature;
  }
  private async verify(
    history: PublicMembershipHistory,
    deviceId: string,
    bytes: Uint8Array,
    signature: Uint8Array,
  ): Promise<boolean> {
    const key = history.keys.rows.find((row) => row.deviceId === deviceId);
    if (
      !key ||
      key.signingMechanism !== this.signer.mechanism.id ||
      key.signingVersion !== this.signer.mechanism.version
    )
      return false;
    return this.signer.verify(key.signingPublicKey, bytes, signature);
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, i) => byte === right[i]);
}
