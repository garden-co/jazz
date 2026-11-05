import { usesSpaceAdministration } from "./encrypted-schema.js";
import { sha256 } from "@noble/hashes/sha2.js";
import {
  maintainSpaceMember,
  initialisePermissionSpace,
  initialiseSpaceAdministration,
} from "./current-membership.js";
import {
  beginDbTransactionAfter,
  serializableE2eeTransaction,
  insertInitializationRow,
  prepareDbTransaction,
} from "../runtime/db.js";
import type { Db, TableProxy, E2eeTransactionScope, QueryBuilder } from "../runtime/db.js";
import {
  observeE2eeHistory,
  E2eeHistoryUnavailable,
  type E2eeHistoryReader,
} from "./history-reader.js";
import type { RowSettlement } from "../runtime/client.js";
import type { AccountStore } from "../accounts/persistence.js";
import { sameSnapshotValue } from "./public-snapshot.js";
import { decodeRecoveryMaterial, type DecodedRecoveryMaterial } from "./recovery-format.js";
import {
  readAcceptedHistory,
  prepareAcceptedHistory,
  retainLocalAcceptedHistory,
  RetainedHistoryUnavailableError,
} from "./accepted-history.js";
import { E2eeRecoveryError, RecoveryCandidateError } from "./recovery-error.js";
import { E2eeDataError } from "./data-error.js";
import { InitializationJournal, E2eeInitializationNotReady } from "./provisional-initialization.js";
import type { FounderProposal } from "./provisional-initialization.js";
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
  verified: Set<string>;
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
const recoverySnapshotBatchSize = 8;

function isTransactionConflict(error: unknown): boolean {
  return (
    (error instanceof PersistedWriteRejectedError &&
      (error.code === "exclusive_conflict" ||
        error.code === "transaction_conflict" ||
        error.code === "serialization_conflict")) ||
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

/** @internal Only standalone writes may retry with a fresh exclusive transaction. */
export class SpaceInitialisationRequired extends Error {
  constructor() {
    super(
      "Initialising an encryption space requires beginSerializableTransaction() or serializableTransaction()",
    );
    this.name = "SpaceInitialisationRequired";
  }
}
/** @internal Standalone captured-value writes may retry before any encryption or mutation. */
export class ProvisionalSpaceRequiresExclusive extends SpaceInitialisationRequired {
  readonly code = "e2ee_provisional_requires_exclusive";
  readonly retryable = true;
  constructor() {
    super();
    this.name = "ProvisionalSpaceRequiresExclusive";
    this.message = "Provisional encrypted writes require an exclusive transaction";
  }
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
type ReconciliationAttempt = { cancelled: boolean };
class SupersededReconciliation extends Error {}

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
  accountId === root.creatorAccountId &&
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

/** Opaque, Db-bound unpublished key ownership. No key is exposed on this token. */
export type InitialSpaceSeed = Readonly<{ readonly kind: "initial-space-seed" }>;
export type StreamingSpaceOptions = {
  newScope?: boolean;
  initialRecipients?: readonly string[];
  requireAccepted?: boolean;
};
export type StreamingSpacePlan = {
  readonly initialSeed?: InitialSpaceSeed;
  validate(tx: E2eeTransactionScope): Promise<void>;
  dispose(): void;
};
type InitialRecipient = Pick<SpaceGrant, "recipientId" | "recipientKind" | "recipientEpochId">;
type SeedState = {
  owner: Spaces;
  schema: TableProxy<unknown, unknown>["_schema"];
  table: string;
  secret: Uint8Array;
  root: SpaceRoot;
  grants: SpaceGrant[];
  recipients: string[];
  expectation: unknown;
  used: boolean;
  disposed: boolean;
};
const initialSeeds = new WeakMap<InitialSpaceSeed, SeedState>();

/** Scoped roots and accepted-only device delivery. Jazz policies own every write. */
export class Spaces {
  private readonly knownAddresses = new Map<string, Address>();
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
      states(tx?: E2eeHistoryReader): Promise<DeviceState>;
      journal?: InitializationJournal;
      founder?(): Promise<FounderProposal | undefined>;
    },
    private readonly groups?: Groups,
    private readonly staleWrites: "warn" | "reject" = "warn",
  ) {
    if (device)
      db.onE2eeReconnect(() => {
        for (const address of this.knownAddresses.values()) this.reconcileInBackground(address);
      });
  }

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

  async assertWriteTransaction<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    kind: "mergeable" | "serializable",
    requireAccepted = false,
  ): Promise<void> {
    const address = await this.address(scope, identifier, true);
    if (requireAccepted) {
      const roots = this.tables.__e2ee_spaces.where({ id: spaceRootId(address) });
      const accepted = () =>
        observeE2eeHistory(this.db, (reader) => reader.allSettledForE2ee(roots));
      let snapshot = await accepted();
      if (!snapshot.rows.length && !(await this.db.e2eeIsExplicitlyOffline())) {
        await this.db.one(roots, { tier: "remote" });
        snapshot = await accepted();
      }
      if (!snapshot.rows.length) throw new E2eeDataError("space-not-initialised");
    }
    const pending = await this.device?.journal?.space(spaceRootId(address));
    if (pending && kind !== "serializable") throw new ProvisionalSpaceRequiresExclusive();
  }

  private async address<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    localOnly = false,
  ): Promise<Address> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(identifier))
      throw new Error("Invalid E2EE space identifier");
    const scopeId = await this.db.tableIdentity(scope, localOnly);
    if (!scopeId) throw new Error("E2EE scope is unavailable");
    this.assertOpen();
    const address = { scopeId, identifier };
    this.knownAddresses.set(JSON.stringify([scopeId, identifier]), address);
    return address;
  }

  private async discoverRecoveryRoots(requireActiveDevice = false): Promise<{
    own: PublicMembershipHistory;
    roots: SpaceRoot[];
    device?: DeviceState;
  }> {
    await prefetchPublicMembershipHistory(this.db, this.accountId, this.tables);
    await this.groups?.warmMembership({ accountId: this.accountId });
    const read = await serializableE2eeTransaction(this.db, async (tx) => {
      const device = requireActiveDevice ? await this.requireDevice().states(tx) : undefined;
      const own =
        device?.publicHistory ??
        (await readPublicMembershipHistory(tx, this.accountId, this.tables));
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
      return { own, roots, device };
    });
    const { own, roots, device } = await read.wait({ tier: "global" });
    this.assertOpen();
    if (requireActiveDevice && !device?.active.has(this.requireDevice().id))
      throw new Error("Space recovery requires an active device");
    // Discovery is a candidate superset. Fresh per-path replay establishes entitlement.
    return { own, device, roots: roots.filter((root) => root.id === spaceRootId(root)) };
  }

  // At most eight targets share an accepted read-only authority transaction.
  // Exact predicates retain their coverage, including empty results. No keys
  // are opened before acceptance; secrets are handled one at a time afterward.
  // Maintenance/publication still uses fresh authority. No read plan survives
  // the batch, and no cached observation authorises a later write.
  private async *recoverySnapshots(roots: SpaceRoot[], recoveryRootId?: string, restore = false) {
    for (let offset = 0; offset < roots.length; offset += recoverySnapshotBatchSize) {
      const observed = roots.slice(offset, offset + recoverySnapshotBatchSize);
      const ids = observed.map((root) => root.id);
      const selector = ids.length === 1 ? { spaceId: ids[0]! } : { spaceId: { in: ids } };
      const [grants, successors] = await Promise.all([
        this.db.all(this.tables.__e2ee_space_grants.where(selector), { tier: "remote" }),
        this.db.all(this.tables.__e2ee_space_successors.where(selector), { tier: "remote" }),
      ]);
      this.assertOpen();
      const groupIds = this.groupHistoryIds(grants, []);
      if (groupIds !== undefined) await this.groups?.warmMembership({ groupIds });
      await Promise.all(
        [
          ...this.accounts(
            observed,
            grants,
            successors.map((row) => row.authorAccountId),
          ),
        ].map((id) => prefetchPublicMembershipHistory(this.db, id, this.tables)),
      );
      const query =
        recoveryRootId === undefined
          ? undefined
          : this.tables.__e2ee_space_recovery_deliveries.where({
              ...selector,
              recipientAccountId: this.accountId,
              recoveryRootId,
            });
      if (query) await this.db.all(query, { tier: "remote" });
      const read = await serializableE2eeTransaction(this.db, async (tx) => {
        const device = restore ? await this.requireDevice().states(tx) : undefined;
        const own =
          device?.publicHistory ??
          (await readPublicMembershipHistory(tx, this.accountId, this.tables));
        const reads = new Map<string, Promise<Settled<{ id: string }>>>();
        const reader: E2eeTransactionScope = {
          kind: tx.kind,
          all: tx.all.bind(tx),
          one: tx.one.bind(tx),
          insert: tx.insert.bind(tx),
          upsert: tx.upsert.bind(tx),
          allSettledForE2ee<T extends { id: string }>(query: QueryBuilder<T>): Promise<Settled<T>> {
            const key = query._build();
            let result = reads.get(key);
            if (!result) {
              result = tx.allSettledForE2ee(query);
              reads.set(key, result);
            }
            return result as Promise<Settled<T>>;
          },
        };
        const [roots, grants, successors] = await Promise.all([
          tx.allSettledForE2ee(this.tables.__e2ee_spaces.where({ id: { in: ids } })),
          tx.allSettledForE2ee(this.tables.__e2ee_space_grants.where(selector)),
          tx.allSettledForE2ee(this.tables.__e2ee_space_successors.where(selector)),
        ]);
        const select = <T extends { id: string }>(
          records: Settled<T>,
          matches: (row: T) => boolean,
        ): Settled<T> => {
          const rows = records.rows.filter(matches);
          const ids = new Set(rows.map((row) => row.id));
          return { rows, settlements: records.settlements.filter((row) => ids.has(row.rowId)) };
        };
        const spaces: { observed: SpaceRoot; snapshot: Snapshot }[] = [];
        for (const root of observed) {
          const records = {
            roots: select(roots, (row) => row.id === root.id),
            grants: select(grants, (row) => row.spaceId === root.id),
            successors: select(successors, (row) => row.spaceId === root.id),
          };
          spaces.push({
            observed: root,
            snapshot: await this.readRecords(reader, root, root.id, own, [], false, records),
          });
        }
        const recovery: Settled<SpaceRecoveryDelivery> = query
          ? await tx.allSettledForE2ee(query)
          : { rows: [], settlements: [] };
        return { own, device, spaces, recovery };
      });
      const accepted = await read.wait({ tier: "global" });
      this.assertOpen();
      yield accepted;
    }
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
      for await (const batch of this.recoverySnapshots(discovery.roots, material.rootId)) {
        if ((await this.checkRecoveryAuthority(material, batch.own)).epochId !== accountEpochId)
          throw new E2eeRecoveryError("recovery-state-changed");
        for (const { observed, snapshot } of batch.spaces) {
          this.assertOpen();
          const recovery = {
            rows: batch.recovery.rows.filter((row) => row.spaceId === observed.id),
            settlements: batch.recovery.settlements,
          };
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
        this.accountContext(root.creatorAccountId),
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
    const { roots, device } = await this.discoverRecoveryRoots(true);
    if (!device) throw new Error("Space recovery requires an active device");
    const required = new Set<string>();
    for await (const batch of this.recoverySnapshots(roots)) {
      // Record every required path before running maintenance. A later removal
      // cannot silently shrink creation's required coverage.
      const eligible: SpaceRoot[] = [];
      for (const { observed, snapshot } of batch.spaces) {
        const state = await this.recoveryState(snapshot, observed);
        if (!state || state.sealed || !state.members.has(this.accountId)) continue;
        required.add(observed.id);
        eligible.push(observed);
      }
      for (const observed of eligible) {
        if ((await this.explainAddress(observed)).state !== "ready")
          throw new Error("Space key unavailable while creating recovery");
      }
    }
    // Ready creators need proof of recovery coverage, not local recovery-key
    // restaging or another delivery/maintenance pass.
    const paths = await this.inspectRecovery(material, device.epochId);
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
      for await (const batch of this.recoverySnapshots(roots, material.rootId, true)) {
        if (!batch.device?.active.has(device.id))
          throw new Error("Space recovery requires an active device");
        await this.checkRecoveryAuthority(material, batch.own);
        for (const { observed, snapshot } of batch.spaces) {
          this.assertOpen();
          const recovery = {
            rows: batch.recovery.rows.filter((row) => row.spaceId === observed.id),
            settlements: batch.recovery.settlements,
          };
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
          if (result.state === "refused" && result.reason === "device-not-active")
            throw new Error("Space recovery requires an active device");
          if (result.state !== "ready" && result.state !== "refused")
            throw new Error("Recovered space requires maintenance");
        }
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
    return this.foregroundMaintenance(address, async () => {
      const roots = this.tables.__e2ee_spaces.where({ id: spaceRootId(address) });
      const observed = await this.db.one(roots, { tier: "remote" });
      if (observed) {
        await this.changeRecipient(scope._table, address, observed, recipientId, "add");
        await this.explain(scope, identifier);
        return;
      }
      // A reopened client may not have the existing scope row locally yet.
      await this.db.one(
        new TypedTableQueryBuilder(scope._table, scope._schema)
          .where({ id: identifier })
          .select("id"),
        { tier: "remote" },
      );
      await this.warmInitialRecipients([recipientId]);
      const device = await this.loadDevice();
      try {
        const secret = runtimeRandomBytes(32);
        try {
          const proposal = await serializableE2eeTransaction(this.db, async (tx) => {
            return this.stageInitial(tx, scope, identifier, [recipientId], address, device, secret);
          });
          // Global acceptance includes the journal's initial recipient handoff.
          await proposal.wait({ tier: "global" });
          this.assertOpen();
          await this.explain(scope, identifier);
        } finally {
          secret.fill(0);
        }
      } finally {
        this.requireDevice().release(device);
      }
    });
  }

  async warmInitialRecipients(recipientIds: readonly string[] = [this.accountId]): Promise<void> {
    const ids = initialRecipientIds(recipientIds);
    const founder = await this.device?.founder?.();
    if (founder) {
      await this.provisionalRecipients(founder, ids);
      return;
    }
    if (ids.every((id) => id === this.accountId)) return;
    const device = await this.loadDevice();
    try {
      await observeE2eeHistory(this.db, (reader) =>
        this.acceptedInitialEligibility(reader, device.id, ids),
      );
      return;
    } catch (error) {
      if (
        !(error instanceof E2eeHistoryUnavailable) &&
        !(error instanceof E2eeInitializationNotReady)
      )
        throw error;
      if (await this.db.e2eeIsExplicitlyOffline())
        throw new E2eeInitializationNotReady("An explicit recipient has no accepted local keys");
    } finally {
      this.requireDevice().release(device);
    }
    for (const recipientId of ids)
      if (recipientId !== this.accountId)
        await prefetchPublicMembershipHistory(this.db, recipientId, this.tables);
    await this.groups?.warmMembership({ groupIds: ids });
  }

  private async provisionalRecipients(founder: FounderProposal, recipientIds: readonly string[]) {
    const ids = initialRecipientIds(recipientIds);
    const self: InitialRecipient = {
      recipientId: this.accountId,
      recipientKind: "account",
      recipientEpochId: founder.epochId,
    };
    if (ids.every((id) => id === this.accountId)) return [self];
    try {
      return await observeE2eeHistory(this.db, async (reader) => {
        const own = await readPublicMembershipHistory(reader, this.accountId, this.tables);
        const snapshot = await this.groups?.readMembership(reader, { groupIds: ids }, own);
        const graph = snapshot && (await this.groups!.acceptedGraph(snapshot));
        const recipients: InitialRecipient[] = [];
        for (const recipientId of ids) {
          if (recipientId === this.accountId) {
            recipients.push(self);
            continue;
          }
          const history = await readPublicMembershipHistory(reader, recipientId, this.tables);
          const group = graph?.get(recipientId);
          if (group && history.roots.rows.length)
            throw new Error("Ambiguous E2EE space recipient ID");
          if (group) {
            if (group.sealed || group.rotationRequired)
              throw new E2eeInitializationNotReady(
                "The explicit recipient group requires reconciliation",
              );
            recipients.push({
              recipientId,
              recipientKind: "group",
              recipientEpochId: group.keyRoot.epochId,
            });
          } else {
            if (!history.roots.rows.length)
              throw new E2eeInitializationNotReady(
                "An explicit recipient has no accepted local keys",
              );
            const account = await replayAccountMembership(
              history,
              this.accountContext(recipientId),
              this.signer,
            );
            recipients.push({
              recipientId,
              recipientKind: "account",
              recipientEpochId: account.epochId,
            });
          }
        }
        return recipients;
      });
    } catch (error) {
      if (error instanceof E2eeHistoryUnavailable)
        throw new E2eeInitializationNotReady("An explicit recipient has no accepted local keys");
      throw error;
    }
  }

  /** Recover acceptance-before-promotion without requiring a live authority connection. */
  async promoteInitial(expected: SpaceRoot): Promise<void> {
    await retainLocalAcceptedHistory(
      this.db,
      JSON.stringify(["space", expected.scopeId, expected.identifier]),
      (reader) => this.readSnapshot(reader, expected, expected.id),
      async (snapshot) => {
        const state = await this.validate(snapshot, expected);
        if (!sameSnapshotValue(state.root, expected)) throw new E2eeDataError("key-unavailable");
        this.assertOpen();
      },
    );
  }

  /** Accepted original-epoch handoff only; this never rotates or grants membership. */
  async completeInitial(expected: SpaceRoot): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      try {
        await this.completeInitialAttempt(expected);
        return;
      } catch (error) {
        // Retry only rejected maintenance, never the original initialization or
        // its source. Each attempt reopens keys and revalidates accepted history.
        const conflict = isTransactionConflict(error);
        if (!conflict || attempt >= 2) throw error;
      }
    }
  }

  private async completeInitialAttempt(expected: SpaceRoot): Promise<void> {
    this.assertOpen();
    await this.warm(expected);
    const device = await this.loadDevice();
    try {
      // Open the author key under the same authority snapshot as its delivery.
      await this.deliver(expected, expected, undefined, device);
      this.assertOpen();
      // Delivery adds accepted metadata after the preparation snapshot.
      // Retain that coherent state before callers can immediately go offline.
      await this.readAcceptedSnapshot(expected, expected.id);
    } finally {
      this.requireDevice().release(device);
    }
  }

  /** An unavailable key alone never establishes that a space is absent. */
  async prepareMissing<T, Init>(
    tx: E2eeTransactionScope | undefined,
    scope: TableProxy<T, Init>,
    identifier: string,
    prepareData: Parameters<Spaces["prepareInitial"]>[3],
    seed?: InitialSpaceSeed,
  ): Promise<boolean> {
    const retainedScopeId = await this.db.authenticatedTableIdentity(scope);
    const address = await this.address(scope, identifier, retainedScopeId !== null);
    const roots = this.tables.__e2ee_spaces.where(address);
    if (tx) {
      if (await tx.one(roots.includeDeleted(), { tier: "local-first" })) return false;
      // Hydrate a pre-existing scope row in this transaction, not in a later snapshot.
      const scopeRow = new TypedTableQueryBuilder(scope._table, scope._schema)
        .where({ id: identifier })
        .select("id");
      if (
        !(await tx.one(scopeRow, { tier: "local-first" })) &&
        !(await this.db.e2eeIsExplicitlyOffline())
      )
        await tx.one(scopeRow, { tier: "remote" });
      await this.prepareInitial(tx, scope, identifier, prepareData, undefined, seed);
      return true;
    }
    if (await this.db.one(roots.includeDeleted(), { tier: "local-first" })) return false;
    // This read only chooses a retry. The fresh exclusive transaction must
    // independently validate absence and accept root, grant and data together.
    throw new SpaceInitialisationRequired();
  }

  /** Internal preparation only: the caller owns the enclosing transaction and acceptance. */
  async prepareInitial<T, Init>(
    tx: E2eeTransactionScope,
    scope: TableProxy<T, Init>,
    identifier: string,
    prepareData: SpaceDataPreparation,
    recipientIds?: readonly string[],
    seed?: InitialSpaceSeed,
  ): Promise<void> {
    if (tx.kind !== "serializable")
      throw new Error("E2EE initialisation requires an exclusive transaction");
    const retainedScopeId = await this.db.authenticatedTableIdentity(scope);
    const address = await this.address(scope, identifier, retainedScopeId !== null);
    const pending = seed && initialSeeds.get(seed);
    if (
      seed &&
      (!pending ||
        pending.owner !== this ||
        pending.used ||
        pending.disposed ||
        pending.schema !== scope._schema ||
        pending.table !== scope._table ||
        pending.root.scopeId !== address.scopeId ||
        pending.root.identifier !== identifier)
    )
      throw new E2eeDataError("key-unavailable");
    const device = await this.loadDevice();
    try {
      const secret = pending?.secret ?? runtimeRandomBytes(32);
      try {
        const root = await this.stageInitial(
          tx,
          scope,
          identifier,
          recipientIds ?? pending?.recipients ?? [this.accountId],
          address,
          device,
          secret,
          seed,
        );
        await prepareData(secret, root, tx);
        this.assertOpen();
      } finally {
        if (pending) pending.disposed = true;
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
    seed?: InitialSpaceSeed,
  ): Promise<SpaceRoot> {
    const rootId = spaceRootId(address);
    const roots = this.tables.__e2ee_spaces.where({ id: rootId });
    if (
      await tx.one(this.tables.__e2ee_spaces.includeDeleted().where({ id: rootId }), {
        tier: "local-first",
      })
    )
      throw new Error("E2EE space already exists");
    const pending = seed && initialSeeds.get(seed);
    const recipientsToUse = pending?.recipients ?? initialRecipientIds(recipientIds);
    if (
      usesSpaceAdministration(scope._schema, scope._table) &&
      (recipientsToUse.length !== 1 || recipientsToUse[0] !== this.accountId)
    )
      throw new Error(
        "Explicit initial recipients are not supported with space administration helpers yet; create the space for its creator, then grant recipients",
      );
    const eligibility = await this.initialEligibility(tx, device.id, recipientsToUse);
    if (pending) {
      if (
        pending.owner !== this ||
        pending.used ||
        pending.disposed ||
        pending.root.scopeId !== address.scopeId ||
        pending.root.identifier !== identifier ||
        !sameSnapshotValue(pending.expectation, eligibility.expectation)
      )
        throw new E2eeDataError("key-unavailable");
      if (
        recipientIds !== undefined &&
        !sameSnapshotValue(initialRecipientIds(recipientIds).sort(), [...pending.recipients].sort())
      )
        throw new E2eeDataError("key-unavailable");
    }
    const scopeRow = new TypedTableQueryBuilder(scope._table, scope._schema)
      .where({ id: identifier })
      .select("id");
    if (!(await tx.one(scopeRow, { tier: "local-first" })))
      throw new Error("Explicit space initialisation requires an existing scope row");
    if (await tx.one(roots, { tier: "local-first" })) throw new Error("E2EE space already exists");
    const { root: signedRoot, grants: initialGrants } =
      pending ??
      (await this.initialRecords(
        address,
        device,
        secret,
        eligibility.state,
        eligibility.recipients,
      ));
    if (pending) pending.used = true;
    const journal = this.requireDevice().journal;
    if (!journal) throw new Error("Automatic initialization requires its private journal");
    this.assertOpen();
    const { id, ...values } = signedRoot;
    // Exact absence is captured before INSERT, never via overwrite semantics.
    await insertInitializationRow(tx, this.tables.__e2ee_spaces, values, { id });
    for (const { id: grantId, ...grantValues } of initialGrants)
      await insertInitializationRow(tx, this.tables.__e2ee_space_grants, grantValues, {
        id: grantId,
      });
    initialisePermissionSpace(tx, scope._schema, scope._table, identifier);
    initialiseSpaceAdministration(
      tx,
      scope._schema,
      scope._table,
      id,
      identifier,
      signedRoot.creatorAccountId,
    );
    for (const grant of initialGrants)
      maintainSpaceMember(
        tx,
        scope._schema,
        scope._table,
        address.scopeId,
        identifier,
        grant.recipientKind as "account" | "group",
        grant.recipientId,
        "add",
      );
    journal.stage(tx, { kind: "space", id, root: signedRoot, grants: initialGrants });
    return signedRoot;
  }

  private async initialEligibility(
    tx: E2eeTransactionScope,
    deviceId: string,
    recipientIds: readonly string[],
  ) {
    const founder = await this.device?.founder?.();
    if (founder) {
      if (founder.deviceId !== deviceId) throw new E2eeDataError("key-unavailable");
      const identity = await tx.one(
        this.tables.__e2ee_account_identities.where({ id: this.accountId }),
        { tier: "local-first" },
      );
      const root = await tx.one(this.tables.__e2ee_account_roots.where({ id: founder.rootId }), {
        tier: "local-first",
      });
      const keys = await tx.one(this.tables.__e2ee_device_keys.where({ id: founder.publicKeyId }), {
        tier: "local-first",
      });
      if (
        identity?.deviceId !== deviceId ||
        identity.epochId !== founder.epochId ||
        root?.deviceId !== deviceId ||
        root.epochId !== founder.epochId ||
        keys?.deviceId !== deviceId
      )
        throw new E2eeInitializationNotReady("The exact founder proposal is not locally visible");
      const recipients = await this.provisionalRecipients(founder, recipientIds);
      return {
        state: { epochId: founder.epochId },
        recipients,
        expectation: { founder: founder.id, deviceId, epochId: founder.epochId, recipients },
      };
    }
    // Recipient eligibility belongs to the public transaction's fixed snapshot.
    // External recipients need online coverage at that coordinate. Offline
    // proposals use only accepted local rows from that same snapshot.
    const offline = await this.db.e2eeIsExplicitlyOffline();
    // A creator-only proposal does not introduce another recipient. Its accepted
    // local history must remain usable after transport loss, without requiring
    // an explicit disconnect. Keep live coverage for external recipients.
    const creatorOnly = initialRecipientIds(recipientIds).every((id) => id === this.accountId);
    const reader: E2eeHistoryReader = {
      allSettledForE2ee: (query) =>
        tx.allSettledForE2ee(query, offline || creatorOnly ? "local-only" : undefined, true),
    };
    try {
      return await this.acceptedInitialEligibility(reader, deviceId, recipientIds);
    } catch (error) {
      if (
        !offline ||
        (!(error instanceof E2eeHistoryUnavailable) &&
          !(error instanceof E2eeInitializationNotReady))
      )
        throw error;
      throw new E2eeInitializationNotReady("Initial recipient history is not locally available");
    }
  }

  private async acceptedInitialEligibility(
    tx: E2eeHistoryReader,
    deviceId: string,
    recipientIds: readonly string[],
  ) {
    const state = await this.requireDevice().states(tx);
    if (!state.active.has(deviceId)) throw new E2eeDataError("key-unavailable");
    const ids = initialRecipientIds(recipientIds);
    const groupSnapshot = await this.groups?.readMembership(
      tx,
      { groupIds: ids },
      state.publicHistory,
    );
    const graph = groupSnapshot && (await this.groups!.acceptedGraph(groupSnapshot));
    const recipients: InitialRecipient[] = [];
    const expectedRecipients: unknown[] = [];
    for (const recipientId of ids) {
      const history =
        recipientId === this.accountId
          ? state.publicHistory
          : await readPublicMembershipHistory(tx, recipientId, this.tables);
      const group = graph?.get(recipientId);
      if (group && history.roots.rows.length) throw new Error("Ambiguous E2EE space recipient ID");
      if (group) {
        if (group.sealed || group.rotationRequired)
          throw new Error("E2EE initial recipient group requires reconciliation");
        recipients.push({
          recipientId,
          recipientKind: "group",
          recipientEpochId: group.keyRoot.epochId,
        });
        expectedRecipients.push([
          recipientId,
          group.keyRoot.epochId,
          group.revision,
          group.members,
        ]);
      } else {
        if (!history.roots.rows.length)
          throw new E2eeInitializationNotReady("E2EE initial recipient account is unavailable");
        const recipient = await replayAccountMembership(
          history,
          this.accountContext(recipientId),
          this.signer,
        );
        recipients.push({
          recipientId,
          recipientKind: "account",
          recipientEpochId: recipient.epochId,
        });
        expectedRecipients.push([recipientId, recipient.epochId, recipient.active]);
      }
    }
    return {
      state,
      recipients,
      expectation: {
        accountId: this.accountId,
        deviceId,
        epochId: state.epochId,
        active: state.active,
        recipients: expectedRecipients,
      },
    };
  }

  private async initialRecords(
    address: Address,
    device: LocalDevice,
    secret: Uint8Array,
    state: { epochId: string },
    recipients: InitialRecipient[],
  ) {
    const root = {
      id: spaceRootId(address),
      ...address,
      creatorAccountId: this.accountId,
      deviceId: device.id,
      accountEpochId: state.epochId,
      epochId: crypto.randomUUID(),
      initialGrantId: crypto.randomUUID(),
      mechanism: this.keys.mechanism.id,
      version: this.keys.mechanism.version,
    };
    const application = this.accountContext(root.creatorAccountId);
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
      if (!sameBytes(opened, secret)) throw new Error("Invalid generated E2EE space envelope");
    } finally {
      opened.fill(0);
    }
    const signature = await this.sign(device, spaceRootBytes(application, completeRoot));
    const grants: SpaceGrant[] = [];
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
      grants.push({ ...grant, signature: grantSignature });
    }
    this.assertOpen();
    return { root: { ...completeRoot, signature }, grants };
  }

  /** Stage source bytes outside any transaction, then bind publication to its accepted history. */
  async prepareStreaming<T, Init, R>(
    scope: TableProxy<T, Init>,
    identifier: string,
    options: StreamingSpaceOptions,
    stage: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<R>,
  ): Promise<{ value: R; plan: StreamingSpacePlan }> {
    if (options.requireAccepted)
      await this.assertWriteTransaction(scope, identifier, "serializable", true);
    const retainedScopeId = await this.db.authenticatedTableIdentity(scope);
    const address = await this.address(scope, identifier, retainedScopeId !== null);
    const newScope = options.newScope === true;
    const rootId = spaceRootId(address);
    const recipients = initialRecipientIds(options.initialRecipients ?? [this.accountId]);
    await this.warmInitialRecipients(recipients);
    const roots = this.tables.__e2ee_spaces.where(address);
    let observed = await this.db.one(roots.includeDeleted(), { tier: "local-first" });
    const scopeRow = new TypedTableQueryBuilder(scope._table, scope._schema)
      .where({ id: identifier })
      .select("id");
    if (!newScope) {
      const localScope = await this.db.one(scopeRow, { tier: "local-first" });
      if (!observed || !localScope) {
        if (await this.db.e2eeIsExplicitlyOffline())
          throw new E2eeInitializationNotReady("Space history is not locally available");
        // A missing local root is not proof that a legacy scope is unencrypted.
        // Resolve missing facts before choosing keys or consuming source bytes.
        if (!observed) observed = await this.db.one(roots.includeDeleted(), { tier: "remote" });
        if (!localScope) await this.db.one(scopeRow, { tier: "remote" });
      }
    }
    const pendingSpace = await this.device?.journal?.space(rootId);
    if (observed && pendingSpace) {
      if (newScope) throw new E2eeDataError("key-unavailable");
      let value!: R;
      const result = await this.withKeys(
        scope,
        identifier,
        async (secret, root) => {
          value = await stage(secret, root);
        },
        false,
        undefined,
        true,
      );
      if (result.state !== "ready") throw new E2eeDataError("key-unavailable");
      let disposed = false;
      return {
        value,
        plan: {
          validate: async (tx) => {
            if (disposed) throw new E2eeDataError("key-unavailable");
            const root = await tx.one(this.tables.__e2ee_spaces.where({ id: rootId }), {
              tier: "local-first",
            });
            if (root?.epochId !== pendingSpace.root.epochId)
              throw new E2eeDataError("key-unavailable");
            const state = await this.withKeys(
              scope,
              identifier,
              async (_key, current) => {
                if (current.epochId !== pendingSpace.root.epochId)
                  throw new E2eeDataError("key-unavailable");
              },
              false,
              undefined,
              true,
            );
            if (state.state !== "ready") throw new E2eeDataError("key-unavailable");
          },
          dispose() {
            disposed = true;
          },
        },
      };
    }
    if (observed) {
      if (newScope) throw new E2eeDataError("key-unavailable");
      const device = await this.loadDevice();
      const deviceId = device.id;
      this.requireDevice().release(device);
      const expectation = async (snapshot: Snapshot & { state: DeviceState }) => {
        this.assertOpen();
        const state = await this.validate(snapshot, address);
        if (
          this.device?.isKnownRevoked() ||
          !snapshot.state.active.has(deviceId) ||
          !state.members.has(this.accountId) ||
          state.sealed
        )
          throw new E2eeDataError("key-unavailable");
        if (this.staleWrites !== "warn" && (state.rotationRequired || !state.groupsReady))
          throw new E2eeDataError("maintenance-required");
        return {
          epochId: state.keyRoot.epochId,
          accountId: this.accountId,
          deviceId,
          accountEpochId: snapshot.state.epochId,
          active: snapshot.state.active,
          members: state.members,
          revision: state.revision,
          rotationRequired: state.rotationRequired,
          groupsReady: state.groupsReady,
        };
      };
      let snapshot: Snapshot & { state: DeviceState };
      try {
        snapshot = await this.readAcceptedSnapshot(address, rootId, true);
      } catch (error) {
        if (
          !(error instanceof RetainedHistoryUnavailableError) ||
          this.device?.isKnownRevoked() ||
          (await this.db.e2eeIsExplicitlyOffline())
        )
          throw error;
        // Missing or changed retained coverage may be refreshed only before
        // consuming a source. A coherent local eligibility refusal is final.
        await this.warm(observed);
        snapshot = await this.readAcceptedSnapshot(address, rootId);
      }
      const expected = await expectation(snapshot);
      let disposed = false;
      const plan: StreamingSpacePlan = {
        validate: async (tx) => {
          if (disposed) throw new E2eeDataError("key-unavailable");
          const reader: E2eeHistoryReader = {
            allSettledForE2ee: (query) => tx.allSettledForE2ee(query, "local-only"),
          };
          // These remain accepted-row reads inside the publishing exclusive
          // snapshot, not retained rows substituted for transaction evidence.
          if (!(await reader.allSettledForE2ee(scopeRow)).rows.length)
            throw new E2eeDataError("key-unavailable");
          const current = await this.readSnapshot(reader, address, rootId, [], false);
          if (!sameSnapshotValue(expected, await expectation(current)))
            throw new E2eeDataError("key-unavailable");
        },
        dispose() {
          disposed = true;
        },
      };
      try {
        const preparation = beginDbTransactionAfter(this.db, async () => {});
        try {
          await prepareDbTransaction(preparation, (tx) => plan.validate(tx));
        } finally {
          await preparation.rollback();
        }
        let value!: R;
        let used = false;
        const state = await this.withKeys(
          scope,
          identifier,
          async (secret, root) => {
            if (used || root.epochId !== expected.epochId)
              throw new E2eeDataError("key-unavailable");
            used = true;
            // Key lookup may discover newer history before it calls us. Never
            // consume bytes against a different preflight expectation.
            const current = await this.readAcceptedSnapshot(address, rootId, true);
            if (!sameSnapshotValue(expected, await expectation(current)))
              throw new E2eeDataError("key-unavailable");
            value = await stage(secret, root);
          },
          false,
          undefined,
          true,
        );
        if (state.state !== "ready" || !used) throw new E2eeDataError("key-unavailable");
        return { value, plan };
      } catch (error) {
        plan.dispose();
        throw error;
      }
    }
    const device = await this.loadDevice();
    const secret = runtimeRandomBytes(32);
    let seedState: SeedState | undefined;
    try {
      const readInitial = async (tx: E2eeTransactionScope) => {
        this.assertOpen();
        if (
          (await tx.one(this.tables.__e2ee_spaces.includeDeleted().where({ id: rootId }), {
            tier: "local-first",
          })) ||
          (await tx.one(roots.includeDeleted(), { tier: "local-first" }))
        )
          throw new E2eeDataError("key-unavailable");
        if (!newScope && !(await tx.one(scopeRow, { tier: "local-first" })))
          throw new E2eeDataError("key-unavailable");
        return this.initialEligibility(tx, device.id, recipients);
      };
      const read = await serializableE2eeTransaction(this.db, readInitial);
      const eligibility = await read.wait({ tier: "local" });
      const records = await this.initialRecords(
        address,
        device,
        secret,
        eligibility.state,
        eligibility.recipients,
      );
      const initialSeed: InitialSpaceSeed = Object.freeze({ kind: "initial-space-seed" });
      const pending: SeedState = {
        owner: this,
        schema: scope._schema,
        table: scope._table,
        secret,
        ...records,
        recipients,
        expectation: structuredClone(eligibility.expectation),
        used: false,
        disposed: false,
      };
      seedState = pending;
      initialSeeds.set(initialSeed, pending);
      const plan: StreamingSpacePlan = {
        initialSeed,
        validate: async (tx) => {
          if (
            pending.used ||
            pending.disposed ||
            !sameSnapshotValue(pending.expectation, (await readInitial(tx)).expectation)
          )
            throw new E2eeDataError("key-unavailable");
        },
        dispose() {
          pending.disposed = true;
          pending.secret.fill(0);
        },
      };
      const value = await stage(secret, structuredClone(records.root));
      return { value, plan };
    } catch (error) {
      secret.fill(0);
      if (seedState) seedState.disposed = true;
      throw error;
    } finally {
      this.requireDevice().release(device);
    }
  }

  async revoke<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    recipientId: string,
  ): Promise<void> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(recipientId))
      throw new Error("Invalid E2EE space recipient");
    const address = await this.address(scope, identifier);
    return this.foregroundMaintenance(address, async () => {
      const root = await this.db.one(
        this.tables.__e2ee_spaces.where({ id: spaceRootId(address) }),
        { tier: "remote" },
      );
      if (!root) throw new Error("E2EE space not found");
      await this.changeRecipient(scope._table, address, root, recipientId, "remove");
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
    });
  }

  private async readSnapshot(
    tx: E2eeHistoryReader,
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
    tx: E2eeHistoryReader,
    address: Address,
    id: string,
    ownHistory: PublicMembershipHistory,
    extraAccounts: string[] = [],
    includeDeliveries = true,
    // Only supplied from the same recovery transaction. Its bounded
    // set predicates cover these per-space rows and absent-row evidence.
    prepared?: Pick<Snapshot, "roots" | "grants" | "successors">,
  ): Promise<Snapshot> {
    const [roots, grants, deliveries, successors] = await Promise.all([
      prepared?.roots ??
        tx.allSettledForE2ee(this.tables.__e2ee_spaces.where({ id: spaceRootId(address) })),
      prepared?.grants ??
        tx.allSettledForE2ee(this.tables.__e2ee_space_grants.where({ spaceId: id })),
      includeDeliveries
        ? tx.allSettledForE2ee(this.tables.__e2ee_space_deliveries.where({ spaceId: id }))
        : { rows: [], settlements: [] },
      prepared?.successors ??
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
        ...roots.map((row) => row.creatorAccountId),
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

  private async warm(
    observed: SpaceRoot,
    extraAccounts: string[] = [],
    reconciliation?: ReconciliationAttempt,
    includeDeliveries = true,
  ): Promise<void> {
    this.assertCurrentReconciliation(reconciliation);
    const [grants, successors] = await Promise.all([
      this.db.all(this.tables.__e2ee_space_grants.where({ spaceId: observed.id }), {
        tier: "remote",
      }),
      this.db.all(this.tables.__e2ee_space_successors.where({ spaceId: observed.id }), {
        tier: "remote",
      }),
      includeDeliveries
        ? this.db.all(this.tables.__e2ee_space_deliveries.where({ spaceId: observed.id }), {
            tier: "remote",
          })
        : undefined,
    ]);
    this.assertCurrentReconciliation(reconciliation);
    const groupIds = this.groupHistoryIds(grants, extraAccounts);
    if (groupIds !== undefined) await this.groups?.warmMembership({ groupIds });
    this.assertCurrentReconciliation(reconciliation);
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
    scope: string,
    address: Address,
    observed: SpaceRoot,
    recipientId: string,
    operation: "add" | "remove",
  ): Promise<void> {
    await this.warm(observed, [recipientId]);
    const device = await this.loadDevice();
    try {
      const proposal = await serializableE2eeTransaction(this.db, async (tx) => {
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
          spaceGrantBytes(this.accountContext(root.creatorAccountId), keyRoot, record),
        );
        this.assertOpen();
        const { id, ...values } = record;
        tx.insert(this.tables.__e2ee_space_grants, { ...values, signature }, { id });
        maintainSpaceMember(
          tx,
          this.tables.__e2ee_spaces._schema,
          scope,
          address.scopeId,
          address.identifier,
          recipientKind,
          recipientId,
          operation,
        );
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
            ...authority.roots.rows.map((root) => root.creatorAccountId),
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
      rootBytes = spaceRootBytes(accountContext(root.creatorAccountId), root);
    } catch {
      throw new InvalidSpaceRoot("Malformed E2EE space root transcript");
    }
    if (root.id !== spaceRootId(root))
      throw new InvalidSpaceRoot("Invalid E2EE space root identity");
    const history = snapshot.histories.get(root.creatorAccountId);
    if (!history) throw new Error("Missing E2EE space creator history");
    const position = positionOf(snapshot.roots, root.id);
    if (!position) throw new Error("E2EE space root lacks authority coverage");
    const initialHistory = historyBefore(history, BigInt(position));
    if (!initialHistory.roots.rows.length)
      throw new InvalidSpaceRoot("Ineligible E2EE space creator");
    const initial = await replayAccountMembership(
      initialHistory,
      accountContext(root.creatorAccountId),
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
      grant.authorAccountId !== root.creatorAccountId ||
      grant.authorDeviceId !== root.deviceId ||
      grant.authorEpochId !== root.accountEpochId
    )
      throw new Error("Invalid E2EE initial space grant");
    if (
      !(await this.verify(
        history,
        grant.authorDeviceId,
        spaceGrantBytes(accountContext(root.creatorAccountId), root, grant),
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
        grant.recipientId === root.creatorAccountId
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
          event.row.authorAccountId !== root.creatorAccountId ||
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
          bytes = spaceSuccessorBytes(accountContext(root.creatorAccountId), root, row);
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
        bytes = spaceGrantBytes(accountContext(root.creatorAccountId), keyRoot, record);
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
      usedEpochs,
      direct,
      groupRecipients,
      ...state,
      rotationRequired: rotationRequired || state.stale,
      sealed,
    };
  }

  async explain<T, Init>(scope: TableProxy<T, Init>, identifier: string): Promise<SpaceState> {
    const address = await this.address(scope, identifier);
    return this.foregroundMaintenance(address, () => this.explainAddress(address));
  }

  /** Package-internal key access backed by authority coverage or accepted local history. */
  async withKeys<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    use: (
      secret: Uint8Array,
      root: Readonly<SpaceRoot>,
      source: "accepted" | "provisional",
    ) => Promise<void>,
    includeHistory = false,
    prepareOnline?: () => Promise<void>,
    forWrite = false,
    requiredEpochs?: ReadonlySet<string>,
  ): Promise<SpaceState> {
    const address = await this.address(scope, identifier, true);
    const pending = await this.device?.journal?.space(spaceRootId(address));
    if (pending) {
      if (this.device?.isKnownRevoked()) return { state: "refused", reason: "device-not-active" };
      const founder = await this.device?.founder?.();
      const device = await this.loadDevice();
      let secret: Uint8Array | undefined;
      try {
        if (!founder) {
          const state = await observeE2eeHistory(this.db, (reader) =>
            this.requireDevice().states(reader),
          );
          if (
            !state.active.has(device.id) ||
            !state.verified.has(device.id) ||
            state.epochId !== pending.root.accountEpochId
          )
            return { state: "refused", reason: "device-not-active" };
        }
        const { root, grants } = pending;
        const grant = grants.find(
          (grant) =>
            grant.recipientKind === "account" &&
            grant.recipientId === this.accountId &&
            grant.operation === "add" &&
            grant.epochId === root.epochId &&
            grant.spaceId === root.id &&
            grant.authorDeviceId === root.deviceId &&
            grant.authorAccountId === root.creatorAccountId &&
            grant.authorEpochId === root.accountEpochId &&
            (!founder || grant.recipientEpochId === founder.epochId),
        );
        if (
          !grant ||
          root.deviceId !== device.id ||
          root.creatorAccountId !== this.accountId ||
          (founder && root.accountEpochId !== founder.epochId)
        )
          return { state: "refused", reason: "not-a-space-recipient" };
        const application = this.accountContext(root.creatorAccountId);
        if (
          !(await this.signer.verify(
            device.signing.publicKey,
            spaceRootBytes(application, root),
            root.signature,
          )) ||
          !(await this.signer.verify(
            device.signing.publicKey,
            spaceGrantBytes(application, root, grant),
            grant.signature,
          ))
        )
          throw new E2eeDataError("key-unavailable");
        secret = await this.keys.open(
          device,
          spaceContext(application, root, "author", device.id),
          root.authorEnvelope,
        );
        await this.confirm(root, secret);
        this.assertOpen();
        await use(secret, root, "provisional");
        return { state: "ready" };
      } finally {
        secret?.fill(0);
        this.requireDevice().release(device);
      }
    }
    let used = false;
    const localUse = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
      used = true;
      await use(secret, root, "accepted");
    };
    try {
      const address = await this.address(scope, identifier, true);
      const explicitlyOffline = await this.db.e2eeIsExplicitlyOffline();
      const local = await this.explainAddress(
        address,
        localUse,
        includeHistory,
        0,
        true,
        forWrite && this.staleWrites === "warn",
        undefined,
        requiredEpochs,
      );
      // A coherent local write decision does not depend on transport state.
      // Reconciliation may advance the epoch, but must not gate this preparation.
      if (
        local.state === "ready" ||
        explicitlyOffline ||
        (forWrite && local.state === "maintenance-required")
      ) {
        if (!explicitlyOffline) this.reconcileInBackground(address);
        return local;
      }
    } catch (error) {
      if (used || (await this.db.e2eeIsExplicitlyOffline())) throw error;
    }
    // A locally refused recipient may have received a grant since this snapshot.
    // Online reconciliation must discover it before returning that refusal.
    await prepareOnline?.();
    const onlineAddress = await this.address(scope, identifier);
    const state = await this.explainAddress(onlineAddress, localUse, includeHistory);
    if (state.state === "ready") this.reconcileInBackground(onlineAddress);
    return state;
  }

  private readonly pendingReconciliation = new Map<string, ReconciliationAttempt>();
  private readonly foregroundMaintenanceCounts = new Map<string, number>();

  private async foregroundMaintenance<T>(address: Address, work: () => Promise<T>): Promise<T> {
    const key = JSON.stringify([address.scopeId, address.identifier]);
    this.foregroundMaintenanceCounts.set(key, (this.foregroundMaintenanceCounts.get(key) ?? 0) + 1);
    const pending = this.pendingReconciliation.get(key);
    if (pending) pending.cancelled = true;
    try {
      return await work();
    } finally {
      const remaining = this.foregroundMaintenanceCounts.get(key)! - 1;
      if (remaining) this.foregroundMaintenanceCounts.set(key, remaining);
      else this.foregroundMaintenanceCounts.delete(key);
    }
  }

  private assertCurrentReconciliation(reconciliation?: ReconciliationAttempt): void {
    if (reconciliation?.cancelled) throw new SupersededReconciliation();
  }

  private reconcileInBackground(address: Address): void {
    const key = JSON.stringify([address.scopeId, address.identifier]);
    if (this.foregroundMaintenanceCounts.has(key) || this.pendingReconciliation.has(key)) return;
    const reconciliation: ReconciliationAttempt = { cancelled: false };
    this.pendingReconciliation.set(key, reconciliation);
    const finished = () => {
      if (this.pendingReconciliation.get(key) === reconciliation)
        this.pendingReconciliation.delete(key);
    };
    // Local key use does not wait for delivery or a disconnected server. A failed
    // attempt can retry on the next affected operation; explicit explain() still
    // reports maintenance errors to its caller. No key-use callback is replayed.
    this.explainAddress(address, undefined, false, 0, false, false, reconciliation).then(
      finished,
      finished,
    );
  }

  private async explainAddress(
    address: Address,
    use?: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
    includeHistory = false,
    conflicts = 0,
    localOnly = false,
    allowStaleWrite = false,
    reconciliation?: ReconciliationAttempt,
    requiredEpochs?: ReadonlySet<string>,
  ): Promise<SpaceState> {
    this.assertOpen();
    this.assertCurrentReconciliation(reconciliation);
    if (this.device?.isKnownRevoked()) return { state: "refused", reason: "device-not-active" };
    const offline = localOnly || (await this.db.e2eeIsExplicitlyOffline());
    this.assertCurrentReconciliation(reconciliation);
    const rootQuery = this.tables.__e2ee_spaces.where({ id: spaceRootId(address) });
    const observed = offline
      ? (await observeE2eeHistory(this.db, (reader) => reader.allSettledForE2ee(rootQuery))).rows[0]
      : await this.db.one(rootQuery, { tier: "remote" });
    this.assertCurrentReconciliation(reconciliation);
    if (!observed) return { state: "unavailable", reason: "space-not-found" };
    if (!offline) await this.warm(observed, undefined, reconciliation);
    this.assertCurrentReconciliation(reconciliation);
    const device = await this.loadDevice();
    let secret: Uint8Array | undefined;
    try {
      this.assertCurrentReconciliation(reconciliation);
      const maintenance =
        !use && !offline && !includeHistory
          ? await this.readMaintenanceSnapshot(address, observed.id, device, reconciliation)
          : undefined;
      let snapshot =
        maintenance?.snapshot ??
        (await this.readAcceptedSnapshot(address, observed.id, localOnly, reconciliation));
      this.assertCurrentReconciliation(reconciliation);
      this.assertOpen();
      if (maintenance?.prepared) {
        const { result, delivery } = maintenance.prepared;
        if (delivery && !delivery.snapshot) {
          const refreshed = await this.readAcceptedSnapshot(
            address,
            observed.id,
            false,
            reconciliation,
          );
          this.assertCurrentReconciliation(reconciliation);
          if (
            !sameSnapshotValue(snapshot, refreshed) &&
            !this.onlyOwnAcceptedDeliveries(snapshot, refreshed, {
              ...delivery,
              transactionId: maintenance.transactionId,
            })
          ) {
            if (conflicts >= 2) return { state: "unavailable", reason: "history-keeps-changing" };
            return this.explainAddress(
              address,
              use,
              includeHistory,
              conflicts + 1,
              false,
              false,
              reconciliation,
            );
          }
        }
        return result;
      }
      this.assertCurrentReconciliation(reconciliation);
      this.assertOpen();
      if (!snapshot.state.active.has(device.id))
        return { state: "refused", reason: "device-not-active" };
      if (!snapshot.roots.rows.length)
        return { state: "unavailable", reason: "space-not-accepted" };
      let state = await this.validate(snapshot, address);
      this.assertCurrentReconciliation(reconciliation);
      if (
        !offline &&
        !state.groupsReady &&
        !state.sealed &&
        state.members.has(this.accountId) &&
        this.groups
      ) {
        for (const id of state.groupRecipients.keys()) {
          const group = state.graph.get(id);
          if (group?.rotationRequired && !group.sealed && group.members.has(this.accountId)) {
            this.assertCurrentReconciliation(reconciliation);
            await this.groups.explain(id);
            this.assertCurrentReconciliation(reconciliation);
          }
        }
        // Group maintenance is an ordinary accepted write. Re-read authority
        // coverage and eligibility before opening or distributing any space key.
        await this.warm(observed, undefined, reconciliation);
        this.assertCurrentReconciliation(reconciliation);
        snapshot = await this.readAcceptedSnapshot(address, observed.id, false, reconciliation);
        this.assertCurrentReconciliation(reconciliation);
        this.assertOpen();
        if (!snapshot.state.active.has(device.id))
          return { state: "refused", reason: "device-not-active" };
        state = await this.validate(snapshot, address);
        this.assertCurrentReconciliation(reconciliation);
      }
      const { root, keyRoot } = state;
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
      if (!state.groupsReady && !allowStaleWrite)
        return { state: "maintenance-required", reason: "group-epoch-stale" };
      if (offline && state.rotationRequired && !allowStaleWrite)
        return { state: "maintenance-required", reason: "recipient-removed" };
      // A row may arrive before its successor history, especially after reopening
      // persistent storage. Refresh that history before invoking the caller:
      // neither a decryption callback nor a one-shot write may be replayed.
      if (localOnly && requiredEpochs?.size) {
        for (const epoch of requiredEpochs) {
          if (!state.usedEpochs.has(epoch))
            return { state: "unavailable", reason: "space-epoch-not-observed" };
        }
      }
      secret = await this.openSnapshotKey(snapshot, state, device, reconciliation);
      if (!secret) return { state: "unavailable", reason: "space-key-not-delivered" };
      try {
        if (state.rotationRequired && !offline) {
          this.assertCurrentReconciliation(reconciliation);
          await this.rotate(address, keyRoot, secret, device, reconciliation);
          this.assertCurrentReconciliation(reconciliation);
          return this.explainAddress(
            address,
            use,
            includeHistory,
            conflicts,
            false,
            false,
            reconciliation,
          );
        }
        // A verified key read need not wait for envelopes for other devices.
        // Explicit maintenance still delivers and revalidates its changed history.
        if (!use && !offline) {
          this.assertCurrentReconciliation(reconciliation);
          const delivery = await this.deliver(address, keyRoot, secret, device);
          this.assertCurrentReconciliation(reconciliation);
          // A read-only delivery transaction already accepted fresh authority.
          // Writes require another snapshot so newly published keys are included.
          const refreshed =
            delivery.snapshot ??
            (await this.readAcceptedSnapshot(address, observed.id, false, reconciliation));
          this.assertCurrentReconciliation(reconciliation);
          if (!sameSnapshotValue(snapshot, refreshed)) {
            if (!this.onlyOwnAcceptedDeliveries(snapshot, refreshed, delivery)) {
              if (conflicts >= 2) return { state: "unavailable", reason: "history-keeps-changing" };
              return this.explainAddress(
                address,
                use,
                includeHistory,
                conflicts + 1,
                false,
                false,
                reconciliation,
              );
            }
            snapshot = refreshed;
          }
        }
      } catch (error) {
        // Key preparation may follow another client's completed maintenance.
        // Delivery may race another maintainer. Explicit rotation still reports
        // its losing contender; never retry use() itself.
        const conflict = isTransactionConflict(error);
        if (conflict && (use || !state.rotationRequired) && conflicts < 2) {
          this.assertCurrentReconciliation(reconciliation);
          return this.explainAddress(
            address,
            use,
            includeHistory,
            conflicts + 1,
            false,
            false,
            reconciliation,
          );
        }
        if (!(error instanceof PersistedWriteRejectedError) || error.code !== "permission_denied")
          throw error;
        if (state.rotationRequired)
          return { state: "maintenance-required", reason: "recipient-removed" };
      }
      if (!recipient) return { state: "refused", reason: "not-a-space-recipient" };
      this.assertOpen();
      this.assertCurrentReconciliation(reconciliation);
      if (allowStaleWrite && (!state.groupsReady || state.rotationRequired))
        console.warn(
          "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
        );
      if (includeHistory) await this.confirmHistory(snapshot, root, secret, use);
      else await use?.(secret, keyRoot);
      return { state: "ready" };
    } finally {
      secret?.fill(0);
      this.requireDevice().release(device);
    }
  }

  private async readMaintenanceSnapshot(
    address: Address,
    id: string,
    device: LocalDevice,
    reconciliation?: ReconciliationAttempt,
  ) {
    for (let attempt = 0; ; attempt++) {
      let inspecting = false;
      let deliveryStarted = false;
      this.assertCurrentReconciliation(reconciliation);
      try {
        return await prepareAcceptedHistory(
          this.db,
          JSON.stringify(["space", address.scopeId, address.identifier]),
          (tx) => this.readSnapshot(tx, address, id),
          async (tx, snapshot) => {
            this.assertCurrentReconciliation(reconciliation);
            if (!snapshot.state.active.has(device.id) || !snapshot.roots.rows.length) return;
            inspecting = true;
            const state = await this.validate(snapshot, address);
            inspecting = false;
            // Actual epoch changes and group repair keep their existing accepted boundaries.
            if (state.sealed || state.rotationRequired || !state.groupsReady) return;
            const recipient = state.members.has(this.accountId);
            if (
              !recipient &&
              !isInitialAuthor(
                state.root,
                state.keyRoot.epochId,
                this.accountId,
                device.id,
                snapshot.state.epochId,
              )
            )
              return;
            inspecting = true;
            const secret = await this.openSnapshotKey(snapshot, state, device, reconciliation);
            inspecting = false;
            try {
              if (!secret)
                return {
                  result: { state: "unavailable", reason: "space-key-not-delivered" } as SpaceState,
                };
              this.assertCurrentReconciliation(reconciliation);
              deliveryStarted = true;
              const delivery = await this.prepareDelivery(
                tx,
                snapshot,
                address,
                state.keyRoot,
                secret,
                device,
              );
              this.assertOpen();
              this.assertCurrentReconciliation(reconciliation);
              return {
                result: (recipient
                  ? { state: "ready" }
                  : { state: "refused", reason: "not-a-space-recipient" }) as SpaceState,
                delivery,
              };
            } finally {
              secret?.fill(0);
            }
          },
        );
      } catch (error) {
        // Adapter failures during inspection are not transaction rejections,
        // even when a provider forwards an error with an SDK rejection code.
        if (inspecting) throw error;
        if (isTransactionConflict(error) && attempt < 2) continue;
        if (
          !deliveryStarted ||
          !(error instanceof PersistedWriteRejectedError) ||
          error.code !== "permission_denied"
        )
          throw error;
        // Denied delivery must not deny an otherwise usable key. Establish a
        // separately accepted read before the existing policy-denial path runs.
        return {
          snapshot: await this.readAcceptedSnapshot(address, id, false, reconciliation),
          prepared: undefined,
          transactionId: "",
        };
      }
    }
  }

  private async openSnapshotKey(
    snapshot: Snapshot & { state: DeviceState },
    state: Awaited<ReturnType<Spaces["validate"]>>,
    device: LocalDevice,
    reconciliation?: ReconciliationAttempt,
  ): Promise<Uint8Array | undefined> {
    const { root, keyRoot, successor, position } = state;
    let secret: Uint8Array | undefined;
    try {
      if (!successor && root.creatorAccountId === this.accountId && root.deviceId === device.id) {
        secret = await this.keys.open(
          device,
          spaceContext(this.accountContext(root.creatorAccountId), root, "author", device.id),
          root.authorEnvelope,
        );
        this.assertCurrentReconciliation(reconciliation);
        await this.confirmHistory(snapshot, root, secret);
        this.assertCurrentReconciliation(reconciliation);
      } else if (
        successor?.authorAccountId === this.accountId &&
        successor.authorDeviceId === device.id
      ) {
        secret = await this.keys.open(
          device,
          spaceSuccessorContext(
            this.accountContext(root.creatorAccountId),
            root,
            successor,
            "author-envelope",
            device.id,
          ),
          successor.authorEnvelope,
        );
        this.assertCurrentReconciliation(reconciliation);
        await this.confirmHistory(snapshot, root, secret);
        this.assertCurrentReconciliation(reconciliation);
      } else {
        for (const delivery of snapshot.deliveries.rows) {
          this.assertCurrentReconciliation(reconciliation);
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
          this.assertCurrentReconciliation(reconciliation);
          const context = spaceDeliveryContext(
            this.accountContext(root.creatorAccountId),
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
            this.assertCurrentReconciliation(reconciliation);
            await this.confirmHistory(snapshot, root, opened);
            this.assertCurrentReconciliation(reconciliation);
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
      this.assertCurrentReconciliation(reconciliation);
      if (!secret) {
        secret = await loadRecoveredSpaceKey(
          this.requireDevice().store,
          this.accountContext(this.accountId),
          root.id,
          keyRoot.epochId,
          this.assertOpen,
          this.requireDevice().keyLifetime,
        );
        this.assertCurrentReconciliation(reconciliation);
        if (secret) await this.confirmHistory(snapshot, root, secret);
      }
      this.assertCurrentReconciliation(reconciliation);
      return secret;
    } catch (error) {
      secret?.fill(0);
      throw error;
    }
  }

  private async readAcceptedSnapshot(
    address: Address,
    id: string,
    localOnly = false,
    reconciliation?: ReconciliationAttempt,
  ) {
    for (let attempt = 0; ; attempt++) {
      this.assertCurrentReconciliation(reconciliation);
      try {
        return await readAcceptedHistory(
          this.db,
          JSON.stringify(["space", address.scopeId, address.identifier]),
          (tx) => this.readSnapshot(tx, address, id),
          localOnly,
        );
      } catch (error) {
        if (!isTransactionConflict(error) || attempt >= 2) throw error;
        this.assertCurrentReconciliation(reconciliation);
      }
    }
  }

  private async rotate(
    address: Address,
    expected: SpaceRoot,
    previousSecret: Uint8Array,
    device: LocalDevice,
    reconciliation?: ReconciliationAttempt,
  ) {
    const secret = runtimeRandomBytes(32);
    try {
      const proposal = await serializableE2eeTransaction(this.db, async (tx) => {
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
        const application = this.accountContext(state.root.creatorAccountId);
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
      this.assertCurrentReconciliation(reconciliation);
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
      this.accountContext(root.creatorAccountId),
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
          spaceSuccessorContext(
            this.accountContext(root.creatorAccountId),
            root,
            successor,
            "history",
          ),
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
          ? spaceRecoveryBytes(this.accountContext(root.creatorAccountId), keyRoot, row)
          : spaceDeliveryBytes(this.accountContext(root.creatorAccountId), keyRoot, row);
    } catch {
      return false;
    }
    return this.verify(history, row.senderDeviceId, bytes, row.signature);
  }

  private onlyOwnAcceptedDeliveries(
    before: Snapshot & { state: DeviceState },
    after: Snapshot & { state: DeviceState },
    delivery: { rows: SpaceDelivery[]; transactionId: string },
  ): boolean {
    if (!delivery.rows.length) return false;
    const inserted = new Map(delivery.rows.map((row) => [row.id, row]));
    if (
      inserted.size !== delivery.rows.length ||
      before.deliveries.rows.some((row) => inserted.has(row.id)) ||
      before.deliveries.settlements.some((row) => inserted.has(row.rowId))
    )
      return false;
    const rows: SpaceDelivery[] = [];
    const settlements: RowSettlement[] = [];
    const seenRows = new Set<string>();
    const seenSettlements = new Set<string>();
    for (const row of after.deliveries.rows) {
      const own = inserted.get(row.id);
      if (!own) {
        rows.push(row);
        continue;
      }
      if (seenRows.has(row.id) || !sameSnapshotValue(own, row)) return false;
      seenRows.add(row.id);
    }
    for (const settlement of after.deliveries.settlements) {
      if (!inserted.has(settlement.rowId)) {
        settlements.push(settlement);
        continue;
      }
      if (
        seenSettlements.has(settlement.rowId) ||
        settlement.transactionId !== delivery.transactionId
      )
        return false;
      seenSettlements.add(settlement.rowId);
    }
    // Only this invocation's accepted append is discounted for comparison.
    // The full fresh snapshot remains retained; all authority and key-source
    // records, including their settlements, must otherwise be unchanged.
    return (
      seenRows.size === inserted.size &&
      seenSettlements.size === inserted.size &&
      sameSnapshotValue(before, { ...after, deliveries: { rows, settlements } })
    );
  }

  private async deliver(
    address: Address,
    expected: SpaceRoot,
    secret: Uint8Array | undefined,
    device: LocalDevice,
  ) {
    const delivery = await serializableE2eeTransaction(this.db, async (tx) =>
      this.prepareDelivery(
        tx,
        await this.readSnapshot(tx, address, expected.id),
        address,
        expected,
        secret,
        device,
      ),
    );
    const result = await delivery.wait({ tier: "global" });
    const transactionId = await delivery.txId;
    this.assertOpen();
    return { ...result, transactionId };
  }

  private async prepareDelivery(
    tx: E2eeTransactionScope,
    snapshot: Snapshot & { state: DeviceState },
    address: Address,
    expected: SpaceRoot,
    secret: Uint8Array | undefined,
    device: LocalDevice,
  ) {
    let wrote = false;
    const inserted: SpaceDelivery[] = [];
    const { root, keyRoot, members, rotationRequired, position, sealed, groupsReady } =
      await this.validate(snapshot, address);
    // Initial completion may use only the original author's accepted epoch.
    // Keep these reads in the delivery transaction so concurrent changes reject
    // the write, including changes while the asynchronous key open is pending.
    if (
      secret === undefined &&
      (root.id !== expected.id ||
        root.epochId !== expected.epochId ||
        !snapshot.state.active.has(device.id) ||
        sealed ||
        rotationRequired ||
        !groupsReady ||
        !isInitialAuthor(root, keyRoot.epochId, this.accountId, device.id, snapshot.state.epochId))
    )
      throw new E2eeDataError("key-unavailable");
    if (
      root.id !== expected.id ||
      keyRoot.epochId !== expected.epochId ||
      !snapshot.state.active.has(device.id) ||
      rotationRequired ||
      (snapshot.state.epochId !== members.get(this.accountId) &&
        !isInitialAuthor(root, keyRoot.epochId, this.accountId, device.id, snapshot.state.epochId))
    )
      throw new Error("E2EE space membership requires reconciliation");
    const deliverySecret =
      secret ??
      (await this.keys.open(
        device,
        spaceContext(this.accountContext(root.creatorAccountId), root, "author", device.id),
        root.authorEnvelope,
      ));
    try {
      await this.confirmHistory(snapshot, root, deliverySecret);
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
              spaceRecoveryContext(this.accountContext(root.creatorAccountId), keyRoot, row),
              deliverySecret,
            );
            const signature = await this.sign(
              device,
              spaceRecoveryBytes(this.accountContext(root.creatorAccountId), keyRoot, {
                ...row,
                envelope,
              }),
            );
            this.assertOpen();
            const { id, ...values } = row;
            wrote = true;
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
            spaceDeliveryContext(this.accountContext(root.creatorAccountId), keyRoot, row),
            deliverySecret,
          );
          const signature = await this.sign(
            device,
            spaceDeliveryBytes(this.accountContext(root.creatorAccountId), keyRoot, {
              ...row,
              envelope,
            }),
          );
          this.assertOpen();
          const { id, ...values } = row;
          wrote = true;
          inserted.push(
            tx.insert(
              this.tables.__e2ee_space_deliveries,
              { ...values, envelope, signature },
              { id },
            ),
          );
        }
      }
      return { rows: inserted, snapshot: wrote ? undefined : snapshot };
    } finally {
      // Caller-owned keys retain their caller's lifetime.
      if (secret === undefined) deliverySecret.fill(0);
    }
  }

  private async confirm(
    root: Pick<
      SpaceRoot,
      | "id"
      | "scopeId"
      | "identifier"
      | "epochId"
      | "creatorAccountId"
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
      context ?? spaceContext(this.accountContext(root.creatorAccountId), root, "verification"),
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
