import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db, E2eeTransactionScope } from "../runtime/db.js";
import type { RowSettlement } from "../runtime/client.js";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import type { AccountStore } from "../accounts/persistence.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { replayGroupGraph, validGroupGraph } from "./group-graph.js";
import type { DeviceTables } from "./device-requests.js";
import type { LocalDevice } from "./local-device.js";
import type { DeviceSigner, KeyEnvelope } from "./types.js";
import { sameSnapshotValue } from "./public-snapshot.js";
import type {
  GroupTables,
  GroupRoot,
  GroupMembership,
  GroupDelivery,
  GroupRecoveryDelivery,
  GroupRepair,
  GroupSuccessor,
} from "./groups.js";
import {
  readPublicMembershipHistory,
  readAccountMembership,
  prefetchPublicMembershipHistory,
  replayAccountMembership,
  historyBefore,
} from "./public-membership.js";
import {
  groupContext,
  groupRootBytes,
  groupDeliveryContext,
  groupDeliveryBytes,
  groupMembershipBytes,
  groupRepairBytes,
} from "./group-format.js";
import { loadStagedGroupKey, stageGroupKey } from "./local-group-keys.js";
import { decodeRecoveryMaterial } from "./recovery-format.js";
import { E2eeRecoveryError } from "./recovery-error.js";
import { groupRecoveryContext, groupRecoveryBytes } from "./group-recovery-format.js";
import {
  encodeGroupMembership,
  groupSuccessorSigningBytes,
  groupSuccessorContext,
} from "./group-successor.js";

type GroupKey = Pick<
  GroupRoot,
  "id" | "accountId" | "epochId" | "mechanism" | "version" | "verification"
>;

type History = Awaited<ReturnType<typeof readPublicMembershipHistory>>;
type GroupDevice = {
  store: AccountStore;
  isKnownRevoked(): boolean;
  load(): Promise<LocalDevice>;
  states(transaction?: E2eeTransactionScope): Promise<{
    active: Set<string>;
    epochId: string;
    publicHistory: History;
  }>;
};
export type GroupRecoveryPath = { groupId: string; epochId: string } & (
  | { validation: "validated" }
  | {
      validation: "unavailable";
      reason: "missing-recovery-delivery" | "unusable-recovery-delivery" | "maintenance-required";
    }
);
type MembershipSnapshot = {
  roots: { rows: GroupRoot[]; settlements: RowSettlement[] };
  targetRoots: { rows: GroupRoot[]; settlements: RowSettlement[] };
  records: { rows: GroupMembership[]; settlements: RowSettlement[] };
  successors: { rows: GroupSuccessor[]; settlements: RowSettlement[] };
  histories: Map<string, History>;
};

class UnavailableGroupKey extends Error {}

function unavailableGroupKey(cause: unknown): never {
  throw new UnavailableGroupKey("Unable to authenticate E2EE group key", { cause });
}

// Account and group recipients are UUID row IDs. Retain the guard against
// malformed historical candidates before issuing account-reference queries.
function accountMemberId(row: GroupMembership): string | undefined {
  return row.memberKind === "account" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(row.memberId)
    ? row.memberId
    : undefined;
}

/** Group roots are accepted before any recipient envelopes are published. */
export class Groups {
  private validated?: {
    input: unknown;
    verify: DeviceSigner["verify"];
    graphs: Map<bigint | undefined, Awaited<ReturnType<typeof replayGroupGraph>>>;
  };

  constructor(
    private readonly db: Db,
    private readonly accountId: string,
    private readonly application: string,
    private readonly tables: DeviceTables & GroupTables,
    private readonly keys: KeyEnvelope,
    private readonly signer: DeviceSigner,
    private readonly assertOpen: () => void,
    private readonly accountContext: (accountId: string) => string,
    private readonly device?: GroupDevice,
  ) {}

  private requireDevice(): GroupDevice {
    if (!this.device) throw new Error("Device enrolment is required for this group operation");
    return this.device;
  }

  private get store(): AccountStore {
    return this.requireDevice().store;
  }
  private loadDevice(): Promise<LocalDevice> {
    return this.requireDevice().load();
  }
  private deviceStates(transaction?: E2eeTransactionScope) {
    return this.requireDevice().states(transaction);
  }

  /** Discover current effective memberships, not just the envelopes that happen to exist. */
  async inspectRecovery(value: string, accountEpochId: string): Promise<GroupRecoveryPath[]> {
    const material = await decodeRecoveryMaterial(value, this.application, this.keys, this.signer);
    try {
      const query = this.tables.__e2ee_group_recovery_deliveries.where({
        recipientAccountId: this.accountId,
        recoveryRootId: material.rootId,
      });
      await Promise.all([this.warmMembership(null), this.db.all(query, { tier: "edge" })]);
      const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const own = await readPublicMembershipHistory(tx, this.accountId, this.tables);
        const [group, deliveries] = await Promise.all([
          this.readMembership(tx, null, own),
          tx.allSettledForE2ee(query),
        ]);
        return { own, group, deliveries };
      });
      const snapshot = await read.wait({ tier: "global" });
      this.assertOpen();
      const own = await this.checkRecoveryAuthority(material, snapshot.own);
      if (own.epochId !== accountEpochId) throw new E2eeRecoveryError("recovery-state-changed");
      const graph = await this.acceptedGraph(snapshot.group);
      const paths: GroupRecoveryPath[] = [];
      for (const [id, state] of graph) {
        if (!state.members.has(this.accountId)) continue;
        const path = { groupId: id, epochId: state.keyRoot.epochId };
        if (state.rotationRequired) {
          paths.push({ ...path, validation: "unavailable", reason: "maintenance-required" });
          continue;
        }
        const position = snapshot.group.roots.settlements.find(
          (entry) => entry.rowId === id,
        )?.position;
        if (position === undefined) throw new Error("Missing E2EE group authority coverage");
        const root = snapshot.group.roots.rows.find((row) => row.id === id);
        if (!root) throw new Error("Missing accepted E2EE group root");
        const secret = await this.openRecovery(
          root,
          position,
          state.keyRoot.epochId,
          snapshot.group,
          snapshot.deliveries,
          material,
        );
        if (secret) {
          secret.fill(0);
          paths.push({ ...path, validation: "validated" });
        } else {
          const present = snapshot.deliveries.rows.some(
            (row) => row.groupId === id && row.epochId === path.epochId,
          );
          paths.push({
            ...path,
            validation: "unavailable",
            reason: present ? "unusable-recovery-delivery" : "missing-recovery-delivery",
          });
        }
      }
      this.assertOpen();
      return paths.sort((a, b) => a.groupId.localeCompare(b.groupId));
    } finally {
      material.recipient.privateKey.fill(0);
      material.signing.privateKey.fill(0);
    }
  }

  private async checkRecoveryAuthority(
    material: Awaited<ReturnType<typeof decodeRecoveryMaterial>>,
    history: History,
  ) {
    const own = await replayAccountMembership(history, this.application, this.signer);
    const root = own.recoveryRoots.find((row) => row.id === material.rootId);
    const same = (a: Uint8Array, b: Uint8Array) =>
      a.length === b.length && a.every((byte, i) => byte === b[i]);
    if (
      !root ||
      root.mechanism !== this.keys.mechanism.id ||
      root.version !== this.keys.mechanism.version ||
      !same(root.publicKey, material.recipient.publicKey) ||
      !same(root.signingPublicKey, material.signing.publicKey)
    )
      throw new Error("Group recovery material does not match accepted recovery authority");
    return own;
  }

  private async openRecovery(
    root: GroupRoot,
    position: string,
    epochId: string,
    group: MembershipSnapshot,
    deliveries: { rows: GroupRecoveryDelivery[]; settlements: RowSettlement[] },
    material: Awaited<ReturnType<typeof decodeRecoveryMaterial>>,
  ): Promise<Uint8Array | undefined> {
    for (const row of deliveries.rows) {
      if (
        row.groupId !== root.id ||
        row.epochId !== epochId ||
        row.recipientAccountId !== this.accountId ||
        row.recoveryRootId !== material.rootId
      )
        continue;
      const at = deliveries.settlements.find((entry) => entry.rowId === row.id)?.position;
      if (!(await this.acceptedDelivery(root, position, group, row, at))) continue;
      let secret: Uint8Array | undefined;
      try {
        secret = await this.keys
          .open(
            material.recipient,
            groupRecoveryContext(this.accountContext(root.accountId), row),
            row.envelope,
          )
          .catch(unavailableGroupKey);
        await this.confirmHistory(root, position, group, secret);
        this.assertOpen();
        return secret;
      } catch (error) {
        secret?.fill(0);
        this.assertOpen();
        if (!(error instanceof UnavailableGroupKey)) throw error;
      }
    }
    return undefined;
  }

  add(id: string, memberId: string): Promise<void> {
    return this.changeMembership(id, memberId, "add");
  }

  /** Backfill effective memberships, including inherited paths, then verify recovery. */
  async protectRecovery(value: string): Promise<void> {
    await this.warmMembership(null);
    const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
      const own = await this.deviceStates(tx);
      return this.readMembership(tx, null, own.publicHistory);
    });
    const snapshot = await read.wait({ tier: "global" });
    this.assertOpen();
    const graph = await this.acceptedGraph(snapshot);
    const required: string[] = [];
    for (const [id, membership] of graph) {
      if (!membership.members.has(this.accountId)) continue;
      const state = await this.explain(id);
      if (state.state !== "ready") throw new Error("Group key unavailable while creating recovery");
      required.push(id);
    }
    await this.restoreRecovery(value, required);
  }

  /** Recovery private keys are used only for this operation, never saved to the device store. */
  async restoreRecovery(value: string, required: string[] = []): Promise<void> {
    const material = await decodeRecoveryMaterial(value, this.application, this.keys, this.signer);
    let device: LocalDevice | undefined;
    try {
      device = await this.loadDevice();
      const candidates = await this.db.all(
        this.tables.__e2ee_group_recovery_deliveries.where({
          recipientAccountId: this.accountId,
          recoveryRootId: material.rootId,
        }),
        { tier: "edge" },
      );
      const ids = new Set([...required, ...candidates.map((row) => row.groupId)]);
      for (const id of ids) {
        const observed = await this.db.one(this.tables.__e2ee_groups.where({ id }), {
          tier: "edge",
        });
        if (!observed) throw new Error("Recovery group not available");
        await this.warmMembership(observed);
        const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
          const own = await this.deviceStates(tx);
          const group = await this.readMembership(tx, observed, own.publicHistory);
          return {
            own,
            roots: group.targetRoots,
            group,
            deliveries: await tx.allSettledForE2ee(
              this.tables.__e2ee_group_recovery_deliveries.where({
                groupId: id,
                recipientAccountId: this.accountId,
                recoveryRootId: material.rootId,
              }),
            ),
          };
        });
        const snapshot = await read.wait({ tier: "global" });
        this.assertOpen();
        if (!snapshot.own.active.has(device.id))
          throw new Error("Group recovery requires an active device");
        await this.checkRecoveryAuthority(material, snapshot.own.publicHistory);
        if (snapshot.roots.rows[0]?.accountId !== observed.accountId)
          throw new Error("E2EE group creator changed");
        const { root, position } = await this.acceptedRoot(
          snapshot.roots,
          snapshot.group.histories.get(observed.accountId)!,
        );
        const { members, keyRoot } = await this.acceptedMembership(root, position, snapshot.group);
        // Retained material cannot restore membership that has since been removed.
        if (!members.has(this.accountId)) continue;
        const secret = await this.openRecovery(
          root,
          position,
          keyRoot.epochId,
          snapshot.group,
          snapshot.deliveries,
          material,
        );
        if (!secret)
          throw new Error("No authenticated group recovery delivery for the current epoch");
        try {
          await stageGroupKey(
            this.store,
            this.application,
            id,
            keyRoot.epochId,
            secret,
            this.assertOpen,
          );
        } finally {
          secret.fill(0);
        }
        // Revalidate current membership and ordinary write permissions before delivery.
        const state = await this.explain(id);
        if (state.state !== "ready" && state.state !== "refused")
          throw new Error("Recovered group requires maintenance");
      }
    } finally {
      material.recipient.privateKey.fill(0);
      material.signing.privateKey.fill(0);
      device?.privateKey.fill(0);
      device?.signing.privateKey.fill(0);
    }
  }

  remove(id: string, memberId: string): Promise<void> {
    return this.changeMembership(id, memberId, "remove");
  }

  private async changeMembership(
    id: string,
    memberId: string,
    operation: "add" | "remove",
  ): Promise<void> {
    const observed = await this.db.one(this.tables.__e2ee_groups.where({ id }), { tier: "edge" });
    if (!observed) throw new Error("E2EE group not found");
    const child = await this.db.one(this.tables.__e2ee_groups.where({ id: memberId }), {
      tier: "edge",
    });
    await Promise.all(
      [observed.accountId, child?.accountId ?? memberId].map((accountId) =>
        readAccountMembership(
          this.db,
          accountId,
          this.accountContext(accountId),
          this.signer,
          this.tables,
        ),
      ),
    );
    await this.warmMembership(observed);
    const device = await this.loadDevice();
    try {
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const author = await this.deviceStates(tx);
        if (!author.active.has(device.id))
          throw new Error("An active device must author group membership changes");
        const group = await this.readMembership(tx, observed, author.publicHistory);
        const roots = group.targetRoots;
        if (roots.rows[0]?.accountId !== observed.accountId)
          throw new Error("E2EE group creator changed");
        const history = group.histories.get(observed.accountId)!;
        const { root, position } = await this.acceptedRoot(roots, history);
        // Include recipient history in the acceptance predicate; discovery alone
        // must not stand in for the authority-settled account binding.
        const { keyRoot, sealed, graph } = await this.acceptedMembership(root, position, group);
        if (sealed) throw new Error("E2EE group is sealed");
        const memberKind = group.roots.rows.some((candidate) => candidate.id === memberId)
          ? "group"
          : "account";
        if (memberKind === "group") {
          const recipient = graph.get(memberId);
          if (!recipient || (operation === "add" && recipient.sealed))
            throw new Error("Missing or sealed E2EE child group");
          if (operation === "add") {
            graph.get(id)!.children.add(memberId);
            if (!validGroupGraph(graph))
              throw new Error("E2EE group cycle or depth limit exceeded");
          }
        } else {
          const recipient = await readPublicMembershipHistory(tx, memberId, this.tables);
          await replayAccountMembership(recipient, this.accountContext(memberId), this.signer);
        }
        const record = {
          id: crypto.randomUUID(),
          groupId: id,
          epochId: keyRoot.epochId,
          authorAccountId: this.accountId,
          authorDeviceId: device.id,
          authorEpochId: author.epochId,
          operation,
          memberKind,
          memberId,
        };
        const signature = await this.checkedSignature(
          device,
          groupMembershipBytes(this.accountContext(root.accountId), record),
        );
        this.assertOpen();
        const { id: rowId, ...values } = record;
        tx.insert(this.tables.__e2ee_group_membership, { ...values, signature }, { id: rowId });
      });
      // Ordinary Jazz policy decides whether this administrator may make the
      // change. Neither group membership nor a group key grants that permission.
      await proposal.wait({ tier: "global" });
      this.assertOpen();
      await this.explain(id);
    } finally {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  async create(id: string): Promise<{ id: string }> {
    const device = await this.loadDevice();
    const secret = runtimeRandomBytes(32);
    try {
      if (!(await this.deviceStates()).active.has(device.id))
        throw new Error("An approved active device must create the group");
      const epochId = crypto.randomUUID();
      const binding = { id, epochId };
      const verification = await this.checkedWrap(
        secret,
        groupContext(this.application, binding, "verification"),
        new Uint8Array(32),
      );
      await stageGroupKey(this.store, this.application, id, epochId, secret, this.assertOpen);
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const membership = await this.deviceStates(tx);
        if (!membership.active.has(device.id))
          throw new Error("An approved active device must create the group");
        if (await tx.one(this.tables.__e2ee_groups.where({ id }), { tier: "local" }))
          throw new Error("E2EE group already exists");
        const root = {
          id,
          accountId: this.accountId,
          deviceId: device.id,
          accountEpochId: membership.epochId,
          epochId,
          mechanism: this.keys.mechanism.id,
          version: this.keys.mechanism.version,
          verification,
        };
        const signature = await this.checkedSignature(
          device,
          groupRootBytes(this.application, root),
        );
        this.assertOpen();
        const { id: rowId, ...values } = root;
        return tx.insert(this.tables.__e2ee_groups, { ...values, signature }, { id: rowId });
      });
      const root = await proposal.wait({ tier: "global" });
      this.assertOpen();
      await this.deliver(root, secret, device);
      return { id };
    } finally {
      secret.fill(0);
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  async explain(id: string): Promise<{
    state: "ready" | "refused" | "unavailable" | "maintenance-required";
    reason?: string;
  }> {
    this.assertOpen();
    if (this.device?.isKnownRevoked()) return { state: "refused", reason: "device-not-active" };
    const observed = await this.db.one(this.tables.__e2ee_groups.where({ id }), { tier: "edge" });
    if (!observed) return { state: "unavailable", reason: "group-not-found" };
    await this.warmMembership(observed);
    const device = await this.loadDevice();
    try {
      await this.db.all(this.tables.__e2ee_group_deliveries.where({ groupId: id }), {
        tier: "edge",
      });
      const read = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const state = await this.deviceStates(tx);
        const group = await this.readMembership(tx, observed, state.publicHistory);
        return {
          roots: group.targetRoots,
          deliveries: await tx.allSettledForE2ee(
            this.tables.__e2ee_group_deliveries.where({ groupId: id }),
          ),
          history: state.publicHistory,
          group,
          state,
        };
      });
      const snapshot = await read.wait({ tier: "global" });
      this.assertOpen();
      // Check the device against this accepted snapshot, before opening any group key.
      const current = snapshot.state;
      if (!current.active.has(device.id)) return { state: "refused", reason: "device-not-active" };
      if (snapshot.roots.rows[0]?.accountId !== observed.accountId)
        throw new Error("E2EE group creator changed");
      const { root, position } = await this.acceptedRoot(
        snapshot.roots,
        snapshot.group.histories.get(observed.accountId)!,
      );
      const { members, rotationRequired, successor, keyRoot, sealed } =
        await this.acceptedMembership(root, position, snapshot.group);
      if (sealed) return { state: "refused", reason: "group-sealed" };
      if (!members.has(this.accountId)) return { state: "refused", reason: "not-a-group-member" };
      const staged = await loadStagedGroupKey(this.store, this.application, id, this.assertOpen);
      if (staged) {
        try {
          if (staged.epochId !== keyRoot.epochId && !successor)
            throw new Error("Staged E2EE group epoch does not match");
          if (staged.epochId === keyRoot.epochId) {
            // Readiness depends on the authenticated key, not delivery permission.
            // Confirm it before catching any denial from the maintenance path.
            await this.confirmHistory(root, position, snapshot.group, staged.secret);
            try {
              if (rotationRequired) await this.rotate(keyRoot, staged.secret, device);
              else await this.deliver(keyRoot, staged.secret, device);
            } catch (error) {
              if (
                !(error instanceof PersistedWriteRejectedError) ||
                error.code !== "permission_denied"
              )
                throw error;
              if (rotationRequired)
                return { state: "maintenance-required", reason: "recipient-removed" };
            }
            return { state: "ready" };
          }
        } finally {
          staged.secret.fill(0);
        }
      }
      let candidateFailure: { error: unknown } | undefined;
      if (successor?.authorAccountId === this.accountId && successor.authorDeviceId === device.id) {
        try {
          const payload = await this.keys
            .open(
              device,
              groupSuccessorContext(
                this.accountContext(root.accountId),
                successor,
                "author-envelope",
                device.id,
              ),
              successor.authorEnvelope,
            )
            .catch(unavailableGroupKey);
          try {
            await this.confirmHistory(root, position, snapshot.group, payload);
            try {
              if (rotationRequired) await this.rotate(keyRoot, payload, device);
              else await this.deliver(keyRoot, payload, device);
            } catch (error) {
              if (
                !(error instanceof PersistedWriteRejectedError) ||
                error.code !== "permission_denied"
              )
                throw error;
              if (rotationRequired)
                return { state: "maintenance-required", reason: "recipient-removed" };
            }
            return { state: "ready" };
          } finally {
            payload.fill(0);
          }
        } catch (error) {
          this.assertOpen();
          if (!(error instanceof UnavailableGroupKey)) throw error;
          candidateFailure = { error };
        }
      }
      const failedDeliveries: string[] = [];
      for (const delivery of snapshot.deliveries.rows) {
        if (
          delivery.recipientAccountId !== this.accountId ||
          delivery.recipientDeviceId !== device.id ||
          delivery.epochId !== keyRoot.epochId
        )
          continue;
        const deliveryPosition = snapshot.deliveries.settlements.find(
          (entry) => entry.rowId === delivery.id,
        )?.position;
        if (
          !(await this.acceptedDelivery(root, position, snapshot.group, delivery, deliveryPosition))
        )
          continue;
        let usable = false;
        try {
          const payload = await this.keys
            .open(
              device,
              groupDeliveryContext(this.accountContext(root.accountId), keyRoot, delivery),
              delivery.envelope,
            )
            .catch(unavailableGroupKey);
          try {
            await this.confirmHistory(root, position, snapshot.group, payload);
            usable = true;
            try {
              if (rotationRequired) await this.rotate(keyRoot, payload, device);
              else await this.deliver(keyRoot, payload, device);
            } catch (error) {
              // An accepted key remains usable when policy denies this member
              // permission to deliver it to somebody else.
              if (
                !(error instanceof PersistedWriteRejectedError) ||
                error.code !== "permission_denied"
              )
                throw error;
              if (rotationRequired)
                return { state: "maintenance-required", reason: "recipient-removed" };
            }
          } finally {
            payload.fill(0);
          }
        } catch (error) {
          this.assertOpen();
          if (!(error instanceof UnavailableGroupKey)) throw error;
          // One unusable candidate must not hide a later authenticated key.
          candidateFailure ??= { error };
          if (!usable) failedDeliveries.push(delivery.id);
          continue;
        }
        this.assertOpen();
        return { state: "ready" };
      }
      if (candidateFailure) {
        if (failedDeliveries.length) await this.requestRepair(keyRoot, device, failedDeliveries);
        throw candidateFailure.error;
      }
      return { state: "unavailable", reason: "group-key-pending" };
    } finally {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  private async rotate(expected: GroupKey, previousSecret: Uint8Array, device: LocalDevice) {
    await this.warmMembership(expected);
    const secret = runtimeRandomBytes(32);
    try {
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const own = await this.deviceStates(tx);
        const group = await this.readMembership(tx, expected, own.publicHistory);
        const roots = group.targetRoots;
        const { root, position } = await this.acceptedRoot(
          roots,
          group.histories.get(expected.accountId)!,
        );
        const state = await this.acceptedMembership(root, position, group);
        if (root.accountId !== expected.accountId || state.keyRoot.epochId !== expected.epochId)
          throw new Error("E2EE group predecessor changed during rotation");
        if (
          !state.rotationRequired ||
          !state.members.has(this.accountId) ||
          !own.active.has(device.id)
        )
          throw new Error("E2EE group rotation requires an eligible remaining member");
        await this.confirmHistory(root, position, group, previousSecret);
        const members = new Map<string, string>();
        for (const accountId of state.members.keys()) {
          const account = await replayAccountMembership(
            group.histories.get(accountId)!,
            this.accountContext(accountId),
            this.signer,
          );
          members.set(accountId, account.epochId);
        }
        const coordinates = {
          id: crypto.randomUUID(),
          groupId: root.id,
          predecessor: state.keyRoot.epochId,
          epochId: crypto.randomUUID(),
          authorAccountId: this.accountId,
          authorDeviceId: device.id,
          authorEpochId: own.epochId,
        };
        const context = groupSuccessorContext(
          this.accountContext(root.accountId),
          coordinates,
          "author-envelope",
          device.id,
        );
        const authorEnvelope = await this.keys.seal(device.publicKey, context, secret);
        const opened = await this.keys.open(device, context, authorEnvelope);
        try {
          if (opened.length !== secret.length || !opened.every((byte, i) => byte === secret[i]))
            throw new Error("Invalid generated E2EE group successor self-envelope");
        } finally {
          opened.fill(0);
        }
        const record = {
          ...coordinates,
          revision: state.revision,
          membership: encodeGroupMembership(members),
          authorEnvelope,
          verification: await this.checkedWrap(
            secret,
            groupContext(
              this.accountContext(root.accountId),
              { id: root.id, epochId: coordinates.epochId },
              "verification",
            ),
            new Uint8Array(32),
          ),
          history: await this.checkedWrap(
            secret,
            groupSuccessorContext(this.accountContext(root.accountId), coordinates, "history"),
            previousSecret,
          ),
        };
        const signature = await this.checkedSignature(
          device,
          groupSuccessorSigningBytes(this.accountContext(root.accountId), record),
        );
        this.assertOpen();
        const { id, ...values } = record;
        return tx.insert(this.tables.__e2ee_group_successors, { ...values, signature }, { id });
      });
      const accepted = await proposal.wait({ tier: "global" });
      this.assertOpen();
      await this.deliver(
        { ...expected, epochId: accepted.epochId, verification: accepted.verification },
        secret,
        device,
      );
    } finally {
      secret.fill(0);
    }
  }

  private async deliver(expected: GroupKey, secret: Uint8Array, device: LocalDevice) {
    const { id, epochId } = expected;
    await this.warmMembership(expected);
    await this.db.all(this.tables.__e2ee_group_deliveries.where({ groupId: id }), { tier: "edge" });
    await this.db.all(this.tables.__e2ee_group_repairs.where({ groupId: id }), { tier: "edge" });
    await this.db.all(this.tables.__e2ee_group_recovery_deliveries.where({ groupId: id }), {
      tier: "edge",
    });
    const prior = await exclusiveE2eeTransaction(this.db, async (tx) => ({
      deliveries: await tx.allSettledForE2ee(
        this.tables.__e2ee_group_deliveries.where({ groupId: id }),
      ),
      repairs: await tx.allSettledForE2ee(this.tables.__e2ee_group_repairs.where({ groupId: id })),
      recovery: await tx.allSettledForE2ee(
        this.tables.__e2ee_group_recovery_deliveries.where({ groupId: id }),
      ),
    }));
    const { deliveries: existing, repairs, recovery } = await prior.wait({ tier: "global" });
    // This snapshot only avoids redundant envelopes. Concurrent delivery rows
    // may cause duplicates, but must not conflict with fresh valid delivery.
    // Membership and device histories are still revalidated by the write below.
    const delivery = await exclusiveE2eeTransaction(this.db, async (tx) => {
      const membership = await this.deviceStates(tx);
      const group = await this.readMembership(tx, expected, membership.publicHistory);
      const roots = group.targetRoots;
      if (roots.rows[0]?.accountId !== expected.accountId)
        throw new Error("E2EE group creator changed");
      const { root, position } = await this.acceptedRoot(
        roots,
        group.histories.get(expected.accountId)!,
      );
      const { members, rotationRequired, keyRoot } = await this.acceptedMembership(
        root,
        position,
        group,
      );
      if (rotationRequired) throw new Error("E2EE group requires rotation after removal");
      if (keyRoot.epochId !== epochId) throw new Error("Staged E2EE group epoch does not match");
      await this.confirmHistory(root, position, group, secret);
      if (!membership.active.has(device.id) || membership.epochId !== members.get(this.accountId))
        throw new Error("E2EE group requires membership reconciliation");
      const delivered = new Set<string>();
      const rejected = new Set<string>();
      for (const request of repairs.rows) {
        const at = repairs.settlements.find((entry) => entry.rowId === request.id)?.position;
        if (await this.acceptedRepair(root, position, group, request, at, existing))
          rejected.add(request.deliveryId);
      }
      for (const row of existing.rows) {
        if (row.epochId !== keyRoot.epochId) continue;
        if (rejected.has(row.id)) continue;
        const at = existing.settlements.find((entry) => entry.rowId === row.id)?.position;
        if (!(await this.acceptedDelivery(root, position, group, row, at))) continue;
        if (row.recipientAccountId === this.accountId && row.recipientDeviceId === device.id) {
          let opened: Uint8Array | undefined;
          try {
            opened = await this.keys
              .open(
                device,
                groupDeliveryContext(this.accountContext(root.accountId), keyRoot, row),
                row.envelope,
              )
              .catch(unavailableGroupKey);
            await this.confirmHistory(root, position, group, opened);
          } catch (error) {
            this.assertOpen();
            if (!(error instanceof UnavailableGroupKey)) throw error;
            // A signature authenticates the sender, not the enclosed key.
            // Retain the staged key until a usable replacement is accepted.
            continue;
          } finally {
            opened?.fill(0);
          }
        }
        delivered.add(JSON.stringify([row.recipientAccountId, row.recipientDeviceId]));
      }
      for (const [recipientAccountId, accountEpochId] of members) {
        const history = group.histories.get(recipientAccountId)!;
        const recipients = await replayAccountMembership(
          history,
          this.accountContext(recipientAccountId),
          this.signer,
        );
        if (recipients.epochId !== accountEpochId)
          throw new Error("E2EE group requires membership reconciliation");
        for (const recoveryRoot of recipients.recoveryRoots) {
          if (
            recoveryRoot.mechanism !== this.keys.mechanism.id ||
            recoveryRoot.version !== this.keys.mechanism.version
          )
            throw new Error("Unsupported E2EE group recovery recipient");
          let delivered = false;
          for (const row of recovery.rows) {
            if (
              row.epochId !== epochId ||
              row.recipientAccountId !== recipientAccountId ||
              row.recoveryRootId !== recoveryRoot.id
            )
              continue;
            const at = recovery.settlements.find((entry) => entry.rowId === row.id)?.position;
            if (await this.acceptedDelivery(root, position, group, row, at)) {
              delivered = true;
              break;
            }
          }
          if (delivered) continue;
          const row = {
            id: crypto.randomUUID(),
            groupId: id,
            epochId,
            senderAccountId: this.accountId,
            senderDeviceId: device.id,
            recipientAccountId,
            recoveryRootId: recoveryRoot.id,
          };
          const envelope = await this.keys.seal(
            recoveryRoot.publicKey,
            groupRecoveryContext(this.accountContext(root.accountId), row),
            secret,
          );
          const signature = await this.checkedSignature(
            device,
            groupRecoveryBytes(this.accountContext(root.accountId), { ...row, envelope }),
          );
          this.assertOpen();
          const { id: rowId, ...values } = row;
          tx.insert(
            this.tables.__e2ee_group_recovery_deliveries,
            { ...values, envelope, signature },
            { id: rowId },
          );
        }
        for (const recipientId of recipients.active) {
          if (delivered.has(JSON.stringify([recipientAccountId, recipientId]))) continue;
          const key = history.keys.rows.find((row) => row.deviceId === recipientId);
          if (
            !key ||
            key.mechanism !== this.keys.mechanism.id ||
            key.version !== this.keys.mechanism.version
          )
            throw new Error("Unsupported E2EE group recipient key");
          const row = {
            id: crypto.randomUUID(),
            groupId: id,
            epochId,
            senderAccountId: this.accountId,
            senderDeviceId: device.id,
            recipientAccountId,
            recipientDeviceId: recipientId,
          };
          const envelope = await this.keys.seal(
            key.publicKey,
            groupDeliveryContext(this.accountContext(root.accountId), keyRoot, row),
            secret,
          );
          if (recipientAccountId === this.accountId && recipientId === device.id) {
            const opened = await this.keys.open(
              device,
              groupDeliveryContext(this.accountContext(root.accountId), keyRoot, row),
              envelope,
            );
            try {
              if (
                opened.length !== secret.length ||
                opened.some((byte, index) => byte !== secret[index])
              )
                throw new Error("Invalid generated E2EE group envelope");
            } finally {
              opened.fill(0);
            }
          }
          const signature = await this.checkedSignature(
            device,
            groupDeliveryBytes(this.accountContext(root.accountId), keyRoot, { ...row, envelope }),
          );
          this.assertOpen();
          const { id: rowId, ...values } = row;
          tx.insert(
            this.tables.__e2ee_group_deliveries,
            { ...values, envelope, signature },
            { id: rowId },
          );
        }
      }
    });
    await delivery.wait({ tier: "global" });
    const staged = await loadStagedGroupKey(this.store, this.application, id, this.assertOpen);
    if (staged) {
      staged.secret.fill(0);
      if (staged.epochId === epochId)
        await stageGroupKey(this.store, this.application, id, epochId, null, this.assertOpen);
    }
  }

  private async requestRepair(expected: GroupKey, device: LocalDevice, failed: string[]) {
    await this.warmMembership(expected);
    await this.db.all(this.tables.__e2ee_group_repairs.where({ groupId: expected.id }), {
      tier: "edge",
    });
    const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
      const own = await this.deviceStates(tx);
      const group = await this.readMembership(tx, expected, own.publicHistory);
      const roots = group.targetRoots;
      const { root, position } = await this.acceptedRoot(
        roots,
        group.histories.get(expected.accountId)!,
      );
      if (root.accountId !== expected.accountId)
        throw new Error("E2EE group changed during repair request");
      const { members, rotationRequired, keyRoot } = await this.acceptedMembership(
        root,
        position,
        group,
      );
      if (keyRoot.epochId !== expected.epochId)
        throw new Error("E2EE group changed during repair request");
      if (rotationRequired) throw new Error("E2EE group requires rotation after removal");
      if (!own.active.has(device.id) || members.get(this.accountId) !== own.epochId)
        throw new Error("E2EE repair requires an active group member device");
      const deliveries = await tx.allSettledForE2ee(
        this.tables.__e2ee_group_deliveries.where({ groupId: root.id }),
      );
      const repairs = await tx.allSettledForE2ee(
        this.tables.__e2ee_group_repairs.where({ groupId: root.id }),
      );
      for (const deliveryId of failed) {
        const delivery = deliveries.rows.find((row) => row.id === deliveryId);
        const at = deliveries.settlements.find((entry) => entry.rowId === deliveryId)?.position;
        if (
          !delivery ||
          delivery.recipientAccountId !== this.accountId ||
          delivery.recipientDeviceId !== device.id ||
          !(await this.acceptedDelivery(root, position, group, delivery, at))
        )
          continue;
        let requested = false;
        for (const request of repairs.rows) {
          if (request.deliveryId !== deliveryId) continue;
          const requestAt = repairs.settlements.find(
            (entry) => entry.rowId === request.id,
          )?.position;
          if (await this.acceptedRepair(root, position, group, request, requestAt, deliveries)) {
            requested = true;
            break;
          }
        }
        if (requested) continue;
        const record = {
          id: crypto.randomUUID(),
          groupId: root.id,
          epochId: keyRoot.epochId,
          deliveryId,
          accountId: this.accountId,
          deviceId: device.id,
          accountEpochId: own.epochId,
        };
        const signature = await this.checkedSignature(
          device,
          groupRepairBytes(this.accountContext(root.accountId), record),
        );
        this.assertOpen();
        const { id, ...values } = record;
        tx.insert(this.tables.__e2ee_group_repairs, { ...values, signature }, { id });
      }
    });
    await proposal.wait({ tier: "global" });
  }

  private async acceptedRepair(
    root: GroupRoot,
    rootPosition: string,
    group: MembershipSnapshot,
    request: GroupRepair,
    at: string | undefined,
    deliveries: { rows: GroupDelivery[]; settlements: RowSettlement[] },
  ): Promise<boolean> {
    if (request.groupId !== root.id) return false;
    if (at === undefined) throw new Error("Missing E2EE repair authority coverage");
    const delivery = deliveries.rows.find((row) => row.id === request.deliveryId);
    const deliveredAt = deliveries.settlements.find(
      (entry) => entry.rowId === request.deliveryId,
    )?.position;
    if (
      !delivery ||
      delivery.recipientAccountId !== request.accountId ||
      delivery.recipientDeviceId !== request.deviceId
    )
      return false;
    if (deliveredAt === undefined) throw new Error("Missing E2EE delivery authority coverage");
    if (
      BigInt(deliveredAt) >= BigInt(at) ||
      !(await this.acceptedDelivery(root, rootPosition, group, delivery, deliveredAt))
    )
      return false;
    const { members, keyRoot, rotationRequired } = await this.acceptedMembership(
      root,
      rootPosition,
      group,
      BigInt(at),
    );
    if (
      rotationRequired ||
      request.epochId !== keyRoot.epochId ||
      delivery.epochId !== request.epochId
    )
      return false;
    if (members.get(request.accountId) !== request.accountEpochId) return false;
    const history = group.histories.get(request.accountId)!;
    const author = await replayAccountMembership(
      historyBefore(history, BigInt(at)),
      this.accountContext(request.accountId),
      this.signer,
    );
    if (author.epochId !== request.accountEpochId || !author.active.has(request.deviceId))
      return false;
    let bytes: Uint8Array;
    try {
      bytes = groupRepairBytes(this.accountContext(root.accountId), request);
    } catch {
      return false;
    }
    return this.verifySigner(history, request.deviceId, bytes, request.signature);
  }

  /** Internal key-free discovery shared with scoped-space membership. */
  async warmMembership(root: Pick<GroupRoot, "id" | "accountId"> | null) {
    const [roots, rows] = await Promise.all([
      this.db.all(this.tables.__e2ee_groups, { tier: "edge" }),
      this.db.all(this.tables.__e2ee_group_membership, { tier: "edge" }),
      this.db.all(this.tables.__e2ee_group_successors, { tier: "edge" }),
    ]);
    const accounts = new Set([this.accountId]);
    if (root) accounts.add(root.accountId);
    for (const group of roots) accounts.add(group.accountId);
    for (const row of rows) {
      accounts.add(row.authorAccountId);
      const recipient = accountMemberId(row);
      if (recipient) accounts.add(recipient);
    }
    await Promise.all(
      [...accounts].map((accountId) =>
        prefetchPublicMembershipHistory(this.db, accountId, this.tables),
      ),
    );
  }

  private async acceptedDelivery(
    root: GroupRoot,
    rootPosition: string,
    group: MembershipSnapshot,
    delivery: GroupDelivery | GroupRecoveryDelivery,
    at: string | undefined,
  ): Promise<boolean> {
    if (delivery.groupId !== root.id) return false;
    if (at === undefined || BigInt(at) <= BigInt(rootPosition))
      throw new Error("Invalid E2EE group delivery authority coverage");
    const { members, keyRoot, rotationRequired } = await this.acceptedMembership(
      root,
      rootPosition,
      group,
      BigInt(at),
    );
    if (rotationRequired || delivery.epochId !== keyRoot.epochId) return false;
    if (!members.has(delivery.senderAccountId) || !members.has(delivery.recipientAccountId))
      return false;
    const senderHistory = group.histories.get(delivery.senderAccountId)!;
    const sender = await replayAccountMembership(
      historyBefore(senderHistory, BigInt(at)),
      this.accountContext(delivery.senderAccountId),
      this.signer,
    );
    const recipient = await replayAccountMembership(
      historyBefore(group.histories.get(delivery.recipientAccountId)!, BigInt(at)),
      this.accountContext(delivery.recipientAccountId),
      this.signer,
    );
    if (
      !sender.active.has(delivery.senderDeviceId) ||
      ("recoveryRootId" in delivery
        ? !recipient.recoveryRoots.some((root) => root.id === delivery.recoveryRootId)
        : !recipient.active.has(delivery.recipientDeviceId)) ||
      sender.epochId !== members.get(delivery.senderAccountId) ||
      recipient.epochId !== members.get(delivery.recipientAccountId)
    )
      return false;
    let bytes: Uint8Array;
    try {
      bytes =
        "recoveryRootId" in delivery
          ? groupRecoveryBytes(this.accountContext(root.accountId), delivery)
          : groupDeliveryBytes(this.accountContext(root.accountId), keyRoot, delivery);
    } catch {
      // A malformed candidate cannot suppress a later authenticated delivery.
      return false;
    }
    return this.verifySigner(senderHistory, delivery.senderDeviceId, bytes, delivery.signature);
  }

  /** Internal snapshot; its enclosing exclusive transaction must be accepted globally. */
  async readMembership(
    tx: E2eeTransactionScope,
    root: Pick<GroupRoot, "id" | "accountId"> | null,
    ownHistory: History,
  ): Promise<MembershipSnapshot> {
    const [roots, records, successors] = await Promise.all([
      tx.allSettledForE2ee(this.tables.__e2ee_groups),
      tx.allSettledForE2ee(this.tables.__e2ee_group_membership),
      tx.allSettledForE2ee(this.tables.__e2ee_group_successors),
    ]);
    const accounts = new Set(root ? [root.accountId] : []);
    for (const group of roots.rows) accounts.add(group.accountId);
    for (const row of records.rows) {
      accounts.add(row.authorAccountId);
      const recipient = accountMemberId(row);
      if (recipient) accounts.add(recipient);
    }
    const histories = new Map([[this.accountId, ownHistory]]);
    await Promise.all(
      [...accounts]
        .filter((accountId) => !histories.has(accountId))
        .map(async (accountId) => {
          histories.set(accountId, await readPublicMembershipHistory(tx, accountId, this.tables));
        }),
    );
    const targetRoots = {
      rows: roots.rows.filter((row) => row.id === root?.id),
      settlements: roots.settlements.filter((entry) => entry.rowId === root?.id),
    };
    return { roots, targetRoots, records, histories, successors };
  }

  private async acceptedMembership(
    root: GroupRoot,
    _rootPosition: string,
    snapshot: MembershipSnapshot,
    cutoff?: bigint,
  ) {
    const graph = await this.acceptedGraph(snapshot, cutoff);
    const state = graph.get(root.id);
    if (!state) throw new Error("Missing accepted E2EE group root");
    return { ...state, graph };
  }

  /** Internal authenticated replay, including the history strictly before a delivery. */
  async acceptedGraph(snapshot: MembershipSnapshot, cutoff?: bigint) {
    const input = {
      snapshot,
      application: this.application,
      contexts: [...snapshot.histories.keys()].map((id) => [id, this.accountContext(id)]),
      mechanism: this.signer.mechanism,
    };
    if (
      !this.validated ||
      this.validated.verify !== this.signer.verify ||
      !sameSnapshotValue(input, this.validated.input)
    ) {
      this.validated = {
        input: structuredClone(input),
        verify: this.signer.verify,
        graphs: new Map(),
      };
    }
    const cache = this.validated;
    const cached = cache.graphs.get(cutoff);
    if (cached) return structuredClone(cached);
    const graph = await this.replayGraph(snapshot, cutoff);
    // Successful public replay only. Coverage and key opening remain live checks.
    // Retain a small cutoff working set, isolated from other concurrent snapshots.
    cache.graphs.set(cutoff, structuredClone(graph));
    if (cache.graphs.size > 8) cache.graphs.delete(cache.graphs.keys().next().value);
    return graph;
  }

  private async replayGraph(snapshot: MembershipSnapshot, cutoff?: bigint) {
    const accounts = new Map<
      string,
      Promise<Awaited<ReturnType<typeof replayAccountMembership>>>
    >();
    const account = (id: string, before?: bigint) => {
      const key = JSON.stringify([id, before?.toString()]);
      let result = accounts.get(key);
      if (!result) {
        const history = snapshot.histories.get(id);
        if (!history) throw new Error("Missing E2EE group account history");
        result = replayAccountMembership(
          before === undefined ? history : historyBefore(history, before),
          this.accountContext(id),
          this.signer,
        );
        accounts.set(key, result);
      }
      return result;
    };
    const graph = await replayGroupGraph(
      snapshot,
      {
        root: async (candidate, at) => {
          const history = snapshot.histories.get(candidate.accountId)!;
          if (!historyBefore(history, at).roots.rows.length) return false;
          let bytes: Uint8Array;
          try {
            bytes = groupRootBytes(this.accountContext(candidate.accountId), candidate);
          } catch {
            return false;
          }
          const creator = await account(candidate.accountId, at);
          return (
            creator.active.has(candidate.deviceId) &&
            creator.epochId === candidate.accountEpochId &&
            this.verifySigner(history, candidate.deviceId, bytes, candidate.signature)
          );
        },
        membership: async (group, row, at) => {
          if (row.memberKind === "account") {
            const recipient = accountMemberId(row);
            if (!recipient) return false;
            // A settled empty history is an ineligible candidate, not a graph
            // failure. Later enrolment must not legitimise the earlier add.
            if (!historyBefore(snapshot.histories.get(recipient)!, at).roots.rows.length)
              return false;
          }
          const history = snapshot.histories.get(row.authorAccountId)!;
          if (!historyBefore(history, at).roots.rows.length) return false;
          let bytes: Uint8Array;
          try {
            bytes = groupMembershipBytes(this.accountContext(group.accountId), row);
          } catch {
            return false;
          }
          const author = await account(row.authorAccountId, at);
          return (
            author.active.has(row.authorDeviceId) &&
            author.epochId === row.authorEpochId &&
            this.verifySigner(history, row.authorDeviceId, bytes, row.signature)
          );
        },
        successor: async (group, row, at) => {
          const history = snapshot.histories.get(row.authorAccountId)!;
          if (!historyBefore(history, at).roots.rows.length) return false;
          let bytes: Uint8Array;
          try {
            bytes = groupSuccessorSigningBytes(this.accountContext(group.accountId), row);
          } catch {
            return false;
          }
          const author = await account(row.authorAccountId, at);
          return (
            author.active.has(row.authorDeviceId) &&
            author.epochId === row.authorEpochId &&
            this.verifySigner(history, row.authorDeviceId, bytes, row.signature)
          );
        },
        accountEpoch: async (id, at) => (await account(id, at)).epochId,
      },
      cutoff,
    );
    return graph;
  }

  private async acceptedRoot(
    roots: { rows: GroupRoot[]; settlements: RowSettlement[] },
    history: History,
  ) {
    const root = roots.rows[0];
    if (!root || roots.rows.length !== 1) throw new Error("Invalid accepted E2EE group root");
    const position = roots.settlements.find((entry) => entry.rowId === root.id)?.position;
    if (!position) throw new Error("Missing E2EE group authority coverage");
    const initial = await replayAccountMembership(
      historyBefore(history, BigInt(position)),
      this.accountContext(root.accountId),
      this.signer,
    );
    if (!initial.active.has(root.deviceId) || initial.epochId !== root.accountEpochId)
      throw new Error("Unauthorised E2EE group creator");
    if (
      !(await this.verifySigner(
        history,
        root.deviceId,
        groupRootBytes(this.accountContext(root.accountId), root),
        root.signature,
      ))
    )
      throw new Error("Invalid E2EE group signature");
    return { root, position };
  }

  private async checkedSignature(device: LocalDevice, bytes: Uint8Array): Promise<Uint8Array> {
    const signature = await this.signer.sign(device.signing.privateKey, bytes);
    if (!(await this.signer.verify(device.signing.publicKey, bytes, signature)))
      throw new Error("Invalid generated E2EE group signature");
    return signature;
  }

  private async checkedWrap(
    secret: Uint8Array,
    context: Uint8Array,
    plaintext: Uint8Array,
  ): Promise<Uint8Array> {
    const envelope = await this.keys.wrap(secret, context, plaintext);
    const opened = await this.keys.unwrap(secret, context, envelope);
    try {
      if (opened.length !== plaintext.length || opened.some((byte, i) => byte !== plaintext[i]))
        throw new Error("Invalid generated E2EE group wrapped value");
      return envelope;
    } finally {
      opened.fill(0);
    }
  }

  private async verifySigner(
    history: History,
    id: string,
    bytes: Uint8Array,
    signature: Uint8Array,
  ) {
    const key = history.keys.rows.find((row) => row.deviceId === id);
    if (
      !key ||
      key.signingMechanism !== this.signer.mechanism.id ||
      key.signingVersion !== this.signer.mechanism.version
    )
      throw new Error("Missing or unsupported E2EE group signer keys");
    return this.signer.verify(key.signingPublicKey, bytes, signature);
  }

  private async confirmHistory(
    root: GroupRoot,
    position: string,
    snapshot: MembershipSnapshot,
    payload: Uint8Array,
  ) {
    let state = await this.acceptedMembership(root, position, snapshot);
    let secret = payload;
    let owned: Uint8Array | undefined;
    try {
      for (;;) {
        await this.confirm(state.keyRoot, secret);
        if (!state.successor) return;
        const successor = state.successor;
        const previous = await this.keys
          .unwrap(
            secret,
            groupSuccessorContext(this.accountContext(root.accountId), successor, "history"),
            successor.history,
          )
          .catch(unavailableGroupKey);
        owned?.fill(0);
        owned = previous;
        secret = previous;
        state = await this.acceptedMembership(root, position, snapshot, state.epochPosition);
        if (state.keyRoot.epochId !== successor.predecessor)
          throw new Error("Invalid E2EE group predecessor history");
      }
    } finally {
      owned?.fill(0);
    }
  }

  private async confirm(root: GroupKey, payload: Uint8Array) {
    if (root.mechanism !== this.keys.mechanism.id || root.version !== this.keys.mechanism.version)
      throw new UnavailableGroupKey("Unsupported E2EE group key mechanism");
    if (payload.length !== 32) throw new UnavailableGroupKey("Invalid E2EE group key");
    const confirmation = await this.keys
      .unwrap(
        payload,
        groupContext(this.accountContext(root.accountId), root, "verification"),
        root.verification,
      )
      .catch(unavailableGroupKey);
    try {
      if (confirmation.length !== 32 || confirmation.some((byte) => byte !== 0))
        throw new UnavailableGroupKey("Invalid E2EE group key confirmation");
    } finally {
      confirmation.fill(0);
    }
  }
}
