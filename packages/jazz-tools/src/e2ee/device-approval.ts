import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db, E2eeTransactionScope } from "../runtime/db.js";
import { PersistedWriteRejectedError, type RowSettlement } from "../runtime/client.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { encodeCryptoContext } from "./context.js";
import { deviceRequestApp as app } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { accountEpochContext, confirmAccountEpoch } from "./first-epoch.js";
import type { LocalDevice } from "./local-device.js";
import type { DeviceSigner, KeyEnvelope } from "./types.js";
import {
  decodeEpochDeliveries,
  decodeEpochIds,
  encodeEpochDeliveries,
  encodeEpochIds,
  encodePublicApprovalRevision,
  successorContext,
  successorSigningBytes,
  publicSuccessorSigningBytes,
  verifySuccessor,
} from "./account-successor.js";
import type { AccountSuccessor } from "./account-successor.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import { readPublicMembershipHistory, replayAccountMembership } from "./public-membership.js";

type EpochSnapshot = Awaited<ReturnType<DeviceApproval["snapshot"]>> & {
  publicState: Awaited<ReturnType<typeof replayAccountMembership>>;
  epochPosition: bigint;
  baseMembers: Set<string>;
  revoked: Set<string>;
  previous?: EpochSnapshot;
  successor?: AccountSuccessor;
};

type Challenge = {
  id: string;
  deviceId: string;
  epochId: string;
  envelope: Uint8Array;
};

const positions = (entries: RowSettlement[]) =>
  new Map(entries.map((entry) => [entry.rowId, BigInt(entry.position)]));
const comparePosition = (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0);

/** Internal account-device handshake. Ordinary Jazz policies own record authorisation. */
export class DeviceApproval {
  private readonly lifetime = new AbortController();
  private readonly responding = new Set<string>();
  private backgroundError: unknown;
  private knownRevoked = false;

  /** A completed authority read proved this device revoked; never an access grant. */
  isKnownRevoked(): boolean {
    return this.knownRevoked;
  }

  constructor(
    private readonly db: Db,
    private readonly accountId: string,
    private readonly application: string,
    private readonly keys: KeyEnvelope,
    private readonly signer: DeviceSigner,
    private readonly assertOpen: () => void,
    private readonly tables: DeviceTables = app,
    private readonly device?: { id: string; load: () => Promise<LocalDevice> },
  ) {
    // Recovery inspection has no device and must never start a handshake responder.
    if (!device) return;
    const stop = db.subscribe(
      this.tables.__e2ee_device_challenges.where({ deviceId: device.id }),
      {
        onUpdate: (rows) => {
          for (const row of rows) {
            if (this.responding.has(row.id)) continue;
            this.responding.add(row.id);
            this.respond(row.id).catch((error: unknown) => {
              this.responding.delete(row.id);
              this.backgroundError = error;
            });
          }
        },
        onError: (error) => {
          this.backgroundError = error;
        },
      },
      { tier: "edge" },
    );
    db.onShutdown(() => {
      this.lifetime.abort();
      stop();
    });
  }

  private get deviceId(): string {
    if (!this.device) throw new Error("Device enrolment is required for this operation");
    return this.device.id;
  }

  private loadDevice(): Promise<LocalDevice> {
    if (!this.device) throw new Error("Device enrolment is required for this operation");
    return this.device.load();
  }

  private context(
    challenge: Pick<Challenge, "id" | "epochId" | "deviceId">,
    column: string,
  ): Uint8Array {
    return encodeCryptoContext({
      application: this.application,
      policy: "jazz.e2ee.device-approval.v1",
      scope: "account",
      identifier: this.accountId,
      table: "__e2ee_device_challenges",
      row: challenge.id,
      column,
      epoch: challenge.epochId,
      recipient: challenge.deviceId,
    });
  }

  private async readSnapshot(tx: E2eeTransactionScope) {
    const [
      successors,
      identities,
      challenges,
      requests,
      proofs,
      approvals,
      deliveries,
      publicHistory,
    ] = await Promise.all([
      tx.allSettledForE2ee(
        this.tables.__e2ee_account_successors.where({ "$createdBy.account": this.accountId }),
      ),
      tx.allSettledForE2ee(this.tables.__e2ee_account_identities.where({ id: this.accountId })),
      tx.allSettledForE2ee(
        this.tables.__e2ee_device_challenges.where({ "$createdBy.account": this.accountId }),
      ),
      tx.allSettledForE2ee(
        this.tables.__e2ee_device_requests.where({ "$createdBy.account": this.accountId }),
      ),
      tx.allSettledForE2ee(
        this.tables.__e2ee_device_proofs.where({ "$createdBy.account": this.accountId }),
      ),
      tx.allSettledForE2ee(
        this.tables.__e2ee_device_approvals.where({ "$createdBy.account": this.accountId }),
      ),
      tx.allSettledForE2ee(
        this.tables.__e2ee_device_deliveries.where({ "$createdBy.account": this.accountId }),
      ),
      readPublicMembershipHistory(tx, this.accountId, this.tables),
    ]);
    this.assertOpen();
    const identity = identities.rows[0];
    if (!identity) throw new Error("Missing accepted E2EE account identity");
    return {
      publicHistory,
      identity,
      successors: successors.rows,
      challenges: challenges.rows,
      requests: requests.rows,
      proofs: proofs.rows,
      approvals: approvals.rows,
      deliveries: deliveries.rows,
      order: {
        identities: positions(identities.settlements),
        successors: positions(successors.settlements),
        challenges: positions(challenges.settlements),
        requests: positions(requests.settlements),
        proofs: positions(proofs.settlements),
        approvals: positions(approvals.settlements),
      },
    };
  }

  private async snapshot(transaction?: E2eeTransactionScope) {
    if (transaction) return this.readSnapshot(transaction);
    // ponytail: scan this account's history; index per-device history if it grows large.
    for (let attempt = 0; ; attempt++) {
      try {
        this.assertOpen();
        await Promise.all([
          this.db.all(this.tables.__e2ee_account_successors, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_account_identities, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_device_requests, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_device_challenges, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_device_proofs, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_device_approvals, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_device_deliveries, { tier: "edge" }),
          this.db.all(this.tables.__e2ee_recovery_roots.where({ accountId: this.accountId }), {
            tier: "edge",
          }),
          this.db.all(this.tables.__e2ee_account_roots.where({ accountId: this.accountId }), {
            tier: "edge",
          }),
          this.db.all(
            this.tables.__e2ee_device_keys.where({ "$createdBy.account": this.accountId }),
            {
              tier: "edge",
            },
          ),
          this.db.all(
            this.tables.__e2ee_public_device_approvals.where({ accountId: this.accountId }),
            {
              tier: "edge",
            },
          ),
          this.db.all(
            this.tables.__e2ee_public_account_successors.where({ accountId: this.accountId }),
            {
              tier: "edge",
            },
          ),
        ]);
        const read = await exclusiveE2eeTransaction(this.db, (tx) => this.readSnapshot(tx));
        const snapshot = await read.wait({ tier: "global" });
        this.assertOpen();
        if (!snapshot.identity) throw new Error("Missing accepted E2EE account identity");
        return { ...snapshot, identity: snapshot.identity };
      } catch (error) {
        // The core can reject the local snapshot before an authority fate exists.
        const conflict =
          (error instanceof PersistedWriteRejectedError &&
            (error.code === "exclusive_conflict" || error.code === "transaction_conflict")) ||
          (error instanceof Error && error.message.startsWith("(transaction_conflict):"));
        if (attempt >= 2 || !conflict) throw error;
      }
    }
  }

  private async marker(key: Uint8Array, context: Uint8Array, envelope: Uint8Array): Promise<void> {
    this.assertOpen();
    const value = await this.keys.unwrap(key, context, envelope);
    try {
      if (value.length !== 32 || value.some((byte) => byte !== 0))
        throw new Error("Invalid E2EE device approval proof");
    } finally {
      value.fill(0);
    }
  }

  private deliveryContext(
    challenge: Pick<Challenge, "id" | "epochId" | "deviceId">,
    envelope: Uint8Array,
  ): Uint8Array {
    if (envelope.length > 0xffffffff) throw new Error("Invalid E2EE delivery length");
    const prefix = this.context(challenge, "delivery-verification");
    const context = new Uint8Array(prefix.length + 4 + envelope.length);
    context.set(prefix);
    new DataView(context.buffer).setUint32(prefix.length, envelope.length, false);
    context.set(envelope, prefix.length + 4);
    return context;
  }

  private proofContext(
    challenge: Pick<Challenge, "id" | "epochId" | "deviceId">,
    proof: Uint8Array,
  ): Uint8Array {
    if (proof.length > 0xffffffff) throw new Error("Invalid E2EE proof length");
    const prefix = this.context(challenge, "proof-signature");
    const record = new Uint8Array(prefix.length + 4 + proof.length);
    record.set(prefix);
    new DataView(record.buffer).setUint32(prefix.length, proof.length, false);
    record.set(proof, prefix.length + 4);
    return record;
  }

  private revisionView(snapshot: EpochSnapshot, successor: AccountSuccessor): EpochSnapshot {
    const cut = snapshot.order.successors.get(successor.id)!;
    return {
      ...snapshot,
      approvals: snapshot.approvals.filter((row) => snapshot.order.approvals.get(row.id)! < cut),
    };
  }

  private members(snapshot: EpochSnapshot, approvals: Set<string>): Set<string> {
    const members = new Set(snapshot.baseMembers);
    for (const challenge of snapshot.challenges)
      if (approvals.has(challenge.id)) members.add(challenge.deviceId);
    return members;
  }

  private async currentSnapshot(transaction?: E2eeTransactionScope): Promise<EpochSnapshot> {
    const raw = await this.snapshot(transaction);
    const publicState = await replayAccountMembership(
      raw.publicHistory,
      this.application,
      this.signer,
    );
    const publicSuccessorPositions = positions(raw.publicHistory.successors.settlements);
    let view: EpochSnapshot = {
      ...raw,
      publicState,
      epochPosition: raw.order.identities.get(raw.identity.id)!,
      baseMembers: new Set([raw.identity.deviceId]),
      revoked: new Set(),
    };
    const visited = new Set([view.identity.epochId]);
    const candidates = [...raw.successors].sort((a, b) =>
      comparePosition(raw.order.successors.get(a.id)!, raw.order.successors.get(b.id)!),
    );
    for (const successor of candidates) {
      if (!publicState.successorIds.has(successor.id)) continue;
      const publicRecord = raw.publicHistory.successors.rows.find(
        (row) => row.id === successor.id,
      )!;
      if (
        publicSuccessorPositions.get(successor.id) !== raw.order.successors.get(successor.id) ||
        publicRecord.accountId !== successor.accountId ||
        publicRecord.epochId !== successor.epochId ||
        publicRecord.predecessor !== successor.predecessor ||
        publicRecord.signerId !== successor.signerId ||
        publicRecord.removedDeviceId !== successor.removedDeviceId ||
        publicRecord.membership.length !== successor.membership.length ||
        !publicRecord.membership.every((byte, i) => byte === successor.membership[i])
      )
        continue;
      if (successor.predecessor !== view.identity.epochId || visited.has(successor.epochId))
        continue;
      const position = raw.order.successors.get(successor.id)!;
      if (position <= view.epochPosition) continue;
      // Competing transitions within one authority transaction have no order.
      if (
        candidates.some(
          (other) =>
            other.id !== successor.id &&
            other.predecessor === successor.predecessor &&
            raw.order.successors.get(other.id) === position,
        )
      )
        continue;
      let revision: string[];
      let recorded: string[];
      try {
        successorSigningBytes(this.application, successor);
        revision = decodeEpochIds(successor.revision);
        recorded = decodeEpochIds(successor.membership);
      } catch {
        // Malformed proposals do not reserve predecessors. Adapter failures below
        // still propagate: inability to verify is not proof of an invalid signature.
        continue;
      }
      if (successor.accountId !== this.accountId) continue;
      const prior = this.revisionView(view, successor);
      const eligible = await this.eligibleApprovals(prior);
      if (revision.length !== eligible.size || revision.some((id) => !eligible.has(id))) continue;
      const members = this.members(prior, eligible);
      const signer = raw.requests.find((row) => row.id === successor.signerId);
      if (
        !signer ||
        !members.has(signer.id) ||
        !members.has(successor.removedDeviceId) ||
        signer.signingMechanism !== this.signer.mechanism.id ||
        signer.signingVersion !== this.signer.mechanism.version ||
        !(await verifySuccessor(this.application, successor, this.signer, signer.signingPublicKey))
      )
        continue;
      members.delete(successor.removedDeviceId);
      if (recorded.length !== members.size || recorded.some((id) => !members.has(id))) continue;
      view = {
        ...view,
        previous: prior,
        successor,
        epochPosition: position,
        baseMembers: members,
        revoked: new Set([...view.revoked, successor.removedDeviceId]),
        identity: {
          ...view.identity,
          epochId: successor.epochId,
          verification: successor.verification,
        },
      };
      visited.add(successor.epochId);
    }
    const privateMembers = this.members(view, await this.eligibleApprovals(view));
    if (
      publicState.epochId !== view.identity.epochId ||
      publicState.active.size !== privateMembers.size ||
      [...publicState.active].some((id) => !privateMembers.has(id))
    )
      throw new Error("Incomplete E2EE public/private membership history");
    // Supplied transactions have not necessarily been accepted yet. Only the
    // standalone snapshot has completed its covered global wait. Revocation of
    // this device ID is permanent, so an older concurrent read cannot undo it.
    if (!transaction && this.device && view.revoked.has(this.device.id)) this.knownRevoked = true;
    return view;
  }

  private async confirmEpoch(snapshot: EpochSnapshot, secret: Uint8Array): Promise<void> {
    if (secret.length !== 32) throw new Error("Invalid E2EE epoch key");
    if (snapshot.successor)
      await this.marker(
        secret,
        successorContext(this.application, snapshot.successor, "verification"),
        snapshot.successor.verification,
      );
    else
      await confirmAccountEpoch(
        this.keys,
        this.application,
        this.accountId,
        snapshot.identity,
        secret,
      );
  }

  private async authenticateHistory(snapshot: EpochSnapshot, secret: Uint8Array): Promise<void> {
    if (!snapshot.successor || !snapshot.previous) return;
    const previousSecret = await this.keys.unwrap(
      secret,
      successorContext(this.application, snapshot.successor, "history"),
      snapshot.successor.history,
    );
    try {
      await this.confirmEpoch(snapshot.previous, previousSecret);
      const prior = this.revisionView(snapshot.previous, snapshot.successor);
      const accepted = await this.eligibleApprovals(prior, previousSecret);
      const revision = decodeEpochIds(snapshot.successor.revision);
      if (revision.length !== accepted.size || revision.some((id) => !accepted.has(id)))
        throw new Error("Unauthenticated E2EE successor ancestry");
      await this.authenticateHistory(snapshot.previous, previousSecret);
    } finally {
      previousSecret.fill(0);
    }
  }

  private async accountKey(snapshot: EpochSnapshot): Promise<Uint8Array | undefined> {
    const eligible = await this.eligibleApprovals(snapshot);
    return this.openAccountKey(snapshot, eligible);
  }

  private approvalContext(
    challenge: Challenge,
    signerId: string,
    verification: Uint8Array,
  ): Uint8Array {
    const prefix = this.context(challenge, `approval-signature:${signerId}`);
    if (verification.length > 0xffffffff) throw new Error("Invalid E2EE approval length");
    const record = new Uint8Array(prefix.length + 4 + verification.length);
    record.set(prefix);
    new DataView(record.buffer).setUint32(prefix.length, verification.length, false);
    record.set(verification, prefix.length + 4);
    return record;
  }

  private async eligibleApprovals(
    snapshot: EpochSnapshot,
    secret?: Uint8Array,
    requireDeliveries = false,
  ): Promise<Set<string>> {
    const active = new Set(snapshot.baseMembers);
    const accepted = new Set<string>();
    const publicPositions = positions(snapshot.publicHistory.approvals.settlements);
    // Grants confer membership at their authority position, not at key delivery.
    const ordered = [...snapshot.approvals].sort((a, b) =>
      comparePosition(snapshot.order.approvals.get(a.id)!, snapshot.order.approvals.get(b.id)!),
    );
    let batch: bigint | undefined;
    let activeBefore = new Set(active);
    for (const grant of ordered) {
      const position = snapshot.order.approvals.get(grant.id)!;
      if (position <= snapshot.epochPosition) continue;
      if (position !== batch) {
        batch = position;
        activeBefore = new Set(active);
      }
      const challenge = snapshot.challenges.find(
        (item) =>
          item.id === grant.id &&
          item.id === grant.challengeId &&
          item.epochId === snapshot.identity.epochId,
      );
      const signer = snapshot.requests.find((item) => item.id === grant.signerId);
      const recipient = snapshot.requests.find((item) => item.id === challenge?.deviceId);
      const proof = snapshot.proofs.find(
        (item) => item.id === grant.id && item.challengeId === grant.id,
      );
      if (!challenge || !signer || !recipient || !proof) continue;
      const publicGrant = snapshot.publicHistory.approvals.rows.find(
        (row) =>
          snapshot.publicState.approvalIds.has(row.id) &&
          publicPositions.get(row.id) === position &&
          row.epochId === challenge.epochId &&
          row.deviceId === challenge.deviceId &&
          row.signerId === grant.signerId,
      );
      if (!publicGrant) continue;
      if (
        !activeBefore.has(grant.signerId) &&
        !(publicGrant.recoveryRootId != null && grant.signerId === recipient.id)
      )
        continue;
      if (snapshot.revoked.has(recipient.id)) continue;
      const delivery = requireDeliveries
        ? snapshot.deliveries.find((item) => item.id === grant.id && item.challengeId === grant.id)
        : undefined;
      if (requireDeliveries && !delivery) continue;
      if (
        snapshot.order.challenges.get(challenge.id)! > position ||
        snapshot.order.proofs.get(proof.id)! > position ||
        snapshot.order.requests.get(signer.id)! > position ||
        snapshot.order.requests.get(recipient.id)! > position
      )
        continue;
      if (
        [signer, recipient].some(
          (item) =>
            item.signingMechanism !== this.signer.mechanism.id ||
            item.signingVersion !== this.signer.mechanism.version,
        )
      )
        continue;
      if (
        !(await this.signer.verify(
          recipient.signingPublicKey,
          this.proofContext(challenge, proof.proof),
          proof.signature,
        ))
      )
        continue;
      if (
        !(await this.signer.verify(
          signer.signingPublicKey,
          this.approvalContext(challenge, grant.signerId, grant.verification),
          grant.signature,
        ))
      )
        continue;
      if (secret) {
        try {
          await this.marker(secret, this.context(challenge, "approval"), grant.verification);
          if (delivery)
            await this.marker(
              secret,
              this.deliveryContext(challenge, delivery.envelope),
              delivery.verification,
            );
        } catch {
          this.assertOpen();
          continue;
        }
      }
      accepted.add(grant.id);
      active.add(challenge.deviceId);
    }
    return accepted;
  }

  private async openAccountKey(
    snapshot: EpochSnapshot,
    eligible: Set<string>,
  ): Promise<Uint8Array | undefined> {
    const { identity } = snapshot;
    const device = await this.loadDevice();
    const open = async (
      context: Uint8Array,
      envelope: Uint8Array,
      challenge?: Challenge,
      verification?: Uint8Array,
      deliveryVerification?: Uint8Array,
    ) => {
      let secret: Uint8Array | undefined;
      try {
        secret = await this.keys.open(device, context, envelope);
        await this.confirmEpoch(snapshot, secret);
        await this.authenticateHistory(snapshot, secret);
        if (challenge) {
          if (!verification || !deliveryVerification)
            throw new Error("Missing E2EE delivery authentication");
          await this.marker(secret, this.context(challenge, "approval"), verification);
          await this.marker(
            secret,
            this.deliveryContext(challenge, envelope),
            deliveryVerification,
          );
          if (!(await this.eligibleApprovals(snapshot, secret, true)).has(challenge.id))
            throw new Error("Unauthenticated E2EE approval ancestry");
        }
        this.assertOpen();
        return secret;
      } catch (error) {
        secret?.fill(0);
        throw error;
      }
    };
    try {
      if (snapshot.successor && snapshot.baseMembers.has(this.deviceId)) {
        const envelope = decodeEpochDeliveries(snapshot.successor.deliveries).get(this.deviceId);
        if (!envelope) throw new Error("Missing E2EE successor delivery");
        return await open(
          successorContext(this.application, snapshot.successor, "delivery", this.deviceId),
          envelope,
        );
      }
      if (!snapshot.successor && identity.deviceId === this.deviceId)
        return await open(
          accountEpochContext(this.application, this.accountId, identity.epochId, this.deviceId),
          identity.envelope,
        );
      for (const challenge of snapshot.challenges) {
        if (challenge.deviceId !== this.deviceId || challenge.epochId !== identity.epochId)
          continue;
        const grant = snapshot.approvals.find(
          (item) => item.id === challenge.id && item.challengeId === challenge.id,
        );
        const delivery = snapshot.deliveries.find(
          (item) => item.id === challenge.id && item.challengeId === challenge.id,
        );
        if (!grant || !delivery || !eligible.has(grant.id)) continue;
        try {
          return await open(
            this.context(challenge, "delivery"),
            delivery.envelope,
            challenge,
            grant.verification,
            delivery.verification,
          );
        } catch {
          this.assertOpen();
        } // An unauthenticated candidate cannot hide a later valid delivery.
      }
      return undefined;
    } catch {
      // Accepted membership is independent of a usable delivery. Authority and
      // device-store reads occur before this boundary and still reject listing.
      this.assertOpen();
      return undefined;
    } finally {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  private throwBackgroundError(): void {
    if (this.backgroundError) {
      const error = this.backgroundError;
      this.backgroundError = undefined;
      throw error;
    }
  }

  /** With a supplied transaction, the caller must await its global acceptance.
   * This path only validates history; it never starts a nested handshake write.
   */
  async deviceStates(transaction?: E2eeTransactionScope) {
    this.throwBackgroundError();
    const snapshot = await this.currentSnapshot(transaction);
    for (const challenge of transaction ? [] : snapshot.challenges) {
      if (
        challenge.deviceId === this.deviceId &&
        !this.responding.has(challenge.id) &&
        !snapshot.proofs.some((proof) => proof.id === challenge.id)
      )
        await this.respond(challenge.id);
    }
    const active = new Set(snapshot.successor ? [] : snapshot.baseMembers);
    const eligible = await this.eligibleApprovals(snapshot);
    const secret = await this.openAccountKey(snapshot, eligible);
    const state = {
      active,
      approved: snapshot.publicState.active,
      verified: new Set<string>(),
      revoked: snapshot.revoked,
      epochId: snapshot.identity.epochId,
      publicHistory: snapshot.publicHistory,
    };
    if (!secret) return state;
    try {
      for (const id of snapshot.baseMembers) active.add(id);
      const authenticated = await this.eligibleApprovals(snapshot, secret, true);
      for (const challenge of snapshot.challenges)
        if (authenticated.has(challenge.id)) active.add(challenge.deviceId);
      for (const id of active) state.verified.add(id);
      this.assertOpen();
      return state;
    } finally {
      secret.fill(0);
    }
  }

  private async respond(challengeId: string): Promise<void> {
    const snapshot = await this.currentSnapshot();
    const challenge = snapshot.challenges.find(
      (item) =>
        item.id === challengeId &&
        item.deviceId === this.deviceId &&
        item.epochId === snapshot.identity.epochId,
    );
    if (!challenge || snapshot.proofs.some((item) => item.id === challengeId)) return;
    const device = await this.loadDevice();
    let secret: Uint8Array | undefined;
    try {
      try {
        secret = await this.keys.open(
          device,
          this.context(challenge, "challenge"),
          challenge.envelope,
        );
      } catch {
        this.assertOpen();
        return;
      }
      const proof = await this.keys.wrap(
        secret,
        this.context(challenge, "proof"),
        new Uint8Array(32),
      );
      const signature = await this.signer.sign(
        device.signing.privateKey,
        this.proofContext(challenge, proof),
      );
      this.assertOpen();
      try {
        await this.db
          .insert(
            this.tables.__e2ee_device_proofs,
            {
              challengeId,
              proof,
              signature,
            },
            { id: challengeId },
          )
          .wait({ tier: "global" });
      } catch (error) {
        const accepted = (await this.currentSnapshot()).proofs.find(
          (item) => item.id === challengeId && item.challengeId === challengeId,
        );
        if (!accepted) throw error;
        // Another context for this same device may already have supplied the proof.
        try {
          await this.marker(secret, this.context(challenge, "proof"), accepted.proof);
          if (
            !(await this.signer.verify(
              device.signing.publicKey,
              this.proofContext(challenge, accepted.proof),
              accepted.signature,
            ))
          )
            throw new Error("Invalid E2EE device signature");
        } catch {
          return;
        } // A forged proof is rejected by the approver, not by device listing.
      }
    } finally {
      secret?.fill(0);
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  private waitForProof(challengeId: string): Promise<void> {
    return new Promise((resolve, reject) => {
      let stop: (() => void) | undefined;
      let finished = false;
      const finish = (error?: unknown) => {
        if (finished) return;
        finished = true;
        stop?.();
        this.lifetime.signal.removeEventListener("abort", abort);
        if (error) reject(error);
        else resolve();
      };
      const abort = () => finish(new Error("E2EE context closed during device approval"));
      this.lifetime.signal.addEventListener("abort", abort, { once: true });
      if (this.lifetime.signal.aborted) {
        abort();
        return;
      }
      try {
        stop = this.db.subscribe(
          this.tables.__e2ee_device_proofs.where({ id: challengeId }),
          {
            onUpdate: (rows) => {
              if (rows.length) finish();
            },
            onError: finish,
          },
          { tier: "edge" },
        );
        if (finished) stop();
      } catch (error) {
        finish(error);
      }
    });
  }

  async revoke(deviceId: string): Promise<void> {
    this.throwBackgroundError();
    const snapshot = await this.currentSnapshot();
    const secret = await this.accountKey(snapshot);
    if (!secret || snapshot.revoked.has(this.deviceId)) {
      secret?.fill(0);
      throw new Error("An active E2EE device must revoke devices");
    }
    const device = await this.loadDevice().catch((error: unknown) => {
      secret.fill(0);
      throw error;
    });
    try {
      const revision = await this.eligibleApprovals(snapshot, secret);
      const members = this.members(snapshot, revision);
      if (!members.has(this.deviceId) || !members.delete(deviceId))
        throw new Error("Unknown or inactive E2EE device");
      const proposal = await exclusiveE2eeTransaction(this.db, async (tx) => {
        const existing = await tx.all(this.tables.__e2ee_account_successors, { tier: "global" });
        if (
          existing.length !== snapshot.successors.length ||
          existing.some((row) => !snapshot.successors.some((old) => old.id === row.id))
        )
          throw new Error("Stale E2EE predecessor");
        // Predicate reads bind the complete immutable approval revision to acceptance.
        const observed = await Promise.all([
          tx.all(this.tables.__e2ee_device_requests, { tier: "global" }),
          tx.all(this.tables.__e2ee_device_challenges, { tier: "global" }),
          tx.all(this.tables.__e2ee_device_proofs, { tier: "global" }),
          tx.all(this.tables.__e2ee_device_approvals, { tier: "global" }),
          tx.all(this.tables.__e2ee_device_deliveries, { tier: "global" }),
        ]);
        const expected = [
          snapshot.requests,
          snapshot.challenges,
          snapshot.proofs,
          snapshot.approvals,
          snapshot.deliveries,
        ];
        const ids = (rows: { id: string }[]) => JSON.stringify(rows.map((row) => row.id).sort());
        if (observed.some((rows, i) => ids(rows) !== ids(expected[i]!)))
          throw new Error("Stale E2EE membership revision");
        const recoveryRoots = await tx.all(
          this.tables.__e2ee_recovery_roots.where({ accountId: this.accountId }),
          { tier: "global" },
        );
        if (ids(recoveryRoots) !== ids(snapshot.publicHistory.recovery.rows))
          throw new Error("Stale E2EE recovery revision");
        const publicApprovals = await tx.all(
          this.tables.__e2ee_public_device_approvals.where({
            accountId: this.accountId,
            epochId: snapshot.identity.epochId,
          }),
          { tier: "global" },
        );
        const nextSecret = runtimeRandomBytes(32);
        try {
          const coordinates = {
            id: crypto.randomUUID(),
            predecessor: snapshot.identity.epochId,
            accountId: this.accountId,
            epochId: crypto.randomUUID(),
          };
          const deliveries = new Map<string, Uint8Array>();
          for (const member of members) {
            const request = snapshot.requests.find((row) => row.id === member);
            if (
              !request ||
              request.mechanism !== this.keys.mechanism.id ||
              request.version !== this.keys.mechanism.version
            )
              throw new Error("Unsupported E2EE successor recipient");
            deliveries.set(
              member,
              await this.keys.seal(
                request.publicKey,
                successorContext(this.application, coordinates, "delivery", member),
                nextSecret,
              ),
            );
          }
          const row = {
            ...coordinates,
            signerId: this.deviceId,
            removedDeviceId: deviceId,
            membership: encodeEpochIds(members),
            revision: encodeEpochIds(revision),
            verification: await this.keys.wrap(
              nextSecret,
              successorContext(this.application, coordinates, "verification"),
              new Uint8Array(32),
            ),
            history: await this.keys.wrap(
              nextSecret,
              successorContext(this.application, coordinates, "history"),
              secret,
            ),
            deliveries: encodeEpochDeliveries(deliveries),
          };
          const record = successorSigningBytes(this.application, row);
          const signature = await this.signer.sign(device.signing.privateKey, record);
          if (!(await this.signer.verify(device.signing.publicKey, record, signature)))
            throw new Error("Invalid E2EE successor signature");
          const publicRow = {
            ...coordinates,
            signerId: row.signerId,
            removedDeviceId: row.removedDeviceId,
            membership: row.membership,
            revision: encodePublicApprovalRevision(publicApprovals.map((approval) => approval.id)),
          };
          const publicRecord = publicSuccessorSigningBytes(this.application, publicRow);
          const publicSignature = await this.signer.sign(device.signing.privateKey, publicRecord);
          if (!(await this.signer.verify(device.signing.publicKey, publicRecord, publicSignature)))
            throw new Error("Invalid E2EE public successor signature");
          this.assertOpen();
          const { id, ...columns } = row;
          const { id: publicId, ...publicColumns } = publicRow;
          tx.insert(
            this.tables.__e2ee_public_account_successors,
            { ...publicColumns, signature: publicSignature },
            { id: publicId },
          );
          return tx.insert(
            this.tables.__e2ee_account_successors,
            { ...columns, signature },
            { id },
          );
        } finally {
          nextSecret.fill(0);
        }
      });
      const accepted = await proposal.wait({ tier: "global" });
      if ((await this.currentSnapshot()).successor?.id !== accepted.id)
        throw new Error("Stale E2EE successor proposal");
      this.assertOpen();
    } finally {
      secret.fill(0);
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }

  async approve(deviceId: string): Promise<void> {
    this.throwBackgroundError();
    const snapshot = await this.currentSnapshot();
    if (snapshot.revoked.has(this.deviceId) || snapshot.revoked.has(deviceId))
      throw new Error("Revoked E2EE device cannot approve or re-enrol");
    const accountKey = await this.accountKey(snapshot);
    if (!accountKey) throw new Error("An active device with the account key must approve devices");
    let challengeSecret: Uint8Array | undefined;
    try {
      challengeSecret = runtimeRandomBytes(32);
      const request = snapshot.requests.find((item) => item.id === deviceId);
      if (!request) throw new Error("Unknown E2EE device request");
      if (
        request.mechanism !== this.keys.mechanism.id ||
        request.version !== this.keys.mechanism.version ||
        request.signingMechanism !== this.signer.mechanism.id ||
        request.signingVersion !== this.signer.mechanism.version
      )
        throw new Error("Unsupported E2EE device mechanism");
      const challenge = { id: crypto.randomUUID(), deviceId, epochId: snapshot.identity.epochId };
      const envelope = await this.keys.seal(
        request.publicKey,
        this.context(challenge, "challenge"),
        challengeSecret,
      );
      this.assertOpen();
      await this.db
        .insert(
          this.tables.__e2ee_device_challenges,
          { deviceId, epochId: challenge.epochId, envelope },
          { id: challenge.id },
        )
        .wait({ tier: "global" });
      await this.waitForProof(challenge.id);
      const proved = await this.currentSnapshot();
      const response = proved.proofs.find(
        (item) => item.id === challenge.id && item.challengeId === challenge.id,
      );
      if (!response || proved.identity.epochId !== challenge.epochId)
        throw new Error("Stale E2EE device approval");
      await this.marker(challengeSecret, this.context(challenge, "proof"), response.proof);
      if (
        !(await this.signer.verify(
          request.signingPublicKey,
          this.proofContext(challenge, response.proof),
          response.signature,
        ))
      )
        throw new Error("Invalid E2EE device signature");
      const authorised = await this.keys.wrap(
        accountKey,
        this.context(challenge, "approval"),
        new Uint8Array(32),
      );
      const signingDevice = await this.loadDevice();
      let signature: Uint8Array;
      const publicApproval = {
        id: crypto.randomUUID(),
        accountId: this.accountId,
        epochId: challenge.epochId,
        deviceId,
        signerId: this.deviceId,
      };
      let publicSignature: Uint8Array;
      try {
        const record = this.approvalContext({ ...challenge, envelope }, this.deviceId, authorised);
        signature = await this.signer.sign(signingDevice.signing.privateKey, record);
        if (!(await this.signer.verify(signingDevice.signing.publicKey, record, signature)))
          throw new Error("Invalid E2EE approving device signature");
        const publicRecord = publicDeviceApprovalBytes(this.application, publicApproval);
        publicSignature = await this.signer.sign(signingDevice.signing.privateKey, publicRecord);
        if (
          !(await this.signer.verify(
            signingDevice.signing.publicKey,
            publicRecord,
            publicSignature,
          ))
        )
          throw new Error("Invalid E2EE public approval signature");
      } finally {
        signingDevice.privateKey.fill(0);
        signingDevice.signing.privateKey.fill(0);
      }
      this.assertOpen();
      const publication = await this.db.transaction((tx) => {
        tx.insert(
          this.tables.__e2ee_device_approvals,
          {
            challengeId: challenge.id,
            verification: authorised,
            signerId: this.deviceId,
            signature,
          },
          { id: challenge.id },
        );
        const { id: publicId, ...publicColumns } = publicApproval;
        tx.insert(
          this.tables.__e2ee_public_device_approvals,
          { ...publicColumns, signature: publicSignature },
          { id: publicId },
        );
      });
      await publication.wait({ tier: "global" });
      // Never put a decryptable account key in the optimistic approval transaction.
      this.assertOpen();
      const accepted = await this.currentSnapshot();
      if (
        accepted.identity.epochId !== challenge.epochId ||
        !(await this.eligibleApprovals(accepted, accountKey)).has(challenge.id)
      )
        throw new Error("Stale or revoked E2EE device approval");
      const delivered = await this.keys.seal(
        request.publicKey,
        this.context(challenge, "delivery"),
        accountKey,
      );
      const deliveryVerification = await this.keys.wrap(
        accountKey,
        this.deliveryContext(challenge, delivered),
        new Uint8Array(32),
      );
      this.assertOpen();
      await this.db
        .insert(
          this.tables.__e2ee_device_deliveries,
          { challengeId: challenge.id, envelope: delivered, verification: deliveryVerification },
          { id: challenge.id },
        )
        .wait({ tier: "global" });
      this.assertOpen();
    } finally {
      accountKey.fill(0);
      challengeSecret?.fill(0);
    }
  }
}
