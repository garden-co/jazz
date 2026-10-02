import type { AccountStore } from "../accounts/persistence.js";
import type { Db, E2eeTransactionScope } from "../runtime/db.js";
import { initializationStatus, journalE2eeInitialization } from "../runtime/db.js";
import type { ReservedTxId } from "../runtime/provisional-initialization.js";
import { decodeLocalDeviceStore, type StoredDevices } from "./local-device.js";
import type { SpaceRoot, SpaceGrant } from "./spaces.js";

export class E2eeInitializationNotReady extends Error {
  readonly code = "e2ee_initialization_not_ready";
  readonly retryable = true;
  constructor(message: string) {
    super(message);
    this.name = "E2eeInitializationNotReady";
  }
}
export type FounderProposal = {
  kind: "founder";
  id: string;
  deviceId: string;
  epochId: string;
  envelope: Uint8Array;
  verification: Uint8Array;
  publicKeyId: string;
  rootId: string;
};
type SpaceProposal = { kind: "space"; id: string; root: SpaceRoot; grants: SpaceGrant[] };
type Proposal = FounderProposal | SpaceProposal;
type Entry = {
  scope: string;
  proposal: Proposal;
  reservation?: ReservedTxId;
  local: boolean;
  outcome: "pending" | "accepted" | "rejected" | "interrupted";
  promoted?: boolean;
};
type StoredEntry = Omit<Entry, "proposal"> & { proposal: string };
const founderPublications = new WeakMap<AccountStore, Map<string, Promise<FounderProposal>>>();

// Private JSON journal v1. Only BYTEA values use the explicit e2eeBytesV1 tag;
// envelopes are sealed to the retained device. No data cells or uploads are copied.
function encode(proposal: Proposal): string {
  return JSON.stringify(proposal, (_key, value) =>
    value instanceof Uint8Array ? { e2eeBytesV1: Array.from(value) } : value,
  );
}
function decode(value: string): Proposal {
  const proposal = JSON.parse(value, (_key, item) => {
    if (item && typeof item === "object" && Object.hasOwn(item, "e2eeBytesV1")) {
      if (
        Object.keys(item).length !== 1 ||
        !Array.isArray(item.e2eeBytesV1) ||
        item.e2eeBytesV1.some(
          (byte: unknown) =>
            typeof byte !== "number" || !Number.isInteger(byte) || byte < 0 || byte > 255,
        )
      )
        throw new Error("Invalid E2EE initialization bytes");
      return Uint8Array.from(item.e2eeBytesV1);
    }
    return item;
  }) as Proposal;
  if (
    !proposal ||
    typeof proposal.id !== "string" ||
    (proposal.kind !== "founder" && proposal.kind !== "space")
  )
    throw new Error("Invalid E2EE initialization proposal");
  if (
    proposal.kind === "founder" &&
    (typeof proposal.deviceId !== "string" ||
      typeof proposal.epochId !== "string" ||
      typeof proposal.publicKeyId !== "string" ||
      typeof proposal.rootId !== "string" ||
      !(proposal.envelope instanceof Uint8Array) ||
      !(proposal.verification instanceof Uint8Array))
  )
    throw new Error("Invalid E2EE founder proposal");
  if (
    proposal.kind === "space" &&
    (!proposal.root ||
      !Array.isArray(proposal.grants) ||
      proposal.root.id !== proposal.id ||
      !(proposal.root.authorEnvelope instanceof Uint8Array))
  )
    throw new Error("Invalid E2EE space proposal");
  return proposal;
}
function stored(value: string | null) {
  const state = decodeLocalDeviceStore(value) as StoredDevices & {
    initializationJournalV1?: StoredEntry[];
  };
  const entries = state.initializationJournalV1 ?? [];
  if (!Array.isArray(entries)) throw new Error("Invalid E2EE initialization journal");
  const seen = new Set<string>();
  for (const entry of entries) {
    if (
      !entry ||
      typeof entry.scope !== "string" ||
      typeof entry.proposal !== "string" ||
      typeof entry.local !== "boolean" ||
      !["pending", "accepted", "rejected", "interrupted"].includes(entry.outcome) ||
      (entry.promoted !== undefined && typeof entry.promoted !== "boolean") ||
      (entry.promoted === true && entry.outcome !== "accepted") ||
      (entry.reservation !== undefined && typeof entry.reservation !== "string") ||
      (entry.local && !entry.reservation)
    )
      throw new Error("Invalid E2EE initialization entry");
    const proposal = decode(entry.proposal);
    const key = JSON.stringify([entry.scope, proposal.kind, proposal.id]);
    if (seen.has(key)) throw new Error("Duplicate E2EE initialization proposal");
    seen.add(key);
  }
  return { state, entries };
}

export class InitializationJournal {
  private reconciling?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private closed = false;
  private watching = false;
  private pollDelay = 250;
  private statusRevision = 0;
  private wakeRequested = false;
  private readonly reconciledTerminal = new Set<string>();
  private readonly observedStatuses = new Map<ReservedTxId, string>();
  private readonly completions = new Map<
    string,
    {
      promise: Promise<void>;
      resolve(): void;
      reject(error: unknown): void;
    }
  >();
  constructor(
    private readonly db: Db,
    private readonly store: AccountStore,
    private readonly scope: string,
    private readonly assertOpen: () => void,
    private readonly promote: (proposal: Proposal) => Promise<void>,
  ) {
    db.onShutdown(() => {
      this.closed = true;
      clearTimeout(this.timer);
      for (const completion of this.completions.values())
        completion.reject(new Error("E2EE initialization owner closed before settlement"));
      this.completions.clear();
    });
  }

  /** Reset only for publication/reconnect; ordinary reads must not defeat backoff. */
  wake(reset = false): void {
    if (this.closed) return;
    if (reset) {
      this.pollDelay = 250;
      this.wakeRequested = true;
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.watching || this.timer) return;
    this.wakeRequested = false;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.watching = true;
      const revision = this.statusRevision;
      let pending = false;
      void (async () => {
        // Explicit disconnect has a reconnect wake. Automatic transport outages
        // retain bounded polling because they do not emit that SDK event.
        if (await this.db.e2eeIsExplicitlyOffline()) return;
        pending = await this.settle();
      })()
        .catch((error) => {
          pending = true;
          for (const completion of this.completions.values()) completion.reject(error);
          this.completions.clear();
        })
        .finally(() => {
          this.watching = false;
          if (this.statusRevision !== revision) this.pollDelay = 250;
          else if (!this.wakeRequested) this.pollDelay = Math.min(this.pollDelay * 2, 30_000);
          if (pending || this.wakeRequested) this.wake();
        });
    }, this.pollDelay);
  }
  private async settle(): Promise<boolean> {
    await this.reconcile();
    const entries = await this.entries();
    for (const entry of entries) {
      const completion = this.completions.get(entry.proposal.id);
      if (entry.outcome === "rejected" || entry.outcome === "interrupted") {
        completion?.reject(new Error("E2EE initialization was rejected or interrupted"));
        this.completions.delete(entry.proposal.id);
        continue;
      }
      if (entry.outcome !== "accepted") continue;
      if (!entry.promoted) {
        await this.promote(entry.proposal);
        await this.update(entry.proposal.id, (current) => {
          current.promoted = true;
        });
      }
      completion?.resolve();
      this.completions.delete(entry.proposal.id);
    }
    return entries.some((entry) => entry.outcome === "pending" && !!entry.reservation);
  }

  private completion(id: string): Promise<void> {
    const existing = this.completions.get(id);
    if (existing) return existing.promise;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((accept, refuse) => {
      resolve = accept;
      reject = refuse;
    });
    const completion = { promise, resolve, reject };
    void completion.promise.catch(() => {});
    this.completions.set(id, completion);
    this.wake();
    return completion.promise;
  }

  withFounderPublication(prepare: () => Promise<FounderProposal>): Promise<FounderProposal> {
    let scopes = founderPublications.get(this.store);
    if (!scopes) founderPublications.set(this.store, (scopes = new Map()));
    const existing = scopes.get(this.scope);
    if (existing) return existing;
    const pending = prepare().finally(() => scopes!.delete(this.scope));
    scopes.set(this.scope, pending);
    return pending;
  }
  async entries(): Promise<Entry[]> {
    const { entries } = stored(await this.store.read());
    this.assertOpen();
    return entries
      .filter((entry) => entry.scope === this.scope)
      .map((entry) => ({ ...entry, proposal: decode(entry.proposal) }));
  }

  async claimFounder(proposal: FounderProposal): Promise<FounderProposal> {
    let selected: FounderProposal | undefined;
    await this.store.update((value) => {
      this.assertOpen();
      const { state, entries } = stored(value);
      const found = entries.find(
        (entry) => entry.scope === this.scope && decode(entry.proposal).kind === "founder",
      );
      if (found) {
        selected = decode(found.proposal) as FounderProposal;
        if (selected.id !== proposal.id) throw new Error("Founder account mismatch");
        if (found.outcome === "rejected" || found.outcome === "interrupted")
          throw new E2eeInitializationNotReady("The original founder proposal cannot be replayed");
        if (selected.deviceId !== proposal.deviceId) throw new Error("Founder device mismatch");
      } else {
        selected = proposal;
        entries.push({
          scope: this.scope,
          proposal: encode(proposal),
          local: false,
          outcome: "pending",
        });
      }
      state.initializationJournalV1 = entries;
      return JSON.stringify(state);
    });
    if (!selected) throw new Error("Initialization store did not perform its atomic update");
    return selected;
  }

  private async update(id: string, change: (entry: StoredEntry) => void): Promise<void> {
    let updated = false;
    await this.store.update((value) => {
      this.assertOpen();
      const { state, entries } = stored(value);
      const entry = entries.find(
        (entry) => entry.scope === this.scope && decode(entry.proposal).id === id,
      );
      if (!entry) throw new Error("Missing E2EE initialization journal entry");
      change(entry);
      state.initializationJournalV1 = entries;
      updated = true;
      return JSON.stringify(state);
    });
    if (!updated) throw new Error("Initialization store did not perform its atomic update");
    this.assertOpen();
  }

  stage(tx: E2eeTransactionScope, proposal: Proposal): void {
    journalE2eeInitialization(tx, {
      sealed: async (reservation) => {
        let updated = false;
        await this.store.update((value) => {
          this.assertOpen();
          const { state, entries } = stored(value);
          let entry = entries.find(
            (entry) => entry.scope === this.scope && decode(entry.proposal).id === proposal.id,
          );
          if (entry?.reservation && entry.reservation !== reservation)
            throw new E2eeInitializationNotReady(
              "The original initialization transaction already exists",
            );
          if (!entry) {
            entry = {
              scope: this.scope,
              proposal: encode(proposal),
              local: false,
              outcome: "pending",
            };
            entries.push(entry);
          }
          if (entry.outcome !== "pending")
            throw new E2eeInitializationNotReady("Initialization is terminal");
          entry.reservation = reservation;
          state.initializationJournalV1 = entries;
          updated = true;
          return JSON.stringify(state);
        });
        if (!updated) throw new Error("Initialization store did not perform its atomic update");
        this.assertOpen();
        this.wake(true);
      },
      local: async () => {
        await this.update(proposal.id, (entry) => {
          entry.local = true;
        });
        this.wake(true);
      },
      completion: () => this.completion(proposal.id),
    });
  }

  reconcile(): Promise<void> {
    return (this.reconciling ??= this.reconcileNow().finally(() => {
      this.reconciling = undefined;
    }));
  }
  private async reconcileNow(): Promise<void> {
    const entries = (await this.entries()).filter(
      (entry) =>
        entry.reservation &&
        // Verified promotion transfers ongoing authorization to accepted history.
        // Its retained linkage is not pending work owned by a newly opened Db.
        !(entry.outcome === "accepted" && entry.promoted) &&
        (entry.outcome === "pending" || !this.reconciledTerminal.has(entry.proposal.id)),
    );
    for (let offset = 0; offset < entries.length; offset += 64) {
      const batch = entries.slice(offset, offset + 64);
      const statuses = await initializationStatus(
        this.db,
        batch.map((entry) => entry.reservation!),
      );
      if (statuses.length !== batch.length)
        throw new Error("Incomplete initialization status response");
      for (let index = 0; index < batch.length; index++) {
        const entry = batch[index]!;
        const status = statuses[index]!;
        if (status.reservedTxId !== entry.reservation)
          throw new Error("Initialization status identity mismatch");
        const observed =
          status.kind === "complete"
            ? `${status.kind}:${status.fate.kind}:${status.durability}`
            : status.kind;
        const previous = this.observedStatuses.get(status.reservedTxId);
        this.observedStatuses.set(status.reservedTxId, observed);
        if (previous !== undefined && previous !== observed) {
          this.statusRevision++;
          this.wake(true);
        }
        if (status.kind === "not-observed") {
          if (entry.local)
            throw new Error("Corrupt initialization: acknowledged durable transaction is missing");
          // It may still belong to a live unpublished owner. Do not cancel it,
          // reuse its reservation, or manufacture a durable receipt.
        } else if (status.kind === "complete") {
          if (
            (entry.outcome === "accepted" || entry.outcome === "rejected") &&
            entry.outcome !== status.fate.kind
          )
            throw new Error("Corrupt initialization: terminal authority outcome changed");
          const local = status.durability !== "none";
          if ((local && !entry.local) || status.fate.kind !== entry.outcome) {
            await this.update(entry.proposal.id, (current) => {
              if (local) current.local = true;
              if (status.fate.kind !== "pending") current.outcome = status.fate.kind;
            });
            this.statusRevision++;
            this.wake(true);
          }
          if (status.fate.kind !== "pending") {
            this.reconciledTerminal.add(entry.proposal.id);
            this.observedStatuses.delete(status.reservedTxId);
          }
        } else if (entry.local)
          throw new Error("Corrupt initialization: acknowledged transaction is incomplete");
      }
    }
  }

  async waitForFounderAcceptance(): Promise<void> {
    this.assertOpen();
    await this.founder();
    const entry = (await this.entries()).find((entry) => entry.proposal.kind === "founder");
    if (!entry || entry.outcome === "accepted") return;
    if (!entry.local || (await this.db.e2eeIsExplicitlyOffline()))
      throw new E2eeInitializationNotReady("The original founder is not accepted while offline");
    await this.completion(entry.proposal.id);
  }

  async founder(): Promise<FounderProposal | undefined> {
    await this.reconcile();
    this.wake();
    const entry = (await this.entries()).find((entry) => entry.proposal.kind === "founder");
    if (!entry) return undefined;
    if (entry.outcome === "rejected" || entry.outcome === "interrupted")
      throw new E2eeInitializationNotReady("The founder proposal was rejected or interrupted");
    return entry.proposal as FounderProposal;
  }

  async space(id: string): Promise<SpaceProposal | undefined> {
    await this.reconcile();
    this.wake();
    const entries = await this.entries();
    const founder = entries.find((entry) => entry.proposal.kind === "founder");
    if (founder && (founder.outcome === "rejected" || founder.outcome === "interrupted"))
      throw new E2eeInitializationNotReady("The founder proposal no longer permits dependent use");
    const entry = entries.find(
      (entry) => entry.proposal.kind === "space" && entry.proposal.id === id,
    );
    if (!entry) return undefined;
    // Terminal losing material cannot veto independently accepted winning history.
    if (entry.outcome === "rejected" || entry.outcome === "interrupted") return undefined;
    if (!entry.local)
      throw new E2eeInitializationNotReady("Space initialization is not locally durable");
    // Accepted roots must use normal accepted-history verification and revocation.
    return entry.outcome === "accepted" ? undefined : (entry.proposal as SpaceProposal);
  }
}
