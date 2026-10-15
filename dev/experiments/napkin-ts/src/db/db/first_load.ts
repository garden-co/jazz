// The local-first opening gate and the host remote-link hint.
import { monotonicMs } from "../../support/runtime.js";
import {
  DurabilityTier,
  SubscriptionPublicationSnapshot,
  scheduleTickIn,
  subscriptionOutputsWithOccurrenceSidecar,
} from "../../_extern/db-rest-wasm.js";

/** How long an opening may wait on a remote link that is still being attempted. */
export const REMOTE_LINK_ATTEMPT_WINDOW_MS = 5000;

export type FirstLoad = { kind: "Deliver" } | { kind: "WaitForRemote"; timeoutMs: number };

function requestedWait(f: FirstLoad | undefined): number | undefined {
  return f?.kind === "WaitForRemote" && f.timeoutMs !== 0 ? f.timeoutMs : undefined;
}

export type RemoteLinkHint = "NoServer" | "Attempting" | "Live" | "Failed";

export function remoteLinkHintFromHostStr(value: string): RemoteLinkHint | undefined {
  switch (value) {
    case "none":
    case "None":
    case "no-server":
    case "NoServer":
      return "NoServer";
    case "attempting":
    case "Attempting":
    case "connecting":
    case "Connecting":
      return "Attempting";
    case "live":
    case "Live":
    case "connected":
    case "Connected":
      return "Live";
    case "failed":
    case "Failed":
    case "unavailable":
    case "Unavailable":
      return "Failed";
  }
  return undefined;
}

type HostLink = { kind: "NoServer" | "Live" | "Failed" } | { kind: "Attempting"; since: number };
type RemoteReach = { kind: "Live" | "Unreachable" } | { kind: "Attempting"; since: number };

export type OpeningRoute = "LocalFirst" | "RemoteWindow";

export interface OpeningGate {
  epoch: number;
  deadline: number;
  route: OpeningRoute;
  withheld: boolean;
  witnessed: boolean;
  witnessAnswered: boolean;
}

export const armedGate = (epoch: number, deadline: number, route: OpeningRoute): OpeningGate => ({
  epoch,
  deadline,
  route,
  withheld: false,
  witnessed: false,
  witnessAnswered: false,
});

/** Still waiting for the answer that may release a withheld opening. */
export const gateAwaitsAnswer = (g: OpeningGate, settled: boolean) =>
  g.witnessed ? !g.witnessAnswered : !settled;

/** Remote reachability shared by every read of one `Db` runtime. */
export class RemoteLinkTracker {
  hint: HostLink | undefined;
  liveUpstreams = 0;
  lossEpoch = 0;
  attemptExpiryObserved = false;
  deadlines: number[] = [];
  gated: WeakRef<any>[] = [];
  witnessed: WeakRef<any>[] = [];
  wakers = new Set<() => void>();
  constructor(readonly scheduler: { current?: any }) {}

  private reach(): RemoteReach {
    const h = this.hint;
    if (!h) return { kind: this.liveUpstreams > 0 ? "Live" : "Unreachable" };
    if (h.kind === "Live") return { kind: "Live" };
    if (h.kind === "Attempting") return h;
    return { kind: "Unreachable" };
  }

  mayWait(epoch: number, deadline: number | undefined): boolean {
    const now = monotonicMs();
    if (epoch !== this.lossEpoch || (deadline !== undefined && now >= deadline)) return false;
    const reach = this.reach();
    if (reach.kind === "Live") return true;
    if (reach.kind === "Attempting") return now < reach.since + REMOTE_LINK_ATTEMPT_WINDOW_MS;
    return false;
  }

  arm(timeoutMs: number): [number, number] | undefined {
    const epoch = this.lossEpoch;
    const deadline = monotonicMs() + timeoutMs;
    if (!this.mayWait(epoch, deadline)) return undefined;
    const reach = this.reach();
    if (reach.kind === "Attempting") this.scheduleAttemptExpiry(reach.since);
    this.deadlines.push(deadline);
    // Round up so the tick observes an elapsed deadline.
    this.scheduler.current?.scheduleTickAfter(Math.floor(timeoutMs) + 1);
    return [epoch, deadline];
  }

  /** Arm a wait bounded only by reachability (no deadline). */
  armUntilLoss(): number | undefined {
    const epoch = this.lossEpoch;
    if (!this.mayWait(epoch, undefined)) return undefined;
    const reach = this.reach();
    if (reach.kind === "Attempting") this.scheduleAttemptExpiry(reach.since);
    return epoch;
  }

  private scheduleAttemptExpiry(since: number): void {
    const remaining = Math.max(0, since + REMOTE_LINK_ATTEMPT_WINDOW_MS - monotonicMs());
    this.scheduler.current?.scheduleTickAfter(Math.floor(remaining) + 1);
  }

  setHint(hint: RemoteLinkHint): void {
    const wasLive = this.reach().kind === "Live";
    const host: HostLink =
      hint === "Attempting" ? { kind: "Attempting", since: monotonicMs() } : { kind: hint };
    this.hint = host;
    if (wasLive && this.reach().kind !== "Live") this.lossEpoch += 1;
    if (host.kind === "Attempting") {
      this.attemptExpiryObserved = false;
      this.scheduleAttemptExpiry(host.since);
    }
    this.notify();
  }

  reportedHint(): RemoteLinkHint | undefined {
    return this.hint?.kind;
  }

  upstreamAttached(): void {
    this.liveUpstreams += 1;
  }

  /** Any own-upstream detach is a link loss for every armed gate. */
  upstreamDetached(): void {
    this.liveUpstreams = Math.max(0, this.liveUpstreams - 1);
    this.lossEpoch += 1;
    this.notify();
  }

  /** Observe an elapsed attempt window or server-wait deadline. */
  onTick(): void {
    const now = monotonicMs();
    const before = this.deadlines.length;
    this.deadlines = this.deadlines.filter((d) => now < d);
    let elapsed = this.deadlines.length !== before;
    let next: number | undefined = this.deadlines.length ? Math.min(...this.deadlines) : undefined;
    const reach = this.reach();
    if (reach.kind === "Attempting" && !this.attemptExpiryObserved) {
      const expiry = reach.since + REMOTE_LINK_ATTEMPT_WINDOW_MS;
      if (now >= expiry) {
        this.attemptExpiryObserved = true;
        elapsed = true;
      } else next = next === undefined ? expiry : Math.min(next, expiry);
    }
    if (elapsed) this.notify();
    // Hosts retain only their earliest timer: always re-arm the next obligation.
    if (next !== undefined)
      this.scheduler.current?.scheduleTickAfter(Math.floor(Math.max(0, next - monotonicMs())) + 1);
  }

  register(state: any): void {
    this.gated.push(new WeakRef(state));
    this.notify();
  }

  /** Release every held opening whose remote can no longer answer. */
  private notify(): void {
    const gated = this.gated;
    this.gated = [];
    let busy = false;
    for (const weak of gated) {
      const state = weak.deref();
      if (!state) continue;
      if (state.borrowed) {
        // Mid-refresh; re-evaluate on the next owner turn.
        busy = true;
        this.gated.push(weak);
        continue;
      }
      const gate = openingGate(state.sender);
      if (!gate) continue;
      if (this.mayWait(gate.epoch, gate.deadline)) this.gated.push(weak);
      else {
        releaseOpeningGate(state);
        busy ||= state.authorityWitness.length > 0;
      }
    }
    if (busy) scheduleTickIn(this.scheduler, "AfterCurrentTurn");
    const wakers = [...this.wakers];
    this.wakers.clear();
    for (const w of wakers) w();
  }

  hasWitnesses(): boolean {
    return this.witnessed.length > 0;
  }
  registerWitness(state: any): void {
    this.witnessed.push(new WeakRef(state));
  }

  /** Resolve the authority witnesses at the end of an owner turn. */
  resolveWitnesses(answered: (handles: any[]) => boolean): any[] {
    const witnessed = this.witnessed;
    this.witnessed = [];
    const retired: any[] = [];
    for (const weak of witnessed) {
      const state = weak.deref();
      if (!state) continue;
      if (state.borrowed) {
        this.witnessed.push(weak);
        continue;
      }
      // Stream finalization retires the witness with the stream.
      if (state.closed.value) continue;
      const gate = openingGate(state.sender);
      if (gate && !gate.witnessAnswered && answered(state.authorityWitness)) {
        gate.witnessAnswered = true;
        if (gate.withheld) releaseOpeningGate(state);
      }
      if (openingGate(state.sender)) this.witnessed.push(weak);
      else retired.push(...state.authorityWitness.splice(0));
    }
    return retired;
  }

  registerWaker(waker: () => void): void {
    this.wakers.add(waker);
  }
}

export function openingGate(sender: any): OpeningGate | undefined {
  return sender.publication.openingGate;
}

/** Deliver a rejection; a withheld local-first opening is published first. */
export function sendRejection(state: any, event: any): void {
  if (openingGate(state.sender)?.route === "LocalFirst") releaseOpeningGate(state);
  state.sender.unboundedSend(event);
}

/** Release an armed opening gate from outside the publication path. */
export function releaseOpeningGate(state: any): void {
  const publication = state.sender.publication;
  const gate: OpeningGate | undefined = publication.openingGate;
  if (!gate) return;
  publication.openingGate = undefined;
  const delta = (reset: boolean, publishable: boolean, added: any[], settled: boolean) => ({
    kind: "Delta",
    reset,
    publishable,
    added,
    updated: [],
    removed: [],
    terminalOperations: [],
    settled,
    tier: state.readTier,
  });
  if (gate.route === "RemoteWindow") {
    if (!publication.opened) {
      publication.windowFellBack = true;
      // Wake the consumer so the stream switches to its fallback.
      state.sender.sender.send(delta(false, false, [], false));
    }
    return;
  }
  if (publication.opened || !gate.withheld || publication.unresolved.size > 0) return;
  let added: any[];
  try {
    const current = SubscriptionPublicationSnapshot.capture(state.snapshot, state.snapshotIndex);
    added = subscriptionOutputsWithOccurrenceSidecar(current.snapshot, current.occurrences);
  } catch {
    return;
  }
  // Keep any pending canonical-reset request.
  publication.opened = true;
  publication.deferred = undefined;
  state.sender.sender.send(delta(true, true, added, state.settled));
}

/** Db::set_remote_link_hint */
export function setRemoteLinkHint(db: any, hint: RemoteLinkHint): void {
  db.node.remoteLink.setHint(hint);
}

/** Resolve a WaitForRemote request: the execution coverage and the gate to install. */
export function resolveFirstLoad(
  db: any,
  prepared: any,
  opts: any,
  authorizationMode: string,
  coverageTier: number,
): [number, OpeningGate | undefined] {
  const timeout = requestedWait(opts.firstLoad);
  if (timeout === undefined) return [coverageTier, undefined];
  if (
    authorizationMode !== "ClientLocal" ||
    opts.tier === "LocalOnly" ||
    coverageTier !== DurabilityTier.Local
  )
    return [coverageTier, undefined];
  const armed = db.node.remoteLink.arm(timeout);
  if (!armed) return [coverageTier, undefined];
  if (prepared.shape.query().offset > 0)
    return [DurabilityTier.Global, armedGate(armed[0], armed[1], "RemoteWindow")];
  return [coverageTier, armedGate(armed[0], armed[1], "LocalFirst")];
}

export function registerOpeningGate(db: any, state: any): void {
  if (state.authorityWitness.length) db.node.remoteLink.registerWitness(state);
  db.node.remoteLink.register(state);
}

/** One-shot WaitForRemote read: local first with a server-wait timeout. */
export async function readLocalFirstWithin<T>(
  db: any,
  timeoutMs: number,
  local: () => Promise<T>,
  remote: () => Promise<T>,
): Promise<T> {
  const armed = timeoutMs > 0 ? db.node.remoteLink.arm(timeoutMs) : undefined;
  if (armed) {
    const result = await raceRemoteAnswer(
      db,
      armed[0],
      armed[1],
      remote().then(
        (v) => ({ ok: true as const, v }),
        () => ({ ok: false as const }),
      ),
    );
    if (result?.ok) return result.v;
  }
  return local();
}

/** Await `remote` until it completes, the remote can no longer answer, or `deadline` passes. */
export function raceRemoteAnswer<T>(
  db: any,
  epoch: number,
  deadline: number | undefined,
  remote: Promise<T>,
): Promise<T | undefined> {
  const tracker: RemoteLinkTracker = db.node.remoteLink;
  return new Promise((resolve) => {
    let done = false;
    remote.then((v) => {
      done = true;
      resolve(v);
    });
    const check = () => {
      if (done) return;
      if (!tracker.mayWait(epoch, deadline)) {
        done = true;
        resolve(undefined);
      } else tracker.registerWaker(check);
    };
    check();
  });
}
