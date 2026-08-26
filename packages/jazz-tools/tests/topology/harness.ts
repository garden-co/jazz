export type TopologyKind = "core" | "edge" | "browser" | "native" | "fixture";

export type TopologyFaultKind = "disconnect" | "reconnect" | "restart" | "failure";

export interface TopologyCleanupContext {
  signal: AbortSignal;
}

export type TopologyCompensation = (context: TopologyCleanupContext) => Promise<void>;

export interface TopologyOperationContext {
  signal: AbortSignal;
  /**
   * Register a named, idempotent inverse for external state acquired by this
   * phase or fault. Remaining compensations run in reverse registration order
   * before scenario cleanup, even when a later operation fails. Registration
   * is valid only while this operation is running.
   */
  defer(name: string, cleanup: TopologyCompensation): void;
}

export interface TopologyFaultTarget {
  disconnect?: (context: TopologyOperationContext) => Promise<void>;
  reconnect?: (context: TopologyOperationContext) => Promise<void>;
  restart?: (context: TopologyOperationContext) => Promise<void>;
  failure?: (context: TopologyOperationContext) => Promise<void>;
}

export interface TopologyFault {
  kind: TopologyFaultKind;
  target: string;
  timeoutMs?: number;
}

export interface TopologyPhaseContext extends TopologyOperationContext {
  seed: number;
  random: () => number;
}

export interface TopologyPhase {
  name: string;
  run: (context: TopologyPhaseContext) => Promise<void>;
  faultsAfter?: readonly TopologyFault[];
  timeoutMs?: number;
}

export interface TopologyScenario {
  id: string;
  topology: readonly TopologyKind[];
  seed: number;
  phaseTimeoutMs: number;
  faultTimeoutMs: number;
  targets: Readonly<Record<string, TopologyFaultTarget>>;
  phases: readonly TopologyPhase[];
  replay: string;
  /**
   * Test-only transport schedulers owned by this scenario. The runner closes
   * them after app cleanup so a held packet cannot leak into a later test.
   */
  envelopeSchedulers?: readonly TopologyEnvelopeScheduler[];
  envelopeCleanupTimeoutMs?: number;
  cleanup?: (context: TopologyCleanupContext) => Promise<void>;
  cleanupTimeoutMs?: number;
}

export type TopologyActivityStatus = "attempted" | "completed" | "failed";

export interface TopologyActivityReceipt {
  status: TopologyActivityStatus;
  elapsedMs: number;
  error?: string;
}

export interface TopologyReceipt {
  schemaVersion: 1;
  scenario: string;
  topology: readonly TopologyKind[];
  seed: number;
  status: "passed" | "failed";
  elapsedMs: number;
  phases: Array<TopologyActivityReceipt & { name: string }>;
  faults: Array<TopologyActivityReceipt & { kind: TopologyFaultKind; target: string }>;
  compensations: Array<TopologyActivityReceipt & { name: string }>;
  envelopes: TopologyEnvelopeSchedulerReceipt[];
  cleanup?: TopologyActivityReceipt;
  replay: string;
  error?: string;
}

export interface TopologyReporter {
  phase(status: "start" | "complete" | "failed", label: string, elapsedMs: number): void;
}

const noReporter: TopologyReporter = { phase() {} };

/** Run app-owned phases with shared deterministic fault and timeout plumbing. */
export async function runTopologyScenario(
  scenario: TopologyScenario,
  reporter: TopologyReporter = noReporter,
): Promise<TopologyReceipt> {
  const receipt: TopologyReceipt = {
    schemaVersion: 1,
    scenario: scenario.id,
    topology: scenario.topology,
    seed: scenario.seed,
    status: "passed",
    elapsedMs: 0,
    phases: [],
    faults: [],
    compensations: [],
    envelopes: scenario.envelopeSchedulers?.map((scheduler) => scheduler.receipt()) ?? [],
    replay: scenario.replay,
  };
  const scenarioStarted = now();
  const random = deterministicRandom(scenario.seed);
  let scenarioError: unknown;
  let compensationsClosed = false;
  const compensations: Array<{ name: string; cleanup: TopologyCompensation }> = [];
  const registerCompensation = (name: string, cleanup: TopologyCompensation): void => {
    if (compensationsClosed) {
      throw new Error(`topology compensation registered after cleanup: ${name}`);
    }
    if (typeof name !== "string" || !name.trim()) {
      throw new Error("topology compensation name must not be empty");
    }
    if (typeof cleanup !== "function") {
      throw new Error(`topology compensation ${name} must be a function`);
    }
    compensations.push({ name, cleanup });
  };
  try {
    for (const phase of scenario.phases) {
      const started = now();
      const activity: TopologyReceipt["phases"][number] = {
        name: phase.name,
        status: "attempted",
        elapsedMs: 0,
      };
      receipt.phases.push(activity);
      reporter.phase("start", phase.name, 0);
      try {
        const registrar = createOperationRegistrar(`phase ${phase.name}`, registerCompensation);
        try {
          await withTopologyTimeout(
            (signal) => phase.run({ seed: scenario.seed, random, signal, defer: registrar.defer }),
            phase.timeoutMs ?? scenario.phaseTimeoutMs,
            `topology phase timed out: ${phase.name}`,
          );
        } finally {
          registrar.close();
        }
        const elapsedMs = elapsed(started);
        Object.assign(activity, { status: "completed", elapsedMs });
        reporter.phase("complete", phase.name, elapsedMs);
      } catch (error) {
        Object.assign(activity, {
          status: "failed",
          elapsedMs: elapsed(started),
          error: errorMessage(error),
        });
        reporter.phase("failed", phase.name, activity.elapsedMs);
        throw error;
      }
      for (const fault of phase.faultsAfter ?? []) {
        const started = now();
        const activity: TopologyReceipt["faults"][number] = {
          kind: fault.kind,
          target: fault.target,
          status: "attempted",
          elapsedMs: 0,
        };
        receipt.faults.push(activity);
        try {
          const operation = scenario.targets[fault.target]?.[fault.kind];
          if (!operation) {
            throw new Error(`topology target ${fault.target} does not support ${fault.kind}`);
          }
          const registrar = createOperationRegistrar(
            `fault ${fault.kind} ${fault.target}`,
            registerCompensation,
          );
          try {
            await withTopologyTimeout(
              (signal) => operation({ signal, defer: registrar.defer }),
              fault.timeoutMs ?? scenario.faultTimeoutMs,
              `topology fault timed out: ${fault.kind} ${fault.target}`,
            );
          } finally {
            registrar.close();
          }
          Object.assign(activity, { status: "completed", elapsedMs: elapsed(started) });
        } catch (error) {
          Object.assign(activity, {
            status: "failed",
            elapsedMs: elapsed(started),
            error: errorMessage(error),
          });
          throw error;
        }
      }
    }
  } catch (error) {
    receipt.status = "failed";
    receipt.error = errorMessage(error);
    scenarioError = error;
  } finally {
    compensationsClosed = true;
    for (const compensation of [...compensations].reverse()) {
      const started = now();
      const activity: TopologyReceipt["compensations"][number] = {
        name: compensation.name,
        status: "attempted",
        elapsedMs: 0,
      };
      receipt.compensations.push(activity);
      try {
        await withTopologyCompensationTimeout(
          (signal) => compensation.cleanup({ signal }),
          scenario.cleanupTimeoutMs ?? scenario.faultTimeoutMs,
          `topology compensation timed out: ${compensation.name}`,
        );
        Object.assign(activity, { status: "completed", elapsedMs: elapsed(started) });
      } catch (cleanupError) {
        const message = `compensation ${compensation.name} failed: ${errorMessage(cleanupError)}`;
        Object.assign(activity, {
          status: "failed",
          elapsedMs: elapsed(started),
          error: errorMessage(cleanupError),
        });
        receipt.status = "failed";
        receipt.error = receipt.error ? `${receipt.error}; ${message}` : message;
        scenarioError ??= cleanupError;
      }
    }
    if (scenario.cleanup) {
      const started = now();
      const activity: TopologyActivityReceipt = { status: "attempted", elapsedMs: 0 };
      receipt.cleanup = activity;
      try {
        // Scenario cleanup can also release external state. Do not return to
        // the next scenario while an abort-ignoring cleanup can still mutate.
        await withTopologyCompensationTimeout(
          (signal) => scenario.cleanup!({ signal }),
          scenario.cleanupTimeoutMs ?? scenario.faultTimeoutMs,
          "topology scenario cleanup timed out",
        );
        Object.assign(activity, { status: "completed", elapsedMs: elapsed(started) });
      } catch (cleanupError) {
        const message = `cleanup failed: ${errorMessage(cleanupError)}`;
        Object.assign(activity, {
          status: "failed",
          elapsedMs: elapsed(started),
          error: errorMessage(cleanupError),
        });
        receipt.status = "failed";
        receipt.error = receipt.error ? `${receipt.error}; ${message}` : message;
        scenarioError ??= cleanupError;
      }
    }
    for (const scheduler of scenario.envelopeSchedulers ?? []) {
      try {
        await scheduler.close(scenario.envelopeCleanupTimeoutMs ?? scenario.faultTimeoutMs);
      } catch (closeError) {
        const message = `envelope scheduler cleanup failed: ${errorMessage(closeError)}`;
        receipt.status = "failed";
        receipt.error = receipt.error ? `${receipt.error}; ${message}` : message;
        scenarioError ??= closeError;
      }
    }
    receipt.envelopes = scenario.envelopeSchedulers?.map((scheduler) => scheduler.receipt()) ?? [];
    receipt.elapsedMs = elapsed(scenarioStarted);
  }
  if (receipt.status === "failed") throw new TopologyScenarioError(receipt, scenarioError);
  return receipt;
}

/** A small, app-owned description of a message at a test transport boundary. */
export interface TopologyTransportEnvelope {
  readonly from: string;
  readonly to: string;
  /** Stable test-only label; never inspect production payloads just to fault them. */
  readonly label?: string;
}

export interface TopologyEnvelopeDeliveryContext {
  envelopeId: number;
  attempt: number;
  tick: number;
  sequence: number;
  signal: AbortSignal;
  /**
   * Enqueue a follow-up from this delivery without waiting on the drain which
   * is currently awaiting this callback. Valid only until the callback settles.
   */
  intercept<T>(
    envelope: TopologyTransportEnvelope,
    value: T,
    deliver: TopologyEnvelopeDelivery<T>,
  ): Promise<void>;
}

export type TopologyEnvelopeDelivery<T> = (
  value: T,
  context: TopologyEnvelopeDeliveryContext,
) => Promise<void> | void;

export type TopologyEnvelopeAction =
  | "queued"
  | "delivered"
  | "duplicated"
  | "delayed"
  | "reordered"
  | "dropped"
  | "retried"
  | "partitioned"
  | "healed"
  | "discarded"
  | "deliveryFailed"
  | "deliveryAborted"
  | "closeTimedOut";

export interface TopologyEnvelopeActivity {
  action: TopologyEnvelopeAction;
  envelopeId?: number;
  sequence?: number;
  from?: string;
  to?: string;
  label?: string;
  tick: number;
  attempt?: number;
  error?: string;
}

export interface TopologyEnvelopeSchedulerReceipt {
  seed: number;
  tick: number;
  closed: boolean;
  pending: number;
  inFlight: number;
  cleanup?: TopologyActivityReceipt;
  activities: readonly TopologyEnvelopeActivity[];
}

interface PendingEnvelope<T> {
  envelopeId: number;
  sequence: number;
  envelope: TopologyTransportEnvelope;
  value: T;
  deliver: TopologyEnvelopeDelivery<T>;
  dueTick: number;
  attempt: number;
  retry: boolean;
}

interface NextEnvelopeFault {
  duplicate: number;
  delayTicks: number;
  reorder: boolean;
  dropThenRetryTicks: number | undefined;
}

interface InFlightEnvelope {
  pending: PendingEnvelope<unknown>;
  controller: AbortController;
  promise: Promise<void>;
}

/**
 * Deterministic, virtual-time transport envelope scheduler for example E2Es.
 *
 * Wrap only an app test transport's delivery callback with `intercept`; no
 * Jazz runtime transport is altered. Normal envelopes deliver immediately.
 * Faults are armed before the next matching envelope, then `advance`/`heal`
 * releases held work in a deterministic order. Its receipt is intentionally
 * payload-free, making it safe to include in scenario reports and replays.
 */
export class TopologyEnvelopeScheduler {
  readonly seed: number;
  #tick = 0;
  #sequence = 0;
  #envelopeId = 0;
  #closed = false;
  #pending: PendingEnvelope<unknown>[] = [];
  #heldForReorder: PendingEnvelope<unknown> | undefined;
  #partitions = new Set<string>();
  #next: NextEnvelopeFault = {
    duplicate: 0,
    delayTicks: 0,
    reorder: false,
    dropThenRetryTicks: undefined,
  };
  #activities: TopologyEnvelopeActivity[] = [];
  #inFlight = new Map<number, InFlightEnvelope>();
  #closePromise: Promise<void> | undefined;
  #cleanup: TopologyActivityReceipt | undefined;
  #pumpPromise: Promise<void> | undefined;
  #failure: unknown;

  constructor(seed: number) {
    if (!Number.isSafeInteger(seed) || seed < 0 || seed > 0xffff_ffff) {
      throw new Error("topology envelope scheduler seed must be a uint32");
    }
    this.seed = seed;
  }

  /** Deliver the next matching envelope more than once (one duplicate by default). */
  duplicateNext(copies = 1): void {
    this.assertOpen();
    this.assertArmedOnly("duplicate");
    if (!Number.isSafeInteger(copies) || copies < 1 || copies > MAX_TOPOLOGY_COPIES)
      throw new Error(`duplicate copies must be a positive integer at most ${MAX_TOPOLOGY_COPIES}`);
    if (this.#next.duplicate + copies > MAX_TOPOLOGY_COPIES)
      throw new Error(`duplicate copies must not exceed ${MAX_TOPOLOGY_COPIES}`);
    this.#next.duplicate += copies;
  }

  /** Hold the next matching envelope for virtual `ticks`; `advance` releases it. */
  delayNext(ticks = 1): void {
    this.assertOpen();
    this.assertArmedOnly("delay");
    assertTopologyTicks("delay", ticks);
    this.#next.delayTicks = ticks;
  }

  /** Reverse the next pair of envelopes, retaining the first until the second arrives. */
  reorderNext(): void {
    this.assertOpen();
    this.assertArmedOnly("reorder");
    this.#next.reorder = true;
  }

  /** Drop the next envelope, then make exactly one retry available after virtual `ticks`. */
  dropNextThenRetry(ticks = 1): void {
    this.assertOpen();
    this.assertArmedOnly("drop-then-retry");
    assertTopologyTicks("retry", ticks);
    this.#next.dropThenRetryTicks = ticks;
  }

  /** Block both directions between two named endpoints until `heal` is called. */
  partition(first: string, second: string): void {
    this.assertOpen();
    assertEndpoint("first partition endpoint", first);
    assertEndpoint("second partition endpoint", second);
    if (
      !this.#partitions.has(linkKey(first, second)) &&
      this.#partitions.size >= MAX_TOPOLOGY_PARTITIONS
    ) {
      throw new Error(
        `topology envelope scheduler partition capacity is ${MAX_TOPOLOGY_PARTITIONS}`,
      );
    }
    this.#partitions.add(linkKey(first, second));
    this.record({ action: "partitioned", from: first, to: second });
  }

  /** Heal one link (or all links) and immediately deliver every now-ready envelope. */
  async heal(first?: string, second?: string): Promise<void> {
    this.assertOpen();
    if ((first === undefined) !== (second === undefined)) {
      throw new Error("heal requires both endpoints or neither");
    }
    if (first !== undefined) {
      assertEndpoint("first heal endpoint", first);
      assertEndpoint("second heal endpoint", second!);
    }
    if (first === undefined) this.#partitions.clear();
    else this.#partitions.delete(linkKey(first, second!));
    this.record({ action: "healed", from: first, to: second });
    await this.pump();
  }

  /** Advance deterministic virtual time and release due, non-partitioned envelopes. */
  async advance(ticks = 1): Promise<void> {
    this.assertOpen();
    assertTopologyTicks("advance", ticks);
    if (this.#tick + ticks > MAX_TOPOLOGY_TICKS) {
      throw new Error(`topology virtual time must not exceed ${MAX_TOPOLOGY_TICKS} ticks`);
    }
    this.#tick += ticks;
    await this.pump();
  }

  /**
   * Intercept one transport-envelope delivery. The caller retains ownership of
   * transport connection/retry semantics; this models only message delivery.
   */
  async intercept<T>(
    envelope: TopologyTransportEnvelope,
    value: T,
    deliver: TopologyEnvelopeDelivery<T>,
  ): Promise<void> {
    await this.interceptInternal(envelope, value, deliver, false);
  }

  private async interceptInternal<T>(
    envelope: TopologyTransportEnvelope,
    value: T,
    deliver: TopologyEnvelopeDelivery<T>,
    reentrant: boolean,
  ): Promise<void> {
    this.assertOpen();
    const descriptor = snapshotEnvelope(envelope);
    const pending: PendingEnvelope<T> = {
      envelopeId: this.allocateEnvelopeId(),
      sequence: this.allocateSequence(),
      envelope: descriptor,
      value,
      deliver,
      dueTick: this.#tick,
      attempt: 1,
      retry: false,
    };
    this.recordPending("queued", pending);
    const fault = this.takeNextFault();
    if (fault.dropThenRetryTicks !== undefined) {
      this.recordPending("dropped", pending);
      pending.attempt = 2;
      pending.retry = true;
      pending.dueTick += fault.dropThenRetryTicks;
      this.ensureCapacity(1);
      this.#pending.push(pending as PendingEnvelope<unknown>);
    } else if (this.#heldForReorder || fault.reorder) {
      if (!this.#heldForReorder) {
        this.ensureCapacity(1);
        this.#heldForReorder = pending as PendingEnvelope<unknown>;
        this.recordPending("reordered", pending);
      } else {
        this.recordPending("reordered", pending);
        this.ensureCapacity(2);
        this.#pending.push(pending as PendingEnvelope<unknown>, this.#heldForReorder);
        this.#heldForReorder = undefined;
      }
    } else {
      pending.dueTick += fault.delayTicks;
      if (fault.delayTicks) this.recordPending("delayed", pending);
      this.ensureCapacity(1 + fault.duplicate);
      this.#pending.push(pending as PendingEnvelope<unknown>);
      for (let copy = 0; copy < fault.duplicate; copy++) {
        const duplicate = {
          ...pending,
          sequence: this.allocateSequence(),
          attempt: copy + 2,
          retry: false,
        };
        this.recordPending("duplicated", duplicate);
        this.#pending.push(duplicate as PendingEnvelope<unknown>);
      }
    }
    await this.pump(reentrant);
  }

  /**
   * Abort and await in-flight delivery before returning. A callback which
   * ignores its signal produces a bounded, visible cleanup failure rather
   * than silently leaking into a later scenario. Idempotent.
   */
  close(timeoutMs: number): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      return Promise.reject(new Error("envelope cleanup timeout must be positive"));
    }
    this.#closePromise = this.closeInternal(timeoutMs);
    return this.#closePromise;
  }

  receipt(): TopologyEnvelopeSchedulerReceipt {
    return {
      seed: this.seed,
      tick: this.#tick,
      closed: this.#closed,
      pending: this.#pending.length + Number(this.#heldForReorder !== undefined),
      inFlight: this.#inFlight.size,
      cleanup: this.#cleanup && { ...this.#cleanup },
      activities: this.#activities.map((activity) => ({ ...activity })),
    };
  }

  private takeNextFault(): NextEnvelopeFault {
    const next = this.#next;
    this.#next = { duplicate: 0, delayTicks: 0, reorder: false, dropThenRetryTicks: undefined };
    return next;
  }

  private async closeInternal(timeoutMs: number): Promise<void> {
    this.#closed = true;
    for (const pending of [
      ...this.#pending,
      ...(this.#heldForReorder ? [this.#heldForReorder] : []),
    ]) {
      this.recordPending("discarded", pending);
    }
    this.#pending = [];
    this.#heldForReorder = undefined;
    this.#partitions.clear();
    const started = now();
    this.#cleanup = { status: "attempted", elapsedMs: 0 };
    const abort = new Error("topology envelope scheduler closed");
    for (const inFlight of this.#inFlight.values()) {
      inFlight.controller.abort(abort);
      this.recordPending("deliveryAborted", inFlight.pending, abort);
    }
    try {
      await waitForSettlement(
        [...this.#inFlight.values()].map(({ promise }) => promise),
        timeoutMs,
        (timeoutError) => {
          const message = errorMessage(timeoutError);
          Object.assign(this.#cleanup!, {
            status: "failed",
            elapsedMs: elapsed(started),
            error: message,
          });
          this.record({ action: "closeTimedOut", error: message });
        },
      );
      Object.assign(this.#cleanup, { status: "completed", elapsedMs: elapsed(started) });
    } catch (error) {
      const message = errorMessage(error);
      if (this.#cleanup.status !== "failed") {
        Object.assign(this.#cleanup, {
          status: "failed",
          elapsedMs: elapsed(started),
          error: message,
        });
      }
      throw error;
    }
  }

  private async pump(reentrant = false): Promise<void> {
    // Only the callback-scoped intercept below may avoid joining its own drain.
    // Independent concurrent callers always await the active serialized pump.
    if (this.#pumpPromise) return reentrant ? undefined : this.#pumpPromise;
    this.#pumpPromise = this.pumpInternal();
    try {
      await this.#pumpPromise;
    } finally {
      this.#pumpPromise = undefined;
    }
  }

  private async pumpInternal(): Promise<void> {
    while (true) {
      if (this.#closed) return;
      const index = this.#pending.findIndex(
        (pending) => pending.dueTick <= this.#tick && !this.isPartitioned(pending.envelope),
      );
      if (index === -1) return;
      const [pending] = this.#pending.splice(index, 1);
      await this.deliver(pending);
    }
  }

  private async deliver(pending: PendingEnvelope<unknown>): Promise<void> {
    const controller = new AbortController();
    const promise = Promise.resolve().then(() => {
      let callbackActive = true;
      const context: TopologyEnvelopeDeliveryContext = {
        envelopeId: pending.envelopeId,
        attempt: pending.attempt,
        tick: this.#tick,
        sequence: pending.sequence,
        signal: controller.signal,
        intercept: async <T>(
          envelope: TopologyTransportEnvelope,
          value: T,
          deliver: TopologyEnvelopeDelivery<T>,
        ) => {
          if (!callbackActive) {
            throw new Error("topology delivery intercept is no longer active");
          }
          await this.interceptInternal(envelope, value, deliver, true);
        },
      };
      let result: Promise<void> | void;
      try {
        result = pending.deliver(pending.value, context);
      } catch (error) {
        callbackActive = false;
        throw error;
      }
      if (result === undefined) {
        callbackActive = false;
        return;
      }
      return Promise.resolve(result).finally(() => {
        callbackActive = false;
      });
    });
    this.#inFlight.set(pending.sequence, { pending, controller, promise });
    try {
      if (pending.retry) this.recordPending("retried", pending);
      await promise;
      this.recordPending("delivered", pending);
    } catch (error) {
      this.recordPending("deliveryFailed", pending, error);
      this.failStop(error);
      throw error;
    } finally {
      this.#inFlight.delete(pending.sequence);
    }
  }

  private isPartitioned(envelope: TopologyTransportEnvelope): boolean {
    return this.#partitions.has(linkKey(envelope.from, envelope.to));
  }

  private recordPending<T>(
    action: TopologyEnvelopeAction,
    pending: PendingEnvelope<T>,
    error?: unknown,
  ): void {
    this.record({
      action,
      envelopeId: pending.envelopeId,
      sequence: pending.sequence,
      attempt: pending.attempt,
      from: pending.envelope.from,
      to: pending.envelope.to,
      ...(pending.envelope.label === undefined ? {} : { label: pending.envelope.label }),
      ...(error === undefined ? {} : { error: topologyEnvelopeErrorClass(error) }),
    });
  }

  private record(activity: Omit<TopologyEnvelopeActivity, "tick">): void {
    if (this.#activities.length >= MAX_TOPOLOGY_ACTIVITIES) {
      throw new Error(
        `topology envelope scheduler activity capacity is ${MAX_TOPOLOGY_ACTIVITIES}`,
      );
    }
    this.#activities.push({ ...activity, tick: this.#tick });
  }

  private assertOpen(): void {
    if (this.#closed) throw new Error("topology envelope scheduler is closed");
    if (this.#failure !== undefined) {
      throw new Error("topology envelope scheduler is failed after a delivery error");
    }
  }

  private assertArmedOnly(kind: string): void {
    const armed =
      this.#next.duplicate > 0 ||
      this.#next.delayTicks > 0 ||
      this.#next.reorder ||
      this.#next.dropThenRetryTicks !== undefined;
    const duplicateOnly =
      this.#next.duplicate > 0 &&
      !this.#next.delayTicks &&
      !this.#next.reorder &&
      this.#next.dropThenRetryTicks === undefined;
    if (armed && !(kind === "duplicate" && duplicateOnly)) {
      throw new Error("only one envelope fault kind may be armed for the next envelope");
    }
  }

  private allocateSequence(): number {
    if (this.#sequence >= Number.MAX_SAFE_INTEGER)
      throw new Error("topology delivery sequence exhausted");
    return ++this.#sequence;
  }

  private allocateEnvelopeId(): number {
    if (this.#envelopeId >= Number.MAX_SAFE_INTEGER)
      throw new Error("topology envelope id exhausted");
    return ++this.#envelopeId;
  }

  private ensureCapacity(additional: number): void {
    if (
      this.#pending.length +
        Number(this.#heldForReorder !== undefined) +
        this.#inFlight.size +
        additional >
      MAX_TOPOLOGY_ENVELOPES
    ) {
      throw new Error(`topology envelope scheduler capacity is ${MAX_TOPOLOGY_ENVELOPES}`);
    }
  }

  private failStop(error: unknown): void {
    if (this.#failure !== undefined) return;
    this.#failure = error;
    for (const queued of [
      ...this.#pending,
      ...(this.#heldForReorder ? [this.#heldForReorder] : []),
    ]) {
      this.recordPending("discarded", queued, error);
    }
    this.#pending = [];
    this.#heldForReorder = undefined;
  }
}

function linkKey(first: string, second: string): string {
  return first < second ? `${first}\u0000${second}` : `${second}\u0000${first}`;
}

const MAX_TOPOLOGY_ENDPOINT_LENGTH = 128;
const MAX_TOPOLOGY_LABEL_LENGTH = 256;
const MAX_TOPOLOGY_COPIES = 16;
const MAX_TOPOLOGY_TICKS = 1_000_000;
const MAX_TOPOLOGY_ENVELOPES = 4_096;
const MAX_TOPOLOGY_PARTITIONS = 4_096;
const MAX_TOPOLOGY_ACTIVITIES = 32_768;
const ENVELOPE_KEYS = new Set(["from", "to", "label"]);

function snapshotEnvelope(envelope: TopologyTransportEnvelope): TopologyTransportEnvelope {
  if (
    typeof envelope !== "object" ||
    envelope === null ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(envelope))
  ) {
    throw new Error("topology envelope must be a plain descriptor");
  }
  for (const key of Reflect.ownKeys(envelope)) {
    if (typeof key !== "string" || !ENVELOPE_KEYS.has(key)) {
      throw new Error(`topology envelope contains unsupported metadata: ${String(key)}`);
    }
  }
  const { from, to, label } = envelope;
  assertEndpoint("from", from);
  assertEndpoint("to", to);
  if (
    label !== undefined &&
    (typeof label !== "string" || label.length > MAX_TOPOLOGY_LABEL_LENGTH)
  ) {
    throw new Error(
      `topology envelope label must be a string at most ${MAX_TOPOLOGY_LABEL_LENGTH} characters`,
    );
  }
  return label === undefined ? { from, to } : { from, to, label };
}

function assertEndpoint(name: string, value: unknown): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_TOPOLOGY_ENDPOINT_LENGTH ||
    value.includes("\u0000")
  ) {
    throw new Error(
      `topology envelope ${name} must be a non-empty string at most ${MAX_TOPOLOGY_ENDPOINT_LENGTH} characters without NUL`,
    );
  }
}

function assertTopologyTicks(name: string, ticks: unknown): asserts ticks is number {
  if (
    typeof ticks !== "number" ||
    !Number.isSafeInteger(ticks) ||
    ticks < 1 ||
    ticks > MAX_TOPOLOGY_TICKS
  ) {
    throw new Error(`${name} ticks must be a positive safe integer at most ${MAX_TOPOLOGY_TICKS}`);
  }
}

async function waitForSettlement(
  promises: readonly Promise<void>[],
  timeoutMs: number,
  onTimeout: (error: Error) => void,
): Promise<void> {
  if (promises.length === 0) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutError = new Error(`topology envelope cleanup timed out after ${timeoutMs}ms`);
  const settlement = Promise.allSettled(promises).then(() => undefined);
  let timedOut = false;
  try {
    await Promise.race([
      settlement,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          onTimeout(timeoutError);
          reject(timeoutError);
        }, timeoutMs);
      }),
    ]);
  } catch (error) {
    if (!timedOut) throw error;
    // A timeout is diagnostic, not permission for the scheduler to leak a
    // still-mutating delivery into the next scenario.
    await settlement;
    throw timeoutError;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function topologyEnvelopeErrorClass(error: unknown): "error" | "non-error" {
  return error instanceof Error ? "error" : "non-error";
}

export class TopologyScenarioError extends Error {
  constructor(
    readonly receipt: TopologyReceipt,
    options?: unknown,
  ) {
    super(`${receipt.scenario} seed=${receipt.seed}: ${receipt.error}`, { cause: options });
  }
}

export function deterministicRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state += 0x6d2b79f5;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export async function withTopologyTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  const pending = operation(controller.signal);
  try {
    return await Promise.race([
      pending,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => {
          const error = new Error(`${label} after ${timeoutMs}ms`);
          reject(error);
          controller.abort(error);
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function createOperationRegistrar(
  operation: string,
  register: (name: string, cleanup: TopologyCompensation) => void,
): { defer: TopologyOperationContext["defer"]; close(): void } {
  let closed = false;
  return {
    defer(name, cleanup) {
      if (closed) throw new Error(`topology ${operation} is no longer active`);
      register(name, cleanup);
    },
    close() {
      closed = true;
    },
  };
}

/**
 * A compensation may take longer than its diagnostic budget, but it must not
 * outlive the scenario: after aborting at the budget, wait for it to settle
 * before running an earlier inverse or returning to the next scenario. An
 * inverse that ignores abort forever therefore intentionally keeps the runner
 * pending; bounded return and no leaked external mutation are incompatible.
 */
async function withTopologyCompensationTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  const pending = Promise.resolve().then(() => operation(controller.signal));
  const timeoutError = new Error(`${label} after ${timeoutMs}ms`);
  let timedOut = false;
  try {
    return await Promise.race([
      pending,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          controller.abort(timeoutError);
          reject(timeoutError);
        }, timeoutMs);
      }),
    ]);
  } catch (error) {
    if (!timedOut) throw error;
    // The timeout is a diagnostic boundary, not permission to abandon an
    // inverse that may still mutate the next scenario's external state.
    await pending.catch(() => undefined);
    throw timeoutError;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function now(): number {
  return typeof performance === "undefined" ? Date.now() : performance.now();
}

function elapsed(started: number): number {
  return Math.max(0, Math.round(now() - started));
}
