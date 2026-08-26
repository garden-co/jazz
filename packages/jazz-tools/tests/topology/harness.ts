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
    receipt.elapsedMs = elapsed(scenarioStarted);
  }
  if (receipt.status === "failed") throw new TopologyScenarioError(receipt, scenarioError);
  return receipt;
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
