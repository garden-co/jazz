export type TopologyKind = "core" | "edge" | "browser" | "native" | "fixture";

export type TopologyFaultKind = "disconnect" | "reconnect" | "restart" | "failure";

export interface TopologyFaultTarget {
  disconnect?: () => Promise<void>;
  reconnect?: () => Promise<void>;
  restart?: () => Promise<void>;
  fail?: () => Promise<void>;
}

export interface TopologyFault {
  kind: TopologyFaultKind;
  target: string;
  timeoutMs?: number;
}

export interface TopologyPhaseContext {
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
}

export interface TopologyReceipt {
  schemaVersion: 1;
  scenario: string;
  topology: readonly TopologyKind[];
  seed: number;
  status: "passed" | "failed";
  elapsedMs: number;
  phases: Array<{ name: string; elapsedMs: number }>;
  faults: Array<{ kind: TopologyFaultKind; target: string; elapsedMs: number }>;
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
    replay: scenario.replay,
  };
  const scenarioStarted = now();
  const random = deterministicRandom(scenario.seed);
  try {
    for (const phase of scenario.phases) {
      const started = now();
      reporter.phase("start", phase.name, 0);
      try {
        await withTopologyTimeout(
          phase.run({ seed: scenario.seed, random }),
          phase.timeoutMs ?? scenario.phaseTimeoutMs,
          `topology phase timed out: ${phase.name}`,
        );
        const elapsedMs = elapsed(started);
        receipt.phases.push({ name: phase.name, elapsedMs });
        reporter.phase("complete", phase.name, elapsedMs);
      } catch (error) {
        reporter.phase("failed", phase.name, elapsed(started));
        throw error;
      }
      for (const fault of phase.faultsAfter ?? []) {
        const target = scenario.targets[fault.target];
        const operation = target?.[fault.kind];
        if (!operation) {
          throw new Error(`topology target ${fault.target} does not support ${fault.kind}`);
        }
        const started = now();
        await withTopologyTimeout(
          operation(),
          fault.timeoutMs ?? scenario.faultTimeoutMs,
          `topology fault timed out: ${fault.kind} ${fault.target}`,
        );
        receipt.faults.push({
          kind: fault.kind,
          target: fault.target,
          elapsedMs: elapsed(started),
        });
      }
    }
  } catch (error) {
    receipt.status = "failed";
    receipt.error = error instanceof Error ? error.message : String(error);
    throw new TopologyScenarioError(receipt, error);
  } finally {
    receipt.elapsedMs = elapsed(scenarioStarted);
  }
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
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function now(): number {
  return typeof performance === "undefined" ? Date.now() : performance.now();
}

function elapsed(started: number): number {
  return Math.max(0, Math.round(now() - started));
}
