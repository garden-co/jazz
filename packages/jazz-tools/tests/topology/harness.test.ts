import { describe, expect, it } from "vitest";
import {
  TopologyEnvelopeScheduler,
  TopologyScenarioError,
  deterministicRandom,
  runTopologyScenario,
  type TopologyFaultTarget,
} from "./harness.js";

describe("shared example topology harness", () => {
  it("deterministically plants envelope duplicates, delay, reordering, retry, and a healed partition", async () => {
    const scheduler = new TopologyEnvelopeScheduler(73);
    const delivered: Array<{ value: string; attempt: number; tick: number }> = [];
    const send = (value: string) =>
      scheduler.intercept(
        { from: "browser", to: "edge", label: value },
        value,
        (received, context) => void delivered.push({ value: received, ...context }),
      );

    scheduler.duplicateNext();
    await send("duplicate");
    scheduler.delayNext(2);
    await send("delayed");
    scheduler.reorderNext();
    await send("first");
    await send("second");
    scheduler.dropNextThenRetry(3);
    await send("retry");

    expect(delivered.map(({ value, attempt }) => `${value}:${attempt}`)).toEqual([
      "duplicate:1",
      "duplicate:2",
      "second:1",
      "first:1",
    ]);
    await scheduler.advance(2);
    expect(delivered.map(({ value }) => value)).toEqual([
      "duplicate",
      "duplicate",
      "second",
      "first",
      "delayed",
    ]);
    await scheduler.advance();
    expect(delivered.some(({ value }) => value === "retry")).toBe(true);
    scheduler.partition("browser", "edge");
    await send("partitioned");
    expect(delivered.some(({ value }) => value === "partitioned")).toBe(false);
    await scheduler.heal("browser", "edge");
    expect(delivered.map(({ value }) => value)).toContain("partitioned");

    const receipt = scheduler.receipt();
    expect(receipt).toMatchObject({ seed: 73, tick: 3, pending: 0, closed: false });
    expect(receipt.activities.map(({ action }) => action)).toContain("dropped");
    expect(receipt.activities.map(({ action }) => action)).toContain("retried");
    expect(receipt.activities.map(({ action }) => action)).toContain("partitioned");
  });

  it("closes held envelopes after scenario cleanup, proving faults cannot leak between cases", async () => {
    const scheduler = new TopologyEnvelopeScheduler(17);
    scheduler.delayNext(10);
    await scheduler.intercept({ from: "a", to: "b", label: "held" }, "held", () => undefined);

    const receipt = await runTopologyScenario({
      id: "harness.fixture.envelope-cleanup",
      topology: ["fixture"],
      seed: 17,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "envelope-cleanup-fixture",
      envelopeSchedulers: [scheduler],
      phases: [],
    });
    expect(receipt.envelopes).toHaveLength(1);
    expect(receipt.envelopes[0]).toMatchObject({ closed: true, pending: 0 });
    expect(receipt.envelopes[0]?.activities.at(-1)?.action).toBe("discarded");
    await expect(
      scheduler.intercept({ from: "a", to: "b" }, "late", () => undefined),
    ).rejects.toThrow("scheduler is closed");
  });

  it("runs deterministic phases and every fault callback with a replayable receipt", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 29);
    const calls: string[] = [];
    const target: TopologyFaultTarget = {
      disconnect: async () => void calls.push("disconnect"),
      reconnect: async () => void calls.push("reconnect"),
      restart: async () => void calls.push("restart"),
      failure: async () => void calls.push("failure"),
    };
    const receipt = await runTopologyScenario({
      id: "harness.fixture.cross-topology-faults",
      topology: ["core", "edge", "browser", "native", "fixture"],
      seed,
      phaseTimeoutMs: 500,
      faultTimeoutMs: 500,
      targets: { subject: target },
      replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --filter jazz-tools test:topology-fixture`,
      phases: [
        {
          name: "deterministic setup",
          run: async ({ random }) => calls.push(random().toFixed(8)),
          faultsAfter: (["disconnect", "reconnect", "restart", "failure"] as const).map((kind) => ({
            kind,
            target: "subject",
          })),
        },
      ],
    });
    const expectedRandom = deterministicRandom(seed)().toFixed(8);
    expect(calls).toEqual([expectedRandom, "disconnect", "reconnect", "restart", "failure"]);
    expect(receipt).toMatchObject({ status: "passed", seed, schemaVersion: 1 });
    expect(receipt.faults.map(({ kind }) => kind)).toEqual([
      "disconnect",
      "reconnect",
      "restart",
      "failure",
    ]);
    expect(receipt.phases[0]?.status).toBe("completed");
    expect(receipt.faults.every(({ status }) => status === "completed")).toBe(true);
  });

  it("bounds a stalled phase and retains its exact replay command", async () => {
    let postTimeoutEffect = false;
    let cleanedUp = false;
    await expect(
      runTopologyScenario({
        id: "harness.fixture.timeout",
        topology: ["fixture"],
        seed: 11,
        phaseTimeoutMs: 5,
        faultTimeoutMs: 5,
        targets: {},
        replay: "JAZZ_EXAMPLE_TOPOLOGY_SEED=11 replay-fixture",
        cleanup: async () => {
          cleanedUp = true;
        },
        phases: [
          {
            name: "planted stall",
            run: ({ signal }) =>
              new Promise<void>((resolve) => {
                const timer = setTimeout(() => {
                  postTimeoutEffect = true;
                  resolve();
                }, 30);
                signal.addEventListener(
                  "abort",
                  () => {
                    clearTimeout(timer);
                    resolve();
                  },
                  { once: true },
                );
              }),
          },
        ],
      }),
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof TopologyScenarioError &&
        error.receipt.replay.includes("JAZZ_EXAMPLE_TOPOLOGY_SEED=11") &&
        error.receipt.error?.includes("planted stall") &&
        error.receipt.phases[0]?.status === "failed",
    );
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(cleanedUp).toBe(true);
    expect(postTimeoutEffect).toBe(false);
  });

  it("aborts timed-out cleanup before it can mutate", async () => {
    let postTimeoutEffect = false;
    await expect(
      runTopologyScenario({
        id: "harness.fixture.cleanup-timeout",
        topology: ["fixture"],
        seed: 11,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        cleanupTimeoutMs: 5,
        targets: {},
        phases: [],
        replay: "cleanup-timeout-fixture",
        cleanup: ({ signal }) =>
          new Promise<void>((resolve) => {
            const timer = setTimeout(() => {
              postTimeoutEffect = true;
              resolve();
            }, 30);
            signal.addEventListener(
              "abort",
              () => {
                clearTimeout(timer);
                resolve();
              },
              { once: true },
            );
          }),
      }),
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof TopologyScenarioError &&
        error.receipt.cleanup?.status === "failed" &&
        error.receipt.cleanup.elapsedMs >= 5 &&
        error.receipt.cleanup.error?.includes("cleanup timed out"),
    );
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(postTimeoutEffect).toBe(false);
  });
});
