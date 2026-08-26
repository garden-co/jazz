import { describe, expect, it } from "vitest";
import {
  TopologyEnvelopeScheduler,
  TopologyScenarioError,
  deterministicRandom,
  runTopologyScenario,
  type TopologyEnvelopeDelivery,
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
    const retry = receipt.activities.find(({ action }) => action === "retried");
    const dropped = receipt.activities.find(({ action }) => action === "dropped");
    expect(retry?.envelopeId).toBe(dropped?.envelopeId);
    expect(retry?.sequence).toBe(dropped?.sequence);
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

  it("waits for an in-flight delivery to finish before the scenario returns", async () => {
    const scheduler = new TopologyEnvelopeScheduler(19);
    let deliveryFinished = false;
    void scheduler.intercept(
      { from: "a", to: "b", label: "in-flight" },
      "in-flight",
      (_, { signal }) =>
        new Promise<void>((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              deliveryFinished = true;
              resolve();
            },
            { once: true },
          );
        }),
    );
    await Promise.resolve();

    const receipt = await runTopologyScenario({
      id: "harness.fixture.in-flight-cleanup",
      topology: ["fixture"],
      seed: 19,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "in-flight-cleanup-fixture",
      envelopeSchedulers: [scheduler],
      phases: [],
    });
    expect(deliveryFinished).toBe(true);
    expect(receipt.envelopes[0]).toMatchObject({ closed: true, pending: 0, inFlight: 0 });
    expect(receipt.envelopes[0]?.cleanup?.status).toBe("completed");
  });

  it("records a rejected delivery without retaining it for cleanup", async () => {
    const scheduler = new TopologyEnvelopeScheduler(23);
    const secret = "credential-shaped-secret-value";
    await expect(
      scheduler.intercept({ from: "a", to: "b", label: "reject" }, "reject", () =>
        Promise.reject(new Error(secret)),
      ),
    ).rejects.toThrow(secret);
    await scheduler.close(20);
    const receipt = scheduler.receipt();
    expect(receipt).toMatchObject({ closed: true, pending: 0, inFlight: 0 });
    expect(receipt.activities.find(({ action }) => action === "deliveryFailed")?.error).toBe(
      "error",
    );
    const serialized = JSON.stringify(receipt);
    expect(serialized).not.toContain(secret);
    expect(serialized.length).toBeLessThan(2_000);
  });

  it("records a bounded timeout but drains an abort-ignoring delivery before returning", async () => {
    const scheduler = new TopologyEnvelopeScheduler(31);
    let release!: () => void;
    let observeAbort!: () => void;
    const abortObserved = new Promise<void>((resolve) => (observeAbort = resolve));
    const delivery = scheduler.intercept(
      { from: "a", to: "b", label: "ignores-abort" },
      "ignores-abort",
      (_, { signal }) =>
        new Promise<void>((resolve) => {
          release = resolve;
          signal.addEventListener("abort", observeAbort, { once: true });
        }),
    );
    await Promise.resolve();
    let scenarioSettled = false;
    const scenario = runTopologyScenario({
      id: "harness.fixture.in-flight-timeout",
      topology: ["fixture"],
      seed: 31,
      phaseTimeoutMs: 200,
      faultTimeoutMs: 50,
      targets: {},
      replay: "in-flight-timeout-fixture",
      envelopeSchedulers: [scheduler],
      phases: [],
    }).finally(() => (scenarioSettled = true));
    await abortObserved;
    while (!scheduler.receipt().activities.some(({ action }) => action === "closeTimedOut")) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    expect(scenarioSettled).toBe(false);
    release();
    let failure: unknown;
    try {
      await scenario;
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    const receipt = (failure as TopologyScenarioError).receipt.envelopes[0];
    expect(receipt).toMatchObject({ closed: true, inFlight: 0, cleanup: { status: "failed" } });
    expect(receipt?.activities.map(({ action }) => action)).toContain("closeTimedOut");
    expect(receipt?.activities.at(-1)?.action).toBe("delivered");
    await delivery;
  });

  it("keeps an independent concurrent intercept pending until its held callback is delivered", async () => {
    const scheduler = new TopologyEnvelopeScheduler(37);
    let releaseFirst!: () => void;
    const delivered: string[] = [];
    const first = scheduler.intercept(
      { from: "a", to: "b", label: "first" },
      "first",
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = () => {
            delivered.push("first");
            resolve();
          };
        }),
    );
    await Promise.resolve();
    const second = scheduler.intercept(
      { from: "a", to: "b", label: "second" },
      "second",
      () => void delivered.push("second"),
    );
    let secondSettled = false;
    void second.then(() => (secondSettled = true));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(delivered).toEqual([]);
    expect(secondSettled).toBe(false);
    releaseFirst();
    await Promise.all([first, second]);
    expect(delivered).toEqual(["first", "second"]);
    expect(secondSettled).toBe(true);
    expect(
      scheduler
        .receipt()
        .activities.filter(({ action }) => action === "delivered")
        .map(({ label }) => label),
    ).toEqual(["first", "second"]);
  });

  it("permits a callback-scoped reentrant intercept without releasing independent callers", async () => {
    const scheduler = new TopologyEnvelopeScheduler(38);
    const delivered: string[] = [];
    let staleIntercept!: Parameters<TopologyEnvelopeDelivery<string>>[1]["intercept"];
    await scheduler.intercept(
      { from: "a", to: "b", label: "parent" },
      "parent",
      async (_, context) => {
        staleIntercept = context.intercept;
        await context.intercept(
          { from: "a", to: "b", label: "child" },
          "child",
          () => void delivered.push("child"),
        );
        delivered.push("parent");
      },
    );
    expect(delivered).toEqual(["parent", "child"]);
    expect(
      scheduler
        .receipt()
        .activities.filter(({ action }) => action === "delivered")
        .map(({ label }) => label),
    ).toEqual(["parent", "child"]);
    await expect(staleIntercept({ from: "a", to: "b" }, "stale", () => undefined)).rejects.toThrow(
      "topology delivery intercept is no longer active",
    );
  });

  it("snapshots bounded descriptors and rejects receipt-field or payload injection", async () => {
    const scheduler = new TopologyEnvelopeScheduler(41);
    const envelope = { from: "a", to: "b", label: "before" };
    scheduler.delayNext();
    await scheduler.intercept(envelope, "value", () => undefined);
    envelope.label = "after";
    await scheduler.advance();
    expect(
      scheduler
        .receipt()
        .activities.filter(({ action }) => action === "delivered")
        .map(({ label }) => label),
    ).toEqual(["before"]);
    await expect(
      scheduler.intercept(
        {
          from: "a",
          to: "b",
          label: "safe",
          action: "delivered",
          payload: "not-a-receipt",
        } as never,
        "value",
        () => undefined,
      ),
    ).rejects.toThrow("unsupported metadata");
    await expect(
      scheduler.intercept({ from: "a", to: "b", label: "x".repeat(257) }, "value", () => undefined),
    ).rejects.toThrow("at most 256");
    scheduler.delayNext();
    expect(() => scheduler.reorderNext()).toThrow("only one envelope fault kind");
  });

  it("rejects non-numeric virtual-time requests before they can schedule a delivery", async () => {
    const scheduler = new TopologyEnvelopeScheduler(42);
    await expect(scheduler.advance("one" as never)).rejects.toThrow("positive safe integer");
    expect(() => scheduler.delayNext(Number.NaN)).toThrow("positive safe integer");
    expect(() => scheduler.dropNextThenRetry(1.5)).toThrow("positive safe integer");
    expect(scheduler.receipt()).toMatchObject({ tick: 0, pending: 0 });
  });

  it("fail-stops after a callback failure and discards remaining duplicate deliveries", async () => {
    const scheduler = new TopologyEnvelopeScheduler(43);
    scheduler.duplicateNext(2);
    await expect(
      scheduler.intercept({ from: "a", to: "b", label: "fail" }, "value", () => {
        throw new Error("planted terminal delivery failure");
      }),
    ).rejects.toThrow("planted terminal delivery failure");
    const receipt = scheduler.receipt();
    expect(receipt.activities.filter(({ action }) => action === "deliveryFailed")).toHaveLength(1);
    expect(receipt.activities.filter(({ action }) => action === "discarded")).toHaveLength(2);
    await expect(
      scheduler.intercept({ from: "a", to: "b" }, "later", () => undefined),
    ).rejects.toThrow("scheduler is failed");
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
          run: async ({ random }) => {
            calls.push(random().toFixed(8));
          },
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

  it("compensates route-style fault state after a later phase fails and leaves the next scenario clean", async () => {
    let routeBlocked = false;
    const calls: string[] = [];
    let failure: unknown;
    try {
      await runTopologyScenario({
        id: "harness.fixture.compensating-route",
        topology: ["fixture"],
        seed: 53,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        targets: {
          route: {
            failure: async ({ defer }) => {
              routeBlocked = true;
              defer("unblock fixture route", async () => {
                calls.push("unblock");
                routeBlocked = false;
              });
            },
          },
        },
        replay: "compensating-route-fixture",
        phases: [
          {
            name: "acquire test route",
            run: async () => undefined,
            faultsAfter: [{ kind: "failure", target: "route" }],
          },
          {
            name: "planted later phase failure",
            run: async () => {
              expect(routeBlocked).toBe(true);
              throw new Error("planted later phase failure");
            },
          },
        ],
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    const receipt = (failure as TopologyScenarioError).receipt;
    expect(receipt.error).toContain("planted later phase failure");
    expect(receipt.compensations).toMatchObject([
      { name: "unblock fixture route", status: "completed" },
    ]);
    expect(calls).toEqual(["unblock"]);
    expect(routeBlocked).toBe(false);

    const next = await runTopologyScenario({
      id: "harness.fixture.compensating-route-next",
      topology: ["fixture"],
      seed: 54,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "compensating-route-next-fixture",
      phases: [
        {
          name: "prove route was released",
          run: async () => expect(routeBlocked).toBe(false),
        },
      ],
    });
    expect(next.status).toBe("passed");
  });

  it("runs every compensation in reverse order while retaining the primary failure", async () => {
    const primary = new Error("planted primary failure");
    const calls: string[] = [];
    let failure: unknown;
    try {
      await runTopologyScenario({
        id: "harness.fixture.compensation-failure",
        topology: ["fixture"],
        seed: 55,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        targets: {},
        replay: "compensation-failure-fixture",
        phases: [
          {
            name: "register compensations",
            run: async ({ defer }) => {
              defer("first cleanup", async () => {
                calls.push("first");
              });
              defer("failing cleanup", async () => {
                calls.push("failing");
                throw new Error("planted cleanup failure");
              });
            },
          },
          {
            name: "fail after acquisition",
            run: async () => {
              throw primary;
            },
          },
        ],
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    expect((failure as Error).cause).toBe(primary);
    const receipt = (failure as TopologyScenarioError).receipt;
    expect(receipt.error).toBe(
      "planted primary failure; compensation failing cleanup failed: planted cleanup failure",
    );
    expect(receipt.compensations).toMatchObject([
      { name: "failing cleanup", status: "failed", error: "planted cleanup failure" },
      { name: "first cleanup", status: "completed" },
    ]);
    expect(calls).toEqual(["failing", "first"]);
  });

  it("closes normal phase, fault, and timed-out phase registrars before their work can resume", async () => {
    let stalePhaseDefer!: (
      name: string,
      cleanup: (context: { signal: AbortSignal }) => Promise<void>,
    ) => void;
    let staleFaultDefer!: (
      name: string,
      cleanup: (context: { signal: AbortSignal }) => Promise<void>,
    ) => void;
    let staleTimedOutDefer!: (
      name: string,
      cleanup: (context: { signal: AbortSignal }) => Promise<void>,
    ) => void;
    await expect(
      runTopologyScenario({
        id: "harness.fixture.compensation-closed-registrars",
        topology: ["fixture"],
        seed: 57,
        phaseTimeoutMs: 5,
        faultTimeoutMs: 50,
        targets: {
          route: {
            disconnect: async ({ defer }) => {
              staleFaultDefer = defer;
            },
          },
        },
        replay: "compensation-closed-registrars-fixture",
        phases: [
          {
            name: "capture normal phase registrar",
            run: async ({ defer }) => {
              stalePhaseDefer = defer;
            },
          },
          {
            name: "reject stale normal phase registrar",
            run: async () => {
              expect(() => stalePhaseDefer("late phase release", async () => undefined)).toThrow(
                "topology phase capture normal phase registrar is no longer active",
              );
            },
          },
          {
            name: "capture fault registrar",
            run: async () => undefined,
            faultsAfter: [{ kind: "disconnect", target: "route" }],
          },
          {
            name: "capture timed-out registrar",
            run: ({ defer }) => {
              staleTimedOutDefer = defer;
              return new Promise<void>(() => undefined);
            },
          },
        ],
      }),
    ).rejects.toThrow("topology phase timed out: capture timed-out registrar after 5ms");
    expect(() => stalePhaseDefer("late phase release", async () => undefined)).toThrow(
      "topology phase capture normal phase registrar is no longer active",
    );
    expect(() => staleFaultDefer("late fault release", async () => undefined)).toThrow(
      "topology fault disconnect route is no longer active",
    );
    expect(() => staleTimedOutDefer("late phase release", async () => undefined)).toThrow(
      "topology phase capture timed-out registrar is no longer active",
    );
  });

  it("drains an abort-ignoring inverse before earlier cleanup or the next scenario", async () => {
    let routeBlocked = true;
    const calls: string[] = [];
    let failure: unknown;
    try {
      await runTopologyScenario({
        id: "harness.fixture.compensation-timeout-drain",
        topology: ["fixture"],
        seed: 57,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        cleanupTimeoutMs: 5,
        targets: {},
        replay: "compensation-timeout-drain-fixture",
        phases: [
          {
            name: "register route inverses",
            run: async ({ defer }) => {
              defer("earlier cleanup", async () => {
                calls.push("earlier cleanup");
              });
              defer("abort-ignoring route release", async () => {
                await new Promise<void>((resolve) => {
                  setTimeout(() => {
                    calls.push("late route release");
                    routeBlocked = false;
                    resolve();
                  }, 15);
                });
              });
            },
          },
          {
            name: "fail after acquiring route",
            run: async () => {
              throw new Error("planted failure after route acquisition");
            },
          },
        ],
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    const receipt = (failure as TopologyScenarioError).receipt;
    expect(receipt.compensations).toMatchObject([
      {
        name: "abort-ignoring route release",
        status: "failed",
        error: "topology compensation timed out: abort-ignoring route release after 5ms",
      },
      { name: "earlier cleanup", status: "completed" },
    ]);
    expect(calls).toEqual(["late route release", "earlier cleanup"]);

    const next = await runTopologyScenario({
      id: "harness.fixture.compensation-timeout-drain-next",
      topology: ["fixture"],
      seed: 58,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "compensation-timeout-drain-next-fixture",
      phases: [
        {
          name: "route cannot mutate next scenario",
          run: async () => expect(routeBlocked).toBe(false),
        },
      ],
    });
    expect(next.status).toBe("passed");
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
        (error.receipt.error?.includes("planted stall") ?? false) &&
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
        (error.receipt.cleanup.error?.includes("cleanup timed out") ?? false),
    );
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(postTimeoutEffect).toBe(false);
  });

  it("drains an abort-ignoring scenario cleanup before the next scenario", async () => {
    let routeBlocked = true;
    const calls: string[] = [];
    let failure: unknown;
    try {
      await runTopologyScenario({
        id: "harness.fixture.scenario-cleanup-timeout-drain",
        topology: ["fixture"],
        seed: 59,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        cleanupTimeoutMs: 5,
        targets: {},
        replay: "scenario-cleanup-timeout-drain-fixture",
        phases: [],
        cleanup: async () => {
          await new Promise<void>((resolve) => {
            setTimeout(() => {
              calls.push("late scenario cleanup");
              routeBlocked = false;
              resolve();
            }, 15);
          });
        },
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    const receipt = (failure as TopologyScenarioError).receipt;
    expect(receipt.cleanup).toMatchObject({
      status: "failed",
      error: "topology scenario cleanup timed out after 5ms",
    });
    expect(calls).toEqual(["late scenario cleanup"]);

    const next = await runTopologyScenario({
      id: "harness.fixture.scenario-cleanup-timeout-drain-next",
      topology: ["fixture"],
      seed: 60,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "scenario-cleanup-timeout-drain-next-fixture",
      phases: [
        {
          name: "scenario cleanup cannot mutate next scenario",
          run: async () => expect(routeBlocked).toBe(false),
        },
      ],
    });
    expect(next.status).toBe("passed");
  });
});
