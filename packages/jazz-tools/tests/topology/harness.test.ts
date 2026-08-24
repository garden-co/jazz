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
    expect(
      receipt.envelopes[0]?.activities.filter(({ action }) =>
        ["deliveryAborted", "delivered", "deliveryFailed"].includes(action),
      ),
    ).toMatchObject([{ action: "deliveryAborted" }]);
  });

  it("records a rejected delivery without retaining it for cleanup", async () => {
    const scheduler = new TopologyEnvelopeScheduler(23);
    await expect(
      scheduler.intercept({ from: "a", to: "b", label: "reject" }, "reject", () =>
        Promise.reject(new Error("planted delivery rejection")),
      ),
    ).rejects.toThrow("topology-envelope-delivery-callback-rejected");
    await expect(scheduler.close(20)).rejects.toThrow(
      "topology-envelope-delivery-callback-rejected",
    );
    const receipt = scheduler.receipt();
    expect(receipt).toMatchObject({ closed: true, pending: 0, inFlight: 0 });
    expect(receipt.activities.find(({ action }) => action === "deliveryFailed")?.error).toBe(
      "delivery-callback-rejected",
    );
    expect(JSON.stringify(receipt)).not.toContain("planted delivery rejection");
  });

  it("bounds a delivery that ignores cancellation and retains an explicit timeout receipt", async () => {
    const scheduler = new TopologyEnvelopeScheduler(31);
    let release!: () => void;
    const delivery = scheduler.intercept(
      { from: "a", to: "b", label: "ignores-abort" },
      "ignores-abort",
      () => new Promise<void>((resolve) => (release = resolve)),
    );
    await Promise.resolve();
    let failure: unknown;
    try {
      await runTopologyScenario({
        id: "harness.fixture.in-flight-timeout",
        topology: ["fixture"],
        seed: 31,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 5,
        targets: {},
        replay: "in-flight-timeout-fixture",
        envelopeSchedulers: [scheduler],
        phases: [],
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(TopologyScenarioError);
    const receipt = (failure as TopologyScenarioError).receipt.envelopes[0];
    expect(receipt).toMatchObject({ closed: true, inFlight: 1, cleanup: { status: "failed" } });
    expect(receipt?.activities.at(-1)?.action).toBe("closeTimedOut");
    release();
    await delivery;
  });

  it("fails scenario cleanup when an aborted callback genuinely rejects", async () => {
    const scheduler = new TopologyEnvelopeScheduler(33);
    const delivery = scheduler
      .intercept(
        { from: "a", to: "b", label: "rejects-on-close" },
        "value",
        (_, { signal }) =>
          new Promise<void>((_, reject) => {
            signal.addEventListener(
              "abort",
              () => reject(new Error("private callback detail must be redacted")),
              { once: true },
            );
          }),
      )
      .catch(() => undefined);
    await Promise.resolve();
    await expect(
      runTopologyScenario({
        id: "harness.fixture.close-rejection",
        topology: ["fixture"],
        seed: 33,
        phaseTimeoutMs: 50,
        faultTimeoutMs: 50,
        targets: {},
        replay: "close-rejection-fixture",
        envelopeSchedulers: [scheduler],
        phases: [],
      }),
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof TopologyScenarioError &&
        error.receipt.error?.includes("delivery-callback-rejected") === true &&
        !JSON.stringify(error.receipt).includes("private callback detail"),
    );
    await delivery;
    const terminal = scheduler
      .receipt()
      .activities.filter(({ action }) =>
        ["deliveryAborted", "delivered", "deliveryFailed"].includes(action),
      );
    expect(terminal).toMatchObject([
      { action: "deliveryFailed", error: "delivery-callback-rejected" },
    ]);
  });

  it("treats rejecting with the abort reason as cooperative cancellation", async () => {
    const scheduler = new TopologyEnvelopeScheduler(34);
    const delivery = scheduler.intercept(
      { from: "a", to: "b", label: "cooperative-abort" },
      "value",
      (_, { signal }) =>
        new Promise<void>((_, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        }),
    );
    await Promise.resolve();
    await scheduler.close(50);
    await delivery;
    const receipt = scheduler.receipt();
    expect(receipt.cleanup?.status).toBe("completed");
    expect(
      receipt.activities.filter(({ action }) =>
        ["deliveryAborted", "delivered", "deliveryFailed"].includes(action),
      ),
    ).toMatchObject([{ action: "deliveryAborted" }]);
  });

  it("rejects impractical cleanup timers without starting close", async () => {
    const scheduler = new TopologyEnvelopeScheduler(35);
    await expect(scheduler.close(Number.MAX_SAFE_INTEGER)).rejects.toThrow("at most 300000ms");
    expect(scheduler.receipt().closed).toBe(false);
    await scheduler.close(20);
  });

  it("serializes concurrent intercepts and permits a delivery to enqueue a follow-up", async () => {
    const scheduler = new TopologyEnvelopeScheduler(37);
    let signalFirstEntered!: () => void;
    const firstEntered = new Promise<void>((resolve) => (signalFirstEntered = resolve));
    let releaseFirst!: () => void;
    const firstReleased = new Promise<void>((resolve) => (releaseFirst = resolve));
    const delivered: string[] = [];
    const first = scheduler.intercept({ from: "a", to: "b", label: "first" }, "first", async () => {
      signalFirstEntered();
      await firstReleased;
      delivered.push("first");
    });
    await firstEntered;
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

    await scheduler.intercept(
      { from: "a", to: "b", label: "parent" },
      "parent",
      (_, { enqueue }) => {
        enqueue(
          { from: "a", to: "b", label: "child" },
          "child",
          () => void delivered.push("child"),
        );
        delivered.push("parent");
      },
    );
    expect(delivered).toEqual(["first", "second", "parent", "child"]);
    expect(
      scheduler
        .receipt()
        .activities.filter(({ action }) => action === "delivered")
        .map(({ label }) => label),
    ).toEqual(["first", "second", "parent", "child"]);
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

  it("fail-stops after a callback failure and discards remaining duplicate deliveries", async () => {
    const scheduler = new TopologyEnvelopeScheduler(43);
    scheduler.duplicateNext(2);
    await expect(
      scheduler.intercept({ from: "a", to: "b", label: "fail" }, "value", () => {
        throw new Error("planted terminal delivery failure");
      }),
    ).rejects.toThrow("topology-envelope-delivery-callback-rejected");
    const receipt = scheduler.receipt();
    expect(receipt.activities.filter(({ action }) => action === "deliveryFailed")).toHaveLength(1);
    expect(receipt.activities.filter(({ action }) => action === "discarded")).toHaveLength(2);
    await expect(
      scheduler.intercept({ from: "a", to: "b" }, "later", () => undefined),
    ).rejects.toThrow("scheduler is failed");
  });

  it("keeps terminal cleanup receipts after activity history truncates", async () => {
    const scheduler = new TopologyEnvelopeScheduler(47);
    for (let index = 0; index < 2_100; index++) {
      await scheduler.intercept({ from: "a", to: "b", label: "fill" }, index, () => undefined);
    }
    scheduler.delayNext(10);
    await scheduler.intercept({ from: "a", to: "b", label: "held" }, "held", () => undefined);
    await scheduler.close(20);
    const receipt = scheduler.receipt();
    expect(receipt.activitiesTruncated).toBeGreaterThan(0);
    expect(receipt.activities.at(-1)).toMatchObject({ action: "discarded", label: "held" });
    expect(receipt).toMatchObject({ closed: true, pending: 0, inFlight: 0 });
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

  it("accepts obligation-first registration but rejects a registrar captured after its phase", async () => {
    const calls: string[] = [];
    let staleDefer!: (
      name: string,
      cleanup: (context: { signal: AbortSignal }) => Promise<void>,
    ) => void;
    const receipt = await runTopologyScenario({
      id: "harness.fixture.compensation-operation-scope",
      topology: ["fixture"],
      seed: 56,
      phaseTimeoutMs: 50,
      faultTimeoutMs: 50,
      targets: {},
      replay: "compensation-operation-scope-fixture",
      phases: [
        {
          name: "register before acquisition finishes",
          run: async ({ defer }) => {
            defer("release obligation", async () => {
              calls.push("release");
            });
            staleDefer = defer;
            await Promise.resolve();
          },
        },
        {
          name: "reject stale registrar",
          run: async () => {
            expect(() => staleDefer("late release", async () => undefined)).toThrow(
              "topology phase register before acquisition finishes is no longer active",
            );
          },
        },
      ],
    });
    expect(receipt.compensations).toMatchObject([
      { name: "release obligation", status: "completed" },
    ]);
    expect(calls).toEqual(["release"]);
  });

  it("closes fault and timed-out phase registrars before their work can resume", async () => {
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
    expect(cleanedUp).toBe(true);
    expect(postTimeoutEffect).toBe(false);
  });

  it("aborts timed-out cleanup before it can mutate", async () => {
    let postTimeoutEffect = false;
    let abortObserved = false;
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
                abortObserved = true;
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
        (error.receipt.cleanup.error?.includes("cleanup timed out") ?? false),
    );
    expect(abortObserved).toBe(true);
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
