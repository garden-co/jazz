import { describe, expect, it } from "vitest";
import {
  TopologyScenarioError,
  deterministicRandom,
  runTopologyScenario,
  type TopologyFaultTarget,
} from "./harness.js";

describe("shared example topology harness", () => {
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
