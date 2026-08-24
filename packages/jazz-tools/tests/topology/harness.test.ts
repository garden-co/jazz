import { describe, expect, it } from "vitest";
import {
  TopologyScenarioError,
  deterministicRandom,
  runTopologyScenario,
  type TopologyFaultKind,
} from "./harness.js";

describe("shared example topology harness", () => {
  it("runs deterministic phases and every fault callback with a replayable receipt", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 29);
    const calls: string[] = [];
    const target = Object.fromEntries(
      (["disconnect", "reconnect", "restart", "failure"] as const).map((kind) => [
        kind,
        async () => {
          calls.push(kind);
        },
      ]),
    ) as Record<TopologyFaultKind, () => Promise<void>>;
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
  });

  it("bounds a stalled phase and retains its exact replay command", async () => {
    const pending = new Promise<void>(() => undefined);
    await expect(
      runTopologyScenario({
        id: "harness.fixture.timeout",
        topology: ["fixture"],
        seed: 11,
        phaseTimeoutMs: 5,
        faultTimeoutMs: 5,
        targets: {},
        replay: "JAZZ_EXAMPLE_TOPOLOGY_SEED=11 replay-fixture",
        phases: [{ name: "planted stall", run: () => pending }],
      }),
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof TopologyScenarioError &&
        error.receipt.replay.includes("JAZZ_EXAMPLE_TOPOLOGY_SEED=11") &&
        error.receipt.error?.includes("planted stall"),
    );
  });
});
