import { describe, expect, it } from "vitest";
import { browserTopologyPhase } from "./topology-harness.js";

describe("MusicAgent browser import preflight", () => {
  it("loads the adopter schema through the same SDK graph as the browser harness", async () => {
    const [{ app }, { DeterministicMusicAgent, JazzMusicStore }] = await browserTopologyPhase(
      "load MusicAgent adopter modules",
      () =>
        Promise.all([
          import("../../../../examples/music-agent/apps/ts-localfirst/schema.js"),
          import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"),
        ]),
    );

    expect(Object.keys(app.wasmSchema).sort()).toEqual([
      "attachments",
      "conversations",
      "tool_calls",
      "turns",
    ]);
    expect(DeterministicMusicAgent).toBeTypeOf("function");
    expect(JazzMusicStore).toBeTypeOf("function");
  }, 15_000);
});
