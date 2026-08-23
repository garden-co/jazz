import { deterministicBandFixture } from "./fixture.js";

export type BandChatScenario = {
  id: "band-chat.smoke.send-message";
  fixtureVersion: number;
  operations: readonly ["create-demo-room", "send-message", "assert-visible"];
  assertion: { visibleText: string };
};

/** Framework-neutral workload contract consumed by UI and headless runners. */
export function createSmokeScenario(seed?: string): BandChatScenario {
  const fixture = deterministicBandFixture(seed);
  return {
    id: "band-chat.smoke.send-message",
    fixtureVersion: fixture.version,
    operations: ["create-demo-room", "send-message", "assert-visible"],
    assertion: { visibleText: fixture.messages[0] },
  };
}
