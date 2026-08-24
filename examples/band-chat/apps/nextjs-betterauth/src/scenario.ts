import { deterministicBandFixture } from "./fixture.js";

export type BandChatScenario = {
  id: "band-chat.topology.room-recovery";
  fixtureVersion: number;
  operations: readonly [
    "create-demo-room",
    "invite-peer",
    "concurrent-message-reaction-attachment",
    "partition-writer",
    "reconnect-and-replay",
    "assert-subscription-convergence",
    "revoke-peer",
  ];
  assertion: { visibleText: string };
  faults: readonly ["authorization", "disconnect", "reconnect"];
};

/** Framework-neutral workload contract consumed by UI and headless runners. */
export function createSmokeScenario(seed?: string): BandChatScenario {
  const fixture = deterministicBandFixture(seed);
  return {
    id: "band-chat.topology.room-recovery",
    fixtureVersion: fixture.version,
    operations: [
      "create-demo-room",
      "invite-peer",
      "concurrent-message-reaction-attachment",
      "partition-writer",
      "reconnect-and-replay",
      "assert-subscription-convergence",
      "revoke-peer",
    ],
    assertion: { visibleText: fixture.messages[0] },
    faults: ["authorization", "disconnect", "reconnect"],
  };
}
