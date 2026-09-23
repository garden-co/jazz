import { describe, it } from "vitest";
import {
  assertApplyFailureSurfacesThroughHandle,
  assertBurstBeyondCapLosesNothing,
  assertResidentTombstoneRejectsThroughHandle,
  assertSameTurnReadAndSubscriptionSeeWrite,
  writeContractApp,
} from "../shared/write-contract-scenarios.js";
import { withNativeRelayFixture } from "./fixture.js";

// Same scenarios as tests/ts-dsl (NAPI) and tests/browser (WASM): #3273 parity.
describe("RN foreground write contract through the real native relay", () => {
  for (const [name, scenario] of [
    [
      "reports a resident tombstone through the write handle",
      assertResidentTombstoneRejectsThroughHandle,
    ],
    [
      "reports an apply-time failure through the write handle",
      assertApplyFailureSurfacesThroughHandle,
    ],
    [
      "applies backpressure to a burst beyond the queue cap without losing writes",
      assertBurstBeyondCapLosesNothing,
    ],
    [
      "fences a same-turn read and subscription behind the write",
      assertSameTurnReadAndSubscriptionSeeWrite,
    ],
  ] as const) {
    it(name, async () => {
      await withNativeRelayFixture(writeContractApp, {}, async (fixture) => {
        await scenario(await fixture.createDb());
      });
    });
  }
});
