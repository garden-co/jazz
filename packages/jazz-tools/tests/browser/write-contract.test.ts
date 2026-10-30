import { afterEach, describe, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { TestCleanup, createBrowserTestDb, uniqueDbName } from "./support.js";
import {
  assertApplyFailureSurfacesThroughHandle,
  assertBurstBeyondCapLosesNothing,
  assertResidentTombstoneRejectsThroughHandle,
  assertSameTurnReadAndSubscriptionSeeWrite,
} from "../shared/write-contract-scenarios.js";

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

async function openDb(label: string) {
  return ctx.track(
    await createBrowserTestDb({
      appId: uniqueDbName(`write-contract-${label}-app`),
      secret: generateAuthSecret(),
      driver: { type: "persistent", dbName: uniqueDbName(`write-contract-${label}-db`) },
    }),
  );
}

// Same scenarios as tests/react-native (RN) and tests/ts-dsl (NAPI): #3273 parity.
describe("WASM write contract", () => {
  it("reports a resident tombstone through the write handle", async () =>
    assertResidentTombstoneRejectsThroughHandle(await openDb("tombstone")));
  it("reports an apply-time failure through the write handle", async () =>
    assertApplyFailureSurfacesThroughHandle(await openDb("handle")));
  it("accepts a burst beyond the RN queue cap without losing writes", async () =>
    assertBurstBeyondCapLosesNothing(await openDb("burst")));
  it("fences a same-turn read and subscription behind the write", async () =>
    assertSameTurnReadAndSubscriptionSeeWrite(await openDb("fence")));
});
