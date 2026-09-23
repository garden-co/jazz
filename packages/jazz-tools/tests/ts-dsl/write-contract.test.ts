import { afterEach, beforeEach, describe, it } from "vitest";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import {
  assertApplyFailureSurfacesThroughHandle,
  assertBurstBeyondCapLosesNothing,
  assertResidentTombstoneRejects,
  assertSameTurnReadAndSubscriptionSeeWrite,
} from "../shared/write-contract-scenarios.js";

// Same scenarios as tests/react-native (RN) and tests/browser (WASM): #3273 parity.
describe("NAPI write contract", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      ...(await localAccountConfig("write-contract")),
      driver: { type: "persistent" },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("reports a resident tombstone through the write handle", () =>
    assertResidentTombstoneRejects(db, "handle"));
  it("reports an apply-time failure through the write handle", () =>
    assertApplyFailureSurfacesThroughHandle(db));
  it("accepts a burst beyond the RN queue cap without losing writes", () =>
    assertBurstBeyondCapLosesNothing(db));
  it("fences a same-turn read and subscription behind the write", () =>
    assertSameTurnReadAndSubscriptionSeeWrite(db));
});
