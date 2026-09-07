import { describe, it } from "vitest";
import {
  schema,
  assertSubscriptionNull,
  assertSubscriptionTies,
  assertSubscriptionNoop,
  assertSubscriptionWindow,
} from "../shared/local-subscription-scenarios.js";
import { withNativeRelayFixture } from "./fixture.js";

describe("real RN public subscription sorting (shared browser scenarios)", () => {
  for (const [name, scenario] of [
    ["keeps null sort ordering stable", assertSubscriptionNull],
    ["uses id as deterministic tie-break", assertSubscriptionTies],
    ["does not reposition on no-op sort update", assertSubscriptionNoop],
    ["moves limit/offset windows", assertSubscriptionWindow],
  ] as const) {
    it(name, async () => {
      await withNativeRelayFixture({ wasmSchema: schema }, async (fixture) => {
        const db = await fixture.createDb();
        await scenario(db, (query, onRows) => db.subscribe(query, onRows));
      });
    });
  }
});
