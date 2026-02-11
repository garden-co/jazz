import { beforeEach, describe, expect, test } from "vitest";
import { CoFeed, CoList, co, z } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";

describe("schema invariant", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("fails fast when creating a CoList class without coValueSchema", () => {
    class LegacyList extends CoList<string> {}

    expect(() => LegacyList.create(["a"])).toThrow(
      "[schema-invariant] LegacyList.create requires a coValueSchema.",
    );
  });

  test("fails fast when creating a CoFeed class without coValueSchema", () => {
    class LegacyFeed extends CoFeed<string> {}

    expect(() => LegacyFeed.create([])).toThrow(
      "[schema-invariant] LegacyFeed.create requires a coValueSchema.",
    );
  });

  test("allows schema-backed list/feed classes", () => {
    const Names = co.list(z.string());
    const Events = co.feed(z.string());

    expect(() => Names.create(["alice"])).not.toThrow();
    expect(() => Events.create(["hello"])).not.toThrow();
  });
});
