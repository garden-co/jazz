import { describe, expect, it } from "vitest";
import { preferSubscribedRows } from "./local-first-query.js";

describe("local-first query startup", () => {
  it("shows a direct local read while the subscription is starting", () => {
    const cached = [{ id: "cached" }];

    expect(preferSubscribedRows(undefined, cached)).toBe(cached);
  });

  it("uses the reactive result as soon as it is available", () => {
    const cached = [{ id: "cached" }];
    const subscribed: { id: string }[] = [];

    expect(preferSubscribedRows(subscribed, cached)).toBe(subscribed);
  });
});
