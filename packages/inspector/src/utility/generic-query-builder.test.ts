import { describe, expect, it } from "vitest";

import { GenericQueryBuilder } from "./generic-query-builder";

describe("GenericQueryBuilder", () => {
  it("keeps object equality values as scalar filter values", () => {
    const query = new GenericQueryBuilder("events", {}).whereColumn("payload", "eq", {
      type: "created",
    });

    expect(JSON.parse(query._build())).toMatchObject({
      conditions: [{ column: "payload", op: "eq", value: { type: "created" } }],
    });
  });

  it("keeps where object values as operator maps for compatibility", () => {
    const query = new GenericQueryBuilder("events", {}).where({ count: { gte: 2 } });

    expect(JSON.parse(query._build())).toMatchObject({
      conditions: [{ column: "count", op: "gte", value: 2 }],
    });
  });
});
