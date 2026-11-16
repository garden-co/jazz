import { describe, expect, it } from "vitest";
import { assertNoEnumCases } from "./app-file-enums.js";

const refusal = (column: string) =>
  `column "${column}": enum cases with fields are not supported (see https://github.com/garden-co/jazz/issues/5758)`;

describe("assertNoEnumCases", () => {
  it("accepts string enums", () => {
    expect(() =>
      assertNoEnumCases({
        tables: {
          issues: {
            columns: {
              status: { enum: ["open", "closed"], default: "open" },
              tags: { array: { enum: ["a", "b"] } },
            },
          },
        },
      }),
    ).not.toThrow();
  });

  it("refuses enum cases with fields, naming the column", () => {
    expect(() =>
      assertNoEnumCases({
        tables: {
          issues: {
            columns: {
              title: "string",
              kind: { enum: { bug: { severity: "int" }, feature: { area: "string" } } },
            },
          },
        },
      }),
    ).toThrow(refusal("issues.kind"));
  });

  it("refuses them inside an array or a type wrapper", () => {
    expect(() =>
      assertNoEnumCases({
        tables: { events: { columns: { log: { array: { enum: { ping: {} } } } } } },
      }),
    ).toThrow(refusal("events.log"));
    expect(() =>
      assertNoEnumCases({
        tables: { events: { columns: { last: { type: { enum: { ping: {} } }, optional: true } } } },
      }),
    ).toThrow(refusal("events.last"));
  });
});
