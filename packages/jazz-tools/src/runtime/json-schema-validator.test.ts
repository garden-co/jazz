import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { parseJson, toJsonText as toJson } from "./json-schema-json.js";
import { compileJsonSchema, compileJsonSchemaText } from "./json-schema-validator.js";

type ParityCase = {
  description: string;
  schema: unknown;
  valid?: unknown[];
  invalid?: unknown[];
  invalidSchema?: boolean;
  browserUnsupported?: boolean;
};

// Shared with `crates/jazz/layers/model/tests/json_schema_parity.rs`, which
// pins the native validator's verdicts on the same cases. The fixture is read
// losslessly so integers beyond 2^53 and literals like `1.0` reach the
// validator as written.
const parityCases = parseJson(
  readFileSync(
    new URL(
      "../../../../crates/jazz/layers/model/tests/fixtures/json_schema_parity.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as ParityCase[];

describe("browser JSON Schema validator", () => {
  describe("matches the native validator on the parity fixture", () => {
    for (const parityCase of parityCases) {
      it(parityCase.description, () => {
        const schemaJson = toJson(parityCase.schema);
        if (parityCase.invalidSchema) {
          expect(() => compileJsonSchemaText(schemaJson)).toThrow();
          return;
        }
        if (parityCase.browserUnsupported) {
          expect(() => compileJsonSchemaText(schemaJson)).toThrow(
            /is not supported by the browser runtime yet/,
          );
          return;
        }
        const check = compileJsonSchemaText(schemaJson);
        for (const value of parityCase.valid ?? []) {
          expect(check(toJson(value)), toJson(value)).toBeUndefined();
        }
        for (const value of parityCase.invalid ?? []) {
          expect(check(toJson(value)), toJson(value)).toEqual(expect.any(String));
        }
      });
    }
  });

  // Jazz refuses, natively too, the `\p{…}` names outside this table
  // (`json_schema_support/regex.rs`), so every name in it must be one the
  // browser translates.
  it("translates every Unicode class name Jazz accepts", () => {
    const table = JSON.parse(
      readFileSync(
        new URL(
          "../../../../crates/jazz/layers/model/src/model/json_schema_support/js_unicode_properties.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as Record<"binary" | "generalCategory" | "script", string[]>;
    const names = [
      ...table.binary,
      ...table.generalCategory,
      ...table.generalCategory.map((value) => `gc=${value}`),
      ...table.script.map((value) => `Script=${value}`),
      ...table.script.map((value) => `scx=${value}`),
    ];
    for (const name of names) {
      expect(() => compileJsonSchema({ pattern: `\\p{${name}}` }), name).not.toThrow();
    }
  });

  it("says which value failed and where", () => {
    const check = compileJsonSchema({
      type: "object",
      properties: { profile: { properties: { age: { minimum: 0 } } } },
    });

    expect(check(JSON.stringify({ profile: { age: -1 } }))).toBe(
      "-1 is less than 0. (at /profile/age)",
    );
    expect(check(JSON.stringify("Ada"))).toBe(
      'Instance type "string" is invalid. Expected "object".',
    );
  });

  it("says why a declared schema is invalid", () => {
    expect(() => compileJsonSchema({ minLength: -1 })).toThrow(/-1 is less than 0/);
    expect(() => compileJsonSchema({ pattern: "(" })).toThrow(
      '"(" is not a valid regular expression',
    );
    expect(() => compileJsonSchema({ $ref: "#/$defs/missing" })).toThrow(
      'unresolvable reference "#/$defs/missing"',
    );
    expect(() => compileJsonSchema({ $schema: "https://example.com/dialect" })).toThrow(
      'unknown JSON Schema dialect "https://example.com/dialect"',
    );
  });
});
