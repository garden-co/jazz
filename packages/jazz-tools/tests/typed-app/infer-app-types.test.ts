// `testApp` infers an inline app's types as `jazz-tools generate` writes
// them: for an app with every column kind, the inferred types are the ones
// of its committed bindings (apps/every-kind/jazz.generated.ts).
import { readFileSync } from "node:fs";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  testApp,
  type InferAppTypes,
  type InferTransformTypes,
  type LooseAppTypes,
} from "../../src/testing/test-app.js";
import type { App, ComputedImpl, TransformImpl } from "../../src/typed-app.js";
import { computed, transforms } from "./apps/every-kind/jazz.computed.js";
import {
  app as generated,
  type AppComputed,
  type AppTransforms,
  type Owner,
  type OwnerInsert,
  type Thing,
  type ThingInsert,
} from "./apps/every-kind/jazz.generated.js";

// apps/every-kind/jazz.app.json, inline (the first test checks they agree).
const everyKind = {
  tables: {
    owners: {
      columns: {
        name: "string",
      },
      relations: {
        things: {
          reverse: "things",
          relation: "owner",
        },
        coOwned: {
          reverse: "things",
          relation: "coOwners",
        },
      },
    },
    things: {
      columns: {
        text: "string",
        maybeText: "string?",
        int: "int",
        float: "float",
        big: "bigint",
        flag: "boolean",
        at: "timestamp",
        blob: "bytes",
        ownerId: "uuid",
        reviewerId: "uuid?",
        coOwnerIds: {
          array: "uuid",
        },
        optionalInt: {
          type: "int",
          optional: true,
        },
        defaultedFloat: {
          type: "float",
          default: 1.5,
        },
        defaultedNull: {
          type: "bigint",
          optional: true,
          default: null,
        },
        counter: {
          type: "int",
          merge: "counter",
        },
        tags: {
          array: "string",
          merge: "g-set",
        },
        body: {
          type: "string",
          merge: "text",
        },
        status: {
          enum: ["todo", "done"],
          default: "todo",
        },
        maybeStatus: {
          enum: ["low", "high"],
          optional: true,
        },
        event: {
          enum: {
            created: {
              by: "uuid",
            },
            moved: {
              to: "int",
              note: "string?",
              reason: {
                type: "string",
                default: "none",
              },
            },
          },
        },
        events: {
          array: {
            enum: {
              ping: {
                at: "timestamp?",
              },
              pong: {},
            },
          },
        },
        statuses: {
          array: {
            enum: ["a", "b"],
          },
        },
        data: {
          json: true,
        },
        maybeData: {
          json: true,
          optional: true,
        },
        nested: {
          array: {
            array: "float",
          },
        },
        shouted: {
          type: "string",
          transform: "upper",
        },
        maybeShouted: {
          type: "string",
          optional: true,
          transform: "upper",
        },
        stamp: {
          type: "timestamp",
          transform: "iso",
        },
      },
      relations: {
        owner: {
          rel: "owners",
          column: "ownerId",
        },
        reviewer: {
          rel: "owners",
          column: "reviewerId",
        },
        coOwners: {
          rel: "owners",
          column: "coOwnerIds",
        },
      },
      computed: {
        summary: {
          type: "string",
          from: ["text", "int"],
        },
        maybeLabel: {
          type: {
            enum: ["x", "y"],
          },
          from: ["id", "maybeStatus", "shouted"],
        },
        score: {
          type: "float?",
          from: ["float"],
        },
      },
    },
  },
  transforms: {
    upper: {
      stored: "string",
      view: "string",
    },
    iso: {
      stored: "timestamp",
      view: "string",
    },
  },
} as const;

type Inferred = InferAppTypes<typeof everyKind>;

/**
 * Whether TS holds `A` and `B` identical. `expectTypeOf(…).toEqualTypeOf`
 * can't compare whole tables: its deep brand of `WasmSchema` doesn't settle.
 */
type Identical<A, B> =
  (<X>() => X extends A ? 1 : 2) extends <X>() => X extends B ? 1 : 2 ? true : false;

describe("testApp's inferred types", () => {
  it("are inferred from the same app as the committed fixture", () => {
    const file = JSON.parse(
      readFileSync(new URL("./apps/every-kind/jazz.app.json", import.meta.url), "utf8"),
    ) as Record<string, unknown>;
    const { $schema: _schema, format: _format, ...tables } = file;
    expect(tables).toEqual(everyKind);
    const app = testApp(everyKind, { computed, transforms });
    expect(app.wasmSchema).toEqual(generated.wasmSchema);
  });

  it("equal the generated rows, inserts, columns, relations, computed fields and transforms", () => {
    expectTypeOf<Inferred["owners"]["row"]>().toEqualTypeOf<Owner>();
    expectTypeOf<Inferred["owners"]["insert"]>().toEqualTypeOf<OwnerInsert>();
    expectTypeOf<Inferred["things"]["row"]>().toEqualTypeOf<Thing>();
    expectTypeOf<Inferred["things"]["insert"]>().toEqualTypeOf<ThingInsert>();
    // Computed fields' `from` and `value`, and the transforms.
    expectTypeOf<ComputedImpl<Inferred>>().toEqualTypeOf<AppComputed>();
    expectTypeOf<
      TransformImpl<InferTransformTypes<typeof everyKind>, Inferred>
    >().toEqualTypeOf<AppTransforms>();
    // What `testApp` returns: the generated app's tables (their columns'
    // `kind`s decide the where operators; their relations, what `include`
    // and `hopTo` take), history queries and unions.
    const app = testApp(everyKind, { computed, transforms });
    expectTypeOf<Identical<typeof app.owners, typeof generated.owners>>().toEqualTypeOf<true>();
    expectTypeOf<Identical<typeof app.things, typeof generated.things>>().toEqualTypeOf<true>();
    expectTypeOf<Identical<typeof app.$history, typeof generated.$history>>().toEqualTypeOf<true>();
    const union = app.union<"things">;
    const generatedUnion = generated.union<"things">;
    expectTypeOf<Identical<typeof union, typeof generatedUnion>>().toEqualTypeOf<true>();
    expectTypeOf<keyof typeof app>().toEqualTypeOf<keyof typeof generated>();
  });

  it("are loose for an app given as a string, or when a test opts out", () => {
    const text = JSON.stringify({ format: 1, tables: { notes: { columns: { title: "string" } } } });
    expectTypeOf(testApp(text)).toEqualTypeOf<App<LooseAppTypes>>();
    expectTypeOf(
      testApp<LooseAppTypes>({ tables: { notes: { columns: { title: "string" } } } }),
    ).toEqualTypeOf<App<LooseAppTypes>>();
  });
});
