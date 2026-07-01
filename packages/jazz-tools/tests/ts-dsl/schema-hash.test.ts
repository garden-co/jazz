import { describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { computeSchemaHash } from "../../src/schema-hash.js";

describe("schema hash", () => {
  it("matches Rust SchemaHash::compute fixtures", () => {
    const fixtures = [
      {
        app: s.defineApp({
          todos: s.table({
            title: s.string(),
            done: s.boolean(),
          }),
        }),
        hash: "bfd77d25b0696da75df2ca82ab129c6289432decaaad8b86adcb31a366bdd217",
      },
      {
        app: s.defineApp({
          todos: s
            .table({
              title: s.string(),
              tags: s.array(s.string()).default([]),
            })
            .indexOnly(["tags"]),
        }),
        hash: "9e1bff4be758172b01b004217437d09c01dfc0b90c7ece70a849316d96afbe93",
      },
      {
        app: s.defineApp({
          todos: s.table({
            project: s.ref("projects").optional(),
            priority: s.int().default(1),
            done: s.boolean().default(false),
          }),
          projects: s.table({
            name: s.string(),
          }),
        }),
        hash: "e28a793c7b612b7dc2556340116375f8b3f16227505cc45e0c1c19e2fec450fe",
      },
      {
        app: s.defineApp({
          events: s.table({
            kind: s.enum("high", "low"),
            payload: s
              .json({
                type: "object",
                properties: {
                  ok: { type: "boolean" },
                },
              })
              .optional(),
          }),
        }),
        hash: "f50c8ea3a0f728aaf5f3cf4d107080be31e876e9889d787ed6652e6433c34060",
      },
    ];

    for (const { app, hash } of fixtures) {
      expect(computeSchemaHash(app.wasmSchema)).toBe(hash);
      expect(app.schemaHash).toBe(hash);
    }
  });
});
