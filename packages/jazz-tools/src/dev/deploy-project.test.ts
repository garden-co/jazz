import { expect, it } from "vitest";
import { shippedMigrations } from "../runtime/client-migrations.js";
import { testApp } from "../testing/index.js";
import { computeSchemaHash } from "./catalogue.js";

// The schema versions and migrations an app's clients ship come from its
// jazz.app.json (`history` and `migrations`), as its bindings bind it.
it("ships the app's schema versions and migrations to its clients", async () => {
  const before = { notes: { columns: { title: "string" } } };
  const after = { notes: { columns: { body: "string" } } };
  const app = testApp({
    tables: after,
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
  });
  const bundle = shippedMigrations(app.wasmSchema)!;
  const [fromHash, toHash] = await Promise.all([
    computeSchemaHash(testApp({ tables: before }).wasmSchema),
    computeSchemaHash(testApp({ tables: after }).wasmSchema),
  ]);
  expect(bundle.schemas.map(({ hash }) => hash).sort()).toEqual([fromHash, toHash].sort());
  expect(bundle.migrations).toEqual([
    {
      fromHash,
      toHash,
      forward: [
        expect.objectContaining({
          table: "notes",
          operations: [{ type: "rename", column: "title", value: "body" }],
        }),
      ],
    },
  ]);
});

it("ships the one schema version of an app without history", async () => {
  const app = testApp({ tables: { notes: { columns: { title: "string" } } } });
  expect(shippedMigrations(app.wasmSchema)).toEqual({
    schemas: [
      {
        hash: await computeSchemaHash(app.wasmSchema),
        // A shipped version carries no relations.
        schema: { tables: { notes: { columns: app.wasmSchema.notes!.columns } } },
      },
    ],
    migrations: [],
  });
});
