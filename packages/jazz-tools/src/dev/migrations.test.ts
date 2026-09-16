import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { renderMigrationStub } from "./migrations.js";

describe("migration stub generation", () => {
  it("generates additive table witnesses with bare, nullable, and referenced UUIDs", () => {
    const from = {
      users: s.table({ name: s.string() }, {}),
    };
    const to = {
      ...from,
      records: s.table(
        {
          externalId: s.uuid(),
          previousId: s.uuid().optional(),
          ownerId: s.uuid(),
          reviewerId: s.uuid().optional(),
        },
        { owner: s.rel("users", "ownerId"), reviewer: s.rel("users", "reviewerId") },
      ),
    };
    const source = renderMigrationStub({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      fromSchema: s.defineApp(from).wasmSchema,
      toSchema: s.defineApp(to).wasmSchema,
    });

    expect(source).toContain('"externalId": s.uuid(),');
    expect(source).toContain('"previousId": s.uuid().optional(),');
    expect(source).toContain('"ownerId": s.uuid(),');
    expect(source).toContain('"reviewerId": s.uuid().optional(),');

    expect(source).toContain('"owner": s.rel("users", "ownerId")');
    expect(source).toContain('"reviewer": s.rel("users", "reviewerId")');

    // The generated stub is executable JavaScript: run it through the public
    // migration builder to verify the additive-table migration is usable.
    const migration = new Function(
      "s",
      source
        .replace('import { schema as s } from "jazz-tools";', "")
        .replace("export default", "return"),
    )(s);
    expect(migration.forward).toEqual([{ table: "records", added: true, operations: [] }]);
  });
});
