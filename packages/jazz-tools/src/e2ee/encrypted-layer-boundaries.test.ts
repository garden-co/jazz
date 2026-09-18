import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { schemaDefinitionToAst } from "../migrations.js";

const definition = {
  projects: s.table({ title: s.string() }, { notes: s.reverse("notes", "project") }),
  notes: s
    .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["body"] }),
};

it("refuses encrypted migration export instead of treating ciphertext as logical plaintext", () => {
  expect(() => schemaDefinitionToAst(definition)).toThrow(/Encrypted schema migrations/);
  expect(() =>
    s.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: definition,
      to: definition,
    }),
  ).toThrow(/Encrypted schema migrations/);
});

it("refuses equality index declarations before their query contract is available", () => {
  const options = {
    space: "projectId" as const,
    columns: ["body"] as const,
    indexes: { body: "equality" },
  };
  expect(() =>
    s
      .table(
        { projectId: s.uuid(), body: s.string() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted(options),
  ).toThrow(/equality indexes/);
});
