import { expect, it } from "vitest";
import { schema as s } from "./schema-namespace.js";
import { schemaDefinitionToAst } from "./migrations.js";

it("exports ciphertext storage types for encrypted migration schemas", () => {
  const definition = {
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  };

  const ast = schemaDefinitionToAst(definition);
  const notes = ast.tables.find((table) => table.name === "notes")!;
  const projects = ast.tables.find((table) => table.name === "projects")!;

  expect(notes.columns.find((column) => column.name === "title")?.sqlType).toBe("BYTEA");
  expect(notes.columns.find((column) => column.name === "projectId")?.sqlType).toBe("UUID");
  expect(projects.columns.find((column) => column.name === "title")?.sqlType).toBe("TEXT");
});
