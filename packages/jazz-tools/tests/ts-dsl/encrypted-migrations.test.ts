import { expect, it } from "vitest";
import { schema as s } from "../../src/index.js";

it.each(["encrypt", "decrypt"])("rejects an in-place %s migration", (direction) => {
  const plain = s.table(
    { projectId: s.uuid(), title: s.string() },
    { project: s.rel("projects", "projectId") },
  );
  const encrypted = plain.encrypted({ space: "projectId", columns: ["title"] });
  expect(() =>
    s.defineMigration({
      from: { notes: direction === "encrypt" ? plain : encrypted },
      to: { notes: direction === "encrypt" ? encrypted : plain },
    }),
  ).toThrow(/encryption.*authorised client/i);
});

it.each(["space column", "scope", "encrypted columns", "equality index", "logical type"])(
  "rejects an in-place change to the %s",
  (change) => {
    const original = s.table(
      {
        projectId: s.uuid(),
        otherProjectId: s.uuid(),
        title: s.string(),
        notes: s.string(),
      },
      {
        project: s.rel("projects", "projectId"),
        otherProject: s.rel("projects", "otherProjectId"),
      },
    );
    const next = s.table(
      {
        projectId: s.uuid(),
        otherProjectId: s.uuid(),
        title: change === "logical type" ? s.int() : s.string(),
        notes: s.string(),
      },
      {
        project: s.rel(change === "scope" ? "teams" : "projects", "projectId"),
        otherProject: s.rel("projects", "otherProjectId"),
      },
    );
    expect(() =>
      // @ts-expect-error Deliberately invalid migration must also reject at runtime.
      s.defineMigration({
        from: { notes: original.encrypted({ space: "projectId", columns: ["title"] }) },
        to: {
          notes: next.encrypted({
            space: change === "space column" ? "otherProjectId" : "projectId",
            columns: change === "encrypted columns" ? ["title", "notes"] : ["title"],
            indexes: change === "equality index" ? { title: "equality" } : undefined,
          }),
        },
      }),
    ).toThrow(/encryption.*authorised client/i);
  },
);

it("allows equivalent encryption declarations in a different order", () => {
  const table = s.table(
    { projectId: s.uuid(), title: s.string(), notes: s.string() },
    { project: s.rel("projects", "projectId") },
  );
  const migration = s.defineMigration({
    from: {
      notes: table.encrypted({
        space: "projectId",
        columns: ["title", "notes"],
        indexes: { title: "equality", notes: "equality" },
      }),
    },
    to: {
      notes: table.encrypted({
        space: "projectId",
        columns: ["notes", "title"],
        indexes: { notes: "equality", title: "equality" },
      }),
    },
  });
  expect(migration.forward).toEqual([]);
});

it.each([false, true])(
  "preserves encrypted column identity through a rename (indexed: %s)",
  (indexed) => {
    const migration = s.defineMigration({
      from: {
        notes: s
          .table(
            { projectId: s.uuid(), title: s.string() },
            { project: s.rel("projects", "projectId") },
          )
          .encrypted({
            space: "projectId",
            columns: ["title"],
            indexes: indexed ? { title: "equality" } : undefined,
          }),
      },
      to: {
        notes: s
          .table(
            { projectId: s.uuid(), body: s.string() },
            { project: s.rel("projects", "projectId") },
          )
          .encrypted({
            space: "projectId",
            columns: ["body"],
            indexes: indexed ? { body: "equality" } : undefined,
          }),
      },
      migrate: { notes: { body: s.renameFrom("title") } },
    });
    expect(migration.forward).toEqual([
      {
        table: "notes",
        operations: [
          { type: "rename", column: "title", value: "body" },
          ...(indexed
            ? [{ type: "rename", column: "__e2ee_eq_title", value: "__e2ee_eq_body" }]
            : []),
        ],
      },
    ]);
  },
);

it("preserves the space reference identity through a rename", () => {
  const migration = s.defineMigration({
    from: {
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title"] }),
    },
    to: {
      notes: s
        .table(
          { spaceRef: s.uuid(), title: s.string() },
          { project: s.rel("projects", "spaceRef") },
        )
        .encrypted({ space: "spaceRef", columns: ["title"] }),
    },
    migrate: { notes: { spaceRef: s.renameFrom("projectId") } },
  });
  expect(migration.forward).toEqual([
    {
      table: "notes",
      operations: [{ type: "rename", column: "projectId", value: "spaceRef" }],
    },
  ]);
});
