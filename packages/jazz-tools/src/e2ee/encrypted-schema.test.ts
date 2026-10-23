import { expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../schema-namespace.js";

it.each([
  { indexes: { done: "equality" }, message: 'Index column "done" must be encrypted' },
  { indexes: { title: "range" }, message: 'Encrypted index "title" only supports equality' },
])("rejects invalid encrypted index declarations ($message)", ({ indexes, message }) => {
  expect(() =>
    s
      .table(
        { projectId: s.uuid(), title: s.string(), done: s.boolean() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["title"],
        // Exercise the runtime boundary for declarations from JavaScript callers.
        indexes: indexes as { title: "equality" },
      }),
  ).toThrow(message);
});

it("rejects encrypting the reference that identifies a row's space", () => {
  expect(() =>
    s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({
        space: "projectId",
        columns: ["projectId", "title"],
      }),
  ).toThrow('Encrypted column "projectId" must not be a reference');
});

it.each(["before", "after"])(
  "keeps explicit references queryable when encryption is declared %s index and branch modifiers",
  (order) => {
    const notes = s.table(
      {
        projectId: s.uuid(),
        reviewerIds: s.array(s.uuid()),
        privateId: s.uuid(),
        title: s.string(),
      },
      {
        project: s.rel("projects", "projectId"),
        reviewers: s.rel("people", "reviewerIds"),
      },
    );
    const encrypted =
      order === "before"
        ? notes.encrypted({ space: "projectId" }).indexOnly(["projectId"]).branchBy("projectId")
        : notes.indexOnly(["projectId"]).branchBy("projectId").encrypted({ space: "projectId" });
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, { notes: s.reverse("notes", "project") }),
      people: s.table({ name: s.string() }, {}),
      notes: encrypted,
    });

    // References must remain usable UUID storage, while unrelated UUID data
    // is encrypted just like text when the encrypted columns are omitted.
    expect(
      app.wasmSchema.notes!.columns.map((column) => [
        column.name,
        column.column_type,
        column.references,
      ]),
    ).toEqual([
      ["projectId", { type: "Uuid" }, "projects"],
      ["reviewerIds", { type: "Array", element: { type: "Uuid" } }, "people"],
      ["privateId", { type: "Bytea" }, undefined],
      ["title", { type: "Bytea" }, undefined],
    ]);
    const query = app.projects.include({
      notes: app.notes.include({ project: true, reviewers: true }),
    });
    type Note = s.RowOf<typeof query>["notes"][number];
    expectTypeOf<Note["project"]>().toEqualTypeOf<{ id: string; title: string } | null>();
    expectTypeOf<Note["reviewers"]>().toEqualTypeOf<{ id: string; name: string }[]>();
    expectTypeOf<Note["privateId"]>().toEqualTypeOf<string>();
    expectTypeOf<Note["title"]>().toEqualTypeOf<string>();
  },
);
