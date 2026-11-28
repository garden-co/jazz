import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { translateQuery } from "../runtime/query-adapter.js";
import { testApp } from "../testing/index.js";
import { EncryptedGuard, type ThinRequestDescription } from "./encrypted-guard.js";

// `tasks.title` (text, compared for equality) and `files.blob` (bytes) are
// encrypted, in the space their `projectId` names; every other column, and
// `projects` and `notes`, are plain.
const app = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes", note: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
    files: {
      columns: { projectId: "uuid", blob: "bytes", name: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { blob: "bytes" } },
    },
    notes: {
      columns: { text: "string", taskId: "uuid?" },
      relations: { task: { rel: "tasks", column: "taskId" } },
    },
  },
}) as never as Record<
  string,
  { _schema: WasmSchema } & Record<string, (...args: never[]) => never>
>;
const schema = (app as never as { wasmSchema: WasmSchema }).wasmSchema;
const guard = () => new EncryptedGuard(schema);

/** The runtime query a `Db` sends for `query`. */
function runtimeQuery(query: unknown): string {
  return translateQuery((query as { _build(): string })._build(), schema);
}

const project = "00000000-0000-7000-8000-000000000001";
const row = new Uint8Array(16).fill(1);

function refusal(request: ThinRequestDescription): unknown {
  try {
    guard().admit(request);
    return undefined;
  } catch (error) {
    return error;
  }
}

function query(queryJson: string): ThinRequestDescription {
  return { type: "query", queryJson, options: {} };
}

describe("the thin client's encrypted-column guard", () => {
  // Every shape it cannot prove plain counts as touching encryption.
  const touching: Array<[string, () => string]> = [
    ["a read with no column list", () => runtimeQuery(app.tasks!)],
    [
      "a column list naming an encrypted column",
      () =>
        runtimeQuery(
          (app.tasks as never as { select(...c: string[]): unknown }).select("note", "title"),
        ),
    ],
    [
      "a filter on an encrypted column",
      () =>
        runtimeQuery(
          (app.tasks as never as { select(c: string): { where(w: object): unknown } })
            .select("note")
            .where({ title: "secret" }),
        ),
    ],
    [
      "an ordering by an encrypted column",
      () =>
        runtimeQuery(
          (app.tasks as never as { select(c: string): { orderBy(c: string): unknown } })
            .select("note")
            .orderBy("title"),
        ),
    ],
    [
      "a plain table including the encrypted one",
      () =>
        runtimeQuery(
          (app.notes as never as { include(i: object): unknown }).include({ task: true }),
        ),
    ],
    [
      "a relation query reaching the encrypted table",
      () =>
        runtimeQuery(
          (app.tasks as never as { select(c: string): { hopTo(r: string): unknown } })
            .select("note")
            .hopTo("project"),
        ),
    ],
    [
      "the history relation of the encrypted table",
      () => JSON.stringify({ table: "history.tasks", conditions: [], select_columns: ["note"] }),
    ],
    [
      "an unknown key in an otherwise plain read",
      () =>
        JSON.stringify({
          table: "tasks",
          conditions: [],
          select_columns: ["note"],
          projection_hint: "anything",
        }),
    ],
    [
      "a filter shape the guard does not read",
      () =>
        JSON.stringify({
          table: "tasks",
          conditions: [{ column: "note", op: "eq", value: "x" }],
          select_columns: ["note"],
        }),
    ],
    [
      "a comparison against another column",
      () =>
        JSON.stringify({
          table: "tasks",
          conditions: [
            { Cmp: { left: { column: "note" }, op: "Eq", right: { Column: { column: "title" } } } },
          ],
          select_columns: ["note"],
        }),
    ],
    [
      "a column the table does not have",
      () => JSON.stringify({ table: "tasks", conditions: [], select_columns: ["nothing"] }),
    ],
  ];
  it.each(touching)("refuses %s", (_, make) => {
    const error = refusal(query(make()));
    expect(error).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((error as EncryptionNeedsLocalFirstError).table).toBe("tasks");
    expect((error as EncryptionNeedsLocalFirstError).reason).toBe("no-loader");
  });

  const plain: Array<[string, () => string]> = [
    ["a read of a plain table", () => runtimeQuery(app.notes!)],
    [
      "plain columns of the encrypted table, filtered and ordered by plain columns",
      () =>
        runtimeQuery(
          (
            app.tasks as never as {
              select(...c: string[]): { where(w: object): { orderBy(c: string): unknown } };
            }
          )
            .select("projectId", "note")
            .where({ projectId: project, note: { contains: "x" } })
            .orderBy("note"),
        ),
    ],
  ];
  it.each(plain)("admits %s", (_, make) => {
    expect(refusal(query(make()))).toBeUndefined();
  });

  it("refuses every whole-row write to the encrypted table, even one naming no encrypted column", () => {
    for (const type of ["insert", "upsert", "restore"] as const) {
      const error = refusal({
        type,
        table: "tasks",
        row,
        values: { projectId: { type: "Uuid", value: project } },
        options: {},
      });
      expect(error, type).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    }
  });

  it("refuses an update naming an encrypted, equality, space or unknown column, and admits a plain one", () => {
    const update = (values: Record<string, unknown>): ThinRequestDescription =>
      ({ type: "update", table: "tasks", row, values, options: {} }) as never;
    const text = { type: "Text", value: "x" };
    for (const column of ["title", "__e2ee_eq_title", "projectId", "nothing"]) {
      const error = refusal(update({ [column]: text }));
      expect(error, column).toBeInstanceOf(EncryptionNeedsLocalFirstError);
      expect((error as EncryptionNeedsLocalFirstError).column, column).toBe(column);
    }
    expect(refusal(update({ note: text }))).toBeUndefined();
    expect(refusal({ type: "delete", table: "tasks", row, options: {} })).toBeUndefined();
  });

  it("refuses a partial edit of an encrypted column as local-first does, and fails closed on an unknown descriptor", () => {
    const edit = (descriptors: unknown[]): ThinRequestDescription => ({
      type: "updateLargeValues",
      table: "tasks",
      row,
      values: {},
      descriptors,
    });
    const partial = refusal(
      edit([{ kind: "text_edit", column: "title", text: "new", edited_from: "old" }]),
    );
    expect(partial).not.toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((partial as Error).message).toBe(
      "reading or writing part of encrypted column `tasks.title` is not supported yet",
    );
    expect(refusal(edit([{ kind: "rewrite", column: "note" }]))).toBeInstanceOf(
      EncryptionNeedsLocalFirstError,
    );
    expect(refusal(edit([{ column: "note" }]))).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect(
      refusal(edit([{ kind: "text_edit", column: "note", text: "b", edited_from: "a" }])),
    ).toBeUndefined();
  });

  it("refuses a stream into an encrypted column and every streaming whole-row write", () => {
    const begin = (table: string, mutation: "insert" | "update", column: string) => () =>
      guard().admit({
        type: "beginStream",
        stream: 1,
        table,
        row,
        values: {},
        column,
        mutation,
        options: {},
      });
    expect(begin("files", "update", "blob")).toThrow(EncryptionNeedsLocalFirstError);
    expect(begin("files", "insert", "name")).toThrow(EncryptionNeedsLocalFirstError);
    expect(begin("files", "update", "name")).not.toThrow();
  });

  it("sends a stream's chunks only for a stream it admitted", () => {
    const guarded = guard();
    const { stream } = guarded.admit({
      type: "beginStream",
      stream: 7,
      table: "notes",
      row,
      values: {},
      column: "text",
      mutation: "update",
      options: {},
    });
    expect(guarded.admit({ type: "streamChunk", stream, chunk: new Uint8Array([1]) })).toEqual({
      type: "streamChunk",
      stream: 7,
      chunk: new Uint8Array([1]),
    });
    expect(() =>
      guarded.admit({ type: "streamChunk", stream: { id: 7 }, chunk: new Uint8Array([1]) }),
    ).toThrow("did not begin");
    guarded.admit({ type: "finishStream", stream });
    expect(() => guarded.admit({ type: "finishStream", stream })).toThrow("did not begin");
  });

  it("refuses a schema with encryption tables but no declarations", () => {
    const { ...unbound } = schema;
    expect(Object.keys(unbound).some((table) => table.startsWith("__e2ee_"))).toBe(true);
    expect(() => new EncryptedGuard(unbound)).toThrow("no encryption declarations");
  });

  it("is the only way the thin runtime makes request bytes", () => {
    const source = (file: string) => readFileSync(new URL(file, import.meta.url), "utf8");
    const runtime = source("./thin-runtime.ts");
    for (const encoder of [
      "encodeCellsForRow",
      "encodeCellsForPatch",
      "encodeCellsForStreamingRow",
      "encodeCellsForStreamingPatch",
      "encodeQueryJson",
      "encodeThinRequest",
    ]) {
      expect(runtime, encoder).not.toMatch(new RegExp(`\\b${encoder}\\b`));
    }
    // Only the guard turns a description into the request the wire takes.
    const minting = readdirSync(new URL(".", import.meta.url))
      .filter((file) => file.endsWith(".ts") && !file.endsWith(".test.ts"))
      .filter((file) => /as ThinRequest\b/.test(source(`./${file}`)));
    expect(minting).toEqual(["encrypted-guard.ts"]);
  });
});
