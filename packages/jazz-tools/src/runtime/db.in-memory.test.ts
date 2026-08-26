import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb, type Db } from "./db.js";

const schema = {
  notes: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
type Note = s.RowOf<typeof app.notes>;

const largeValueSchema = {
  documents: s.table({
    payload: s.bytes(),
    body: s.string(),
    metadata: s.json(),
    done: s.boolean(),
  }),
};
type LargeValueAppSchema = s.Schema<typeof largeValueSchema>;
const largeValues: s.App<LargeValueAppSchema> = s.defineApp(largeValueSchema);

describe("createDb in-memory driver", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("opens a native Db with the current ordinary-column schema layout", async () => {
    db = await createDb({
      appId: "in-memory-current-column-layout-test",
      driver: { type: "memory" },
    });

    // Opening the native client decodes and compiles the TypeScript-authored
    // source schema before this query can run.
    await expect(db.all(app.notes)).resolves.toEqual([]);
  });

  it("can read and write data without connecting to a server", async () => {
    db = await createDb({
      appId: "in-memory-db-test",
      driver: { type: "memory" },
    });

    const { value: inserted } = db.insert(app.notes, {
      title: "Draft test",
      done: false,
    });

    await db.update(app.notes, inserted.id, { done: true }).wait({ tier: "local" });

    const updated = await db.one<Note>(app.notes.where({ id: { eq: inserted.id } }));
    expect(updated).toEqual({
      id: inserted.id,
      title: "Draft test",
      done: true,
    });

    const rows = await db.all<Note>(app.notes.where({ done: true }));
    expect(rows).toEqual([updated]);
  });

  it("executes typed partial selects and page-relative diffs end to end", async () => {
    db = await createDb({
      appId: "in-memory-large-value-dsl-test",
      driver: { type: "memory" },
      backendSecret: "test-backend-secret",
      jwtToken:
        "header.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwic3ViIjoibGFyZ2UtdmFsdWUtdGVzdC11c2VyIn0.signature",
    });

    const payloadOffset = 70_000;
    const payload = new Uint8Array(payloadOffset + 6).fill(7);
    payload.set([0, 1, 2, 3, 4, 5], payloadOffset);
    const textPrefix = "a".repeat(70_000);
    const body = `${textPrefix}A😀BC`;
    const { value: inserted } = db.insert(largeValues.documents, {
      payload,
      body,
      metadata: { padding: "p".repeat(70_000), nested: { answer: 42 } },
      done: false,
    });
    const [page] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        payload: { from: payloadOffset + 1, to: payloadOffset + 5 },
        body: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
        metadata: { at: "/nested/answer" },
      }),
    );
    expect(page).toEqual({
      id: inserted.id,
      payload: new Uint8Array([1, 2, 3, 4]),
      body: "😀",
      metadata: 42,
    });

    const [utf8Page] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        body: { fromUtf8: textPrefix.length + 1, toUtf8: textPrefix.length + 5 },
      }),
    );
    expect(utf8Page).toEqual({ id: inserted.id, body: "😀" });

    await db
      .applyDiffs(largeValues.documents, inserted.id, {
        payload: {
          within: { from: payloadOffset + 1, to: payloadOffset + 5 },
          splices: [{ at: 1, delete: 2, insert: new Uint8Array([9, 8]) }],
        },
        body: {
          within: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
          splices: [{ at: 0, delete: 2, insert: "🪩" }],
        },
        metadata: { edits: [{ op: "set", at: "/nested/answer", value: 43 }] },
      })
      .wait({ tier: "local" });

    await db
      .applyDiffs(largeValues.documents, inserted.id, {
        body: {
          within: { fromUtf8: textPrefix.length + 1, toUtf8: textPrefix.length + 5 },
          splices: [{ atUtf8: 0, deleteUtf8: 4, insert: "🚀" }],
        },
      })
      .wait({ tier: "local" });

    const [updated] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        payload: { from: payloadOffset, to: payloadOffset + 6 },
        body: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
        metadata: { at: "/nested/answer" },
      }),
    );
    expect(updated).toEqual({
      id: inserted.id,
      payload: new Uint8Array([0, 1, 9, 8, 4, 5]),
      body: "🚀",
      metadata: 43,
    });
  });
});
