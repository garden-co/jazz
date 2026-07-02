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

describe("createDb in-memory driver", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
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
});
