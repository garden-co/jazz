import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb, type Db } from "../../src/runtime/db.js";
import { uniqueDbName } from "./support.js";

const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
});

let db: Db | undefined;

afterEach(async () => {
  await db?.shutdown();
  db = undefined;
});

describe("browser streaming mutations", () => {
  it("uses the inferred object-shaped API for insert, update, and upsert", async () => {
    db = await createDb({
      appId: "browser-streaming-mutations",
      driver: { type: "persistent", dbName: uniqueDbName("browser-streaming-mutations") },
    });

    const inserted = await db.insertStreaming(app.todos, {
      title: new ReadableStream<string | Uint8Array>({
        start(controller) {
          controller.enqueue("browser ");
          controller.enqueue(new TextEncoder().encode("stream"));
          controller.close();
        },
      }),
      done: false,
    });
    await db.updateStreaming(app.todos, inserted.value.id, {
      title: (async function* () {
        yield "updated";
      })(),
      done: true,
    });
    await db.upsertStreaming(app.todos, inserted.value.id, {
      title: (async function* () {
        yield "upserted";
      })(),
    });

    await expect(db.all(app.todos)).resolves.toEqual([
      { id: inserted.value.id, title: "upserted", done: true },
    ]);
  });
});
