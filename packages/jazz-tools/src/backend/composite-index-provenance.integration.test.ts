import { expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { createJazz, memory } from "./index.js";

// A room's newest page (roomId = ? ORDER BY $createdAt DESC LIMIT n) can be
// served from a composite index ending in `$createdAt`. The engine accepts
// the declaration and the page reads newest first. (That the plan
// uses the index without sorting is pinned in crates/jazz/tests/composite_indexes.rs.)
const app = testApp({
  tables: {
    messages: {
      columns: { roomId: "string", text: "string" },
      indexes: [["roomId", "$createdAt"]],
    },
  },
});
const permissions = {
  format: 1,
  tables: { messages: { read: "true", insert: "true" } },
};

it("serves a room's newest page from a composite index on (roomId, $createdAt)", async () => {
  const jazz = await createJazz({ app, permissions, store: memory() }).ready();
  try {
    for (let n = 0; n < 6; n++) {
      await jazz.db
        .insert(app.messages, { roomId: n % 2 ? "b" : "a", text: `m${n}` })
        .wait({ tier: "local" });
      await new Promise((resolve) => setTimeout(resolve, 2));
    }
    const page = await jazz.db.all(
      app.messages.where({ roomId: "a" }).orderBy("$createdAt", "desc").limit(2),
    );
    expect(page.map((message) => message.text)).toEqual(["m4", "m2"]);
  } finally {
    await jazz.close();
  }
});
