import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory } from "./index.js";

// A room's newest page (roomId = ? ORDER BY $createdAt DESC LIMIT n) can be
// served from a composite index ending in `$createdAt`. The engine accepts
// the TypeScript declaration and the page reads newest first. (That the plan
// uses the index without sorting is pinned in crates/jazz/tests/composite_indexes.rs.)
const app = s.defineApp(
  {
    messages: s
      .table({ roomId: s.string(), text: s.string() }, {})
      .compositeIndex(["roomId", "$createdAt"]),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.messages.allowRead.always();
  policy.messages.allowInsert.always();
});

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
