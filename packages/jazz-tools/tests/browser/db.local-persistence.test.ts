import { expect, it } from "vitest";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";

const app = s.defineApp({
  tasks: s.table({ title: s.string(), done: s.boolean() }),
});

it("settles an empty local read and retains acknowledged writes across repeated reopen", async () => {
  const config = {
    appId: crypto.randomUUID(),
    secret: generateAuthSecret(),
    driver: { type: "persistent" as const, dbName: `local-reopen-${crypto.randomUUID()}` },
  };
  let db = await createDb(config);
  const titles: string[] = [];
  try {
    // Safari used to overflow the WASM stack while a projection fetched its
    // already evaluated input. This first read hung before any write was made.
    expect(await db.all(app.tasks, { tier: "local" })).toEqual([]);
    for (let cycle = 0; cycle < 3; cycle++) {
      const title = `synthetic task ${cycle}`;
      await db.insert(app.tasks, { title, done: false }).wait({ tier: "local" });
      titles.push(title);
      expect((await db.all(app.tasks, { tier: "local" })).map((row) => row.title).sort()).toEqual(
        titles,
      );
      await db.shutdown();
      db = await createDb(config);
      expect((await db.all(app.tasks, { tier: "local" })).map((row) => row.title).sort()).toEqual(
        titles,
      );
    }
  } finally {
    await db.shutdown();
  }
}, 60_000);
