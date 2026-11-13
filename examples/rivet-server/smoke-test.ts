import assert from "node:assert/strict";
import { createJazzClient, type AccountStore } from "jazz-tools";
import { app } from "./schema.js";

function accountStore(): AccountStore {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
}

const serverUrl = process.env.JAZZ_PUBLIC_URL;
if (!serverUrl) throw new Error("Set JAZZ_PUBLIC_URL to the actor's public /request/jazz URL");
const timeout = setTimeout(() => {
  console.error("Rivet smoke test timed out");
  process.exit(1);
}, 60_000);
async function smoke() {
  const alice = await createJazzClient({
    store: accountStore(),
    serverUrl,
    initial: "device",
    driver: { type: "memory" },
  });
  try {
    const client = alice.getSnapshot().client!;
    const row = await client.db
      .insert(app.todos, {
        title: "Rivet smoke test",
        done: false,
        owner_id: client.session!.user!,
      })
      .wait({ tier: "global" });
    const bob = await createJazzClient({
      store: accountStore(),
      serverUrl,
      initial: "device",
      driver: { type: "memory" },
    });
    try {
      const reader = bob.getSnapshot().client!;
      assert.notEqual(reader.session!.user, client.session!.user);
      const rows = await reader.db.all(app.todos.where({ id: row.id }));
      assert.equal(rows[0]?.title, "Rivet smoke test");
      await assert.rejects(
        reader.db.update(app.todos, row.id, { title: "not my todo" }).wait({ tier: "global" }),
      );
      console.log("Rivet: write, second-client read, and owner permissions passed.");
    } finally {
      await bob.close();
    }
    await client.db.delete(app.todos, row.id).wait({ tier: "global" });
  } finally {
    await alice.close();
  }
}
try {
  await smoke();
} finally {
  clearTimeout(timeout);
}
