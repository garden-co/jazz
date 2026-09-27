import { describe, expect, it } from "vitest";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb } from "./account-fixtures.js";

const app = s.defineApp({
  folders: s.table({ name: s.string(), description: s.string() }, {}),
  notes: s.table(
    { title: s.string(), folderId: s.uuid() },
    { folder: s.rel("folders", "folderId") },
  ),
});

for (const storage of ["memory", "persistent"] as const) {
  describe(`pending read progress with ${storage} storage`, () => {
    it("serves concurrent includes and queued writes while host messages and timers run", async () => {
      const db = await createBrowserTestDb({
        appId: crypto.randomUUID(),
        secret: generateAuthSecret(),
        logLevel: "warn",
        driver:
          storage === "memory"
            ? { type: "memory" }
            : { type: "persistent", dbName: `read-wakes-${crypto.randomUUID()}` },
      });
      const channel = new MessageChannel();
      let messages = 0;
      let timers = 0;
      let frames = 0;
      let frame: number | undefined;
      let running = true;
      let timer: ReturnType<typeof setTimeout> | undefined;
      channel.port1.onmessage = () => {
        messages += 1;
        if (running) channel.port2.postMessage(null);
      };
      const tick = () => {
        timers += 1;
        if (running) timer = setTimeout(tick, 0);
      };
      try {
        let folderId = "";
        const inserted = await db.transaction((tx) => {
          folderId = tx.insert(app.folders, { name: "Shared", description: "x".repeat(2048) }).id;
          for (let i = 0; i < 1500; i++) tx.insert(app.notes, { title: `Note ${i}`, folderId });
        });
        await inserted.wait({ tier: "local" });
        channel.port2.postMessage(null);
        timer = setTimeout(tick, 0);
        const paint = () => {
          frames += 1;
          if (running) frame = requestAnimationFrame(paint);
        };
        frame = requestAnimationFrame(paint);
        const query = app.notes.include({ folder: true });
        const first = db.all(query, { tier: "local" });
        const second = db.all(query, { tier: "local" });
        const write = db.transaction((tx) => tx.insert(app.notes, { title: "Queued", folderId }));
        const [alice, bob, queued] = await Promise.all([first, second, write]);
        await queued.wait({ tier: "local" });
        for (const rows of [alice, bob]) {
          expect(rows.length).toBeGreaterThanOrEqual(1500);
          expect(rows.length).toBeLessThanOrEqual(1501);
          expect(new Set(rows.map((row) => row.id)).size).toBe(rows.length);
          for (const row of rows) {
            expect(row.folder?.id).toBe(folderId);
            expect(row.folder?.name).toBe("Shared");
            expect(row.folder?.description).toBe("x".repeat(2048));
          }
        }
        expect(messages).toBeGreaterThan(0);
        expect(timers).toBeGreaterThan(0);
        expect(frames).toBeGreaterThan(0);
        expect(await db.all(app.notes, { tier: "local" })).toHaveLength(1501);
      } finally {
        running = false;
        if (timer !== undefined) clearTimeout(timer);
        if (frame !== undefined) cancelAnimationFrame(frame);
        channel.port1.close();
        channel.port2.close();
        await db.shutdown();
      }
    }, 60_000);
  });
}
