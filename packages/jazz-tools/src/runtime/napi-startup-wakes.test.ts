import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NapiDb } from "jazz-napi";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createJazzContext } from "../backend/create-jazz-context.js";

// Native callback counts are intentional: correct synchronous row callbacks
// alone do not reveal a queued host-task backlog (#2979).
it("coalesces native subscription wakes and retains a wake requested from its callback", async () => {
  const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
  });
  const path = mkdtempSync(join(tmpdir(), "jazz-napi-startup-wakes-"));
  const originalScheduler = NapiDb.prototype.setTickScheduler;
  const wakes: string[] = [];
  let onWake: (() => void) | undefined;
  NapiDb.prototype.setTickScheduler = function (callback) {
    return originalScheduler.call(this, (error, urgency) => {
      wakes.push(urgency);
      const reenter = onWake;
      onWake = undefined;
      reenter?.();
      callback(error, urgency);
    });
  };
  const context = createJazzContext({
    appId: "00000000-0000-0000-0000-000000002979",
    app,
    permissions,
    driver: { type: "persistent", dataPath: path },
  });
  const handles: (() => void)[] = [];
  try {
    const db = context.forSession({
      issuer: "https://issuer.example",
      user_id: "synthetic-user",
      claims: {},
      account_id: "00000000-0000-0000-0000-000000000001",
      authMode: "external",
    });
    const row = db.insert(app.notes, { text: "initial" }).value;
    await new Promise((resolve) => setTimeout(resolve, 25));
    wakes.length = 0;
    let observed = "";
    handles.push(
      db.subscribe(app.notes.where({ id: row.id }), (rows) => {
        observed = rows[0]?.text ?? "";
      }),
    );
    let unrelatedCallbacks = 0;
    for (let i = 0; i < 16; i++) {
      handles.push(
        db.subscribe(app.notes.where({ text: `unrelated-${i}` }), (rows) => {
          expect(rows).toEqual([]);
          unrelatedCallbacks++;
        }),
      );
    }
    onWake = () => {
      handles.push(
        db.subscribe(app.notes.where({ text: "reentrant" }), (rows) => {
          expect(rows).toEqual([]);
          unrelatedCallbacks++;
        }),
      );
    };
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(unrelatedCallbacks).toBe(17);
    expect(observed).toBe("initial");
    // One queued burst and one new immediate wake admitted during delivery.
    expect(wakes.filter((urgency) => urgency === "immediate")).toHaveLength(2);
    expect(wakes.length).toBeLessThan(16);
    db.update(app.notes, row.id, { text: "edited" });
    expect(observed).toBe("edited");
    expect(unrelatedCallbacks).toBe(17);
  } finally {
    onWake = undefined;
    for (const handle of handles) handle();
    await context.shutdown();
    NapiDb.prototype.setTickScheduler = originalScheduler;
    rmSync(path, { recursive: true, force: true });
  }
});
