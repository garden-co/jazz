// The app's jazz.permissions.json as the engine compiles it: what the Jazz
// server enforces, not the file's source text.
import { expect, it } from "vitest";
import { testPermissions } from "jazz-tools/testing";
import { app } from "../../jazz.generated.js";
import permissionsFile from "../../jazz.permissions.json" with { type: "json" };

const permissions = testPermissions(app, permissionsFile);

it("compiles deny-all CRUD policies for every generated Better Auth table", () => {
  const betterAuthTables = Object.keys(app.wasmSchema).filter((name) =>
    name.startsWith("better_auth_"),
  );
  expect(betterAuthTables.length).toBeGreaterThan(0);
  for (const tableName of betterAuthTables) {
    const table = permissions[tableName];
    expect(table?.select?.using, `${tableName} read`).toEqual({ type: "False" });
    expect(table?.insert?.with_check, `${tableName} insert`).toEqual({ type: "False" });
    expect(table?.update?.using, `${tableName} update (old row)`).toEqual({ type: "False" });
    expect(table?.update?.with_check, `${tableName} update (new row)`).toEqual({ type: "False" });
    expect(table?.delete?.using, `${tableName} delete`).toEqual({ type: "False" });
  }
});

it("checks the old and the new message of an update against the same room/role rule", () => {
  // An update is checked against the old row (`using`) and the new one
  // (`with_check`). Separate per-room rules for each would let a
  // General-message creator move its message into Announcements. So each
  // side, on its own, must be this one rule (an admin in Announcements, or
  // the creator in General), compiled here as a read policy.
  const rule = {
    anyOf: [
      { allOf: ["chat_id = 'announcements'", "session.claims.role = 'admin'"] },
      { allOf: ["chat_id = 'chat-01'", "$createdBy.account = session.user"] },
    ],
  };
  const expected = testPermissions(app, { format: 1, tables: { messages: { read: rule } } })
    .messages?.select?.using;
  expect(expected).toBeDefined();
  const update = permissions.messages?.update;
  expect(update?.using, "update (old row)").toEqual(expected);
  expect(update?.with_check, "update (new row)").toEqual(expected);
});
