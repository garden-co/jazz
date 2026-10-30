// Regression test for garden-co/jazz#3755 (same-transaction policy `exists`),
// reproduced from Wequencer's own schema and permissions.
//
// sessions.allowInsert: always
// sessions.allowUpdate: { "$createdBy.account": me }
// session_members.allowInsert: allowedTo.update("session")
//
// Inserting a session and then its owner membership in ONE mergeable
// transaction must be accepted, exactly as the same two writes awaited
// separately are.
import { expect, it } from "vitest";
import { createPolicyTestApp } from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";

async function outcome(write: Promise<unknown>) {
  try {
    await write;
    return "accepted";
  } catch (error) {
    return String(error).includes("permission_denied") ? "permission_denied" : String(error);
  }
}

it("a membership insert sees the session inserted earlier in the same transaction", async () => {
  const testApp = await createPolicyTestApp(app, permissions, expect);
  const account = crypto.randomUUID();
  const db = testApp.as({
    issuer: "https://repro.test",
    user_id: "ada",
    account_id: account,
    claims: {},
    authMode: "external",
  });

  // Control: separate writes are accepted.
  const session = db.insert(app.sessions, { title: "Separate", tempo_bpm: 120 });
  const separate = [
    await outcome(session.wait({ tier: "global" })),
    await outcome(
      db
        .insert(app.session_members, {
          session_id: session.value.id,
          member_author: account,
          role: "owner",
        })
        .wait({ tier: "global" }),
    ),
  ];

  // The same writes in one transaction.
  const tx = await db.transaction(async (t) => {
    const created = t.insert(app.sessions, { title: "Together", tempo_bpm: 120 });
    t.insert(app.session_members, {
      session_id: created.id,
      member_author: account,
      role: "owner",
    });
  });
  const together = await outcome(tx.wait({ tier: "global" }));

  await testApp.shutdown();
  expect(separate).toEqual(["accepted", "accepted"]);
  expect(together).toBe("accepted");
}, 60_000);
