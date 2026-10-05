import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp({
  users: s.table({ accountId: s.uuid() }, {}),
  topics: s.table({ title: s.string() }, {}),
  notes: s.table(
    { userId: s.uuid(), topicId: s.uuid(), text: s.string() },
    { user: s.rel("users", "userId"), topic: s.rel("topics", "topicId") },
  ),
});

// The referenced table's read policy compares a session claim, and the insert
// policy of the referencing table checks the referenced row.
const permissions = s.definePermissions(app, ({ policy, session, allOf }) => {
  for (const table of [policy.users, policy.topics, policy.notes]) {
    table.allowUpdate.never();
    table.allowDelete.never();
  }
  policy.users.allowRead.always();
  policy.users.allowInsert.where({ accountId: session.user.account });
  policy.topics.allowInsert.always();
  policy.topics.allowRead.where(session.where({ "claims.role": "editor" }));
  policy.notes.allowRead.always();
  policy.notes.allowInsert.where((row) =>
    allOf([
      policy.users.exists.where({ id: row.userId, accountId: session.user.account }),
      policy.topics.exists.where({ id: row.topicId }),
    ]),
  );
});

const outcome = (write: { wait(options: { tier: "global" }): Promise<unknown> }) =>
  write.wait({ tier: "global" }).then(
    () => "accepted",
    (error: { code?: string; message?: string }) => error.code ?? error.message,
  );

describe("deployed permissions with claims in referenced read policies", () => {
  it("authorizes inserts that check a row whose read policy compares a session claim", async () => {
    const issuer = await startTestJwtIssuer();
    const appId = randomUUID();
    const server = await startLocalJazzServer({
      appId,
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    });
    const sessions: { close(): Promise<void> }[] = [];
    const connect = async (subject: string, claims: Record<string, unknown>) => {
      const session = await createJazzSession({
        appId,
        app,
        permissions,
        serverUrl: server.url,
        tier: "local",
        driver: { type: "memory" },
      });
      sessions.push(session);
      await session.loginOrRegisterJWT(issuer.jwtForUser(subject, claims));
      const client = session.getSnapshot().client!;
      const user = client.db.insert(app.users, { accountId: client.session!.user!.account });
      expect(await outcome(user)).toBe("accepted");
      return { db: client.db, userId: user.value.id };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      const viewer = await connect("viewer", { role: "viewer" });
      const topic = viewer.db.insert(app.topics, { title: "topic" });
      expect(await outcome(topic)).toBe("accepted");

      const editor = await connect("editor", { role: "editor" });
      expect(
        await outcome(
          editor.db.insert(app.notes, {
            userId: editor.userId,
            topicId: topic.value.id,
            text: "own",
          }),
        ),
      ).toBe("accepted");
      expect(
        await outcome(
          editor.db.insert(app.notes, {
            userId: viewer.userId,
            topicId: topic.value.id,
            text: "other user's",
          }),
        ),
      ).toBe("permission_denied");
      // The denial must not break the session's later writes.
      expect(await outcome(editor.db.insert(app.topics, { title: "later" }))).toBe("accepted");
    } finally {
      for (const session of [...sessions].reverse()) {
        await session.close();
      }
      await server.stop();
      await issuer.stop();
    }
  }, 60_000);
});
