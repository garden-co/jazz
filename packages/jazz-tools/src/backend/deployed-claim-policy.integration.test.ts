import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp(
  {
    users: s.table({ accountId: s.uuid() }, {}),
    topics: s.table({ title: s.string() }, {}),
    notes: s.table(
      { userId: s.uuid(), topicId: s.uuid(), text: s.string() },
      { user: s.rel("users", "userId"), topic: s.rel("topics", "topicId") },
    ),
  },
  { migrations: "none" },
);

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
    const owner = await createJazzSession({
      appId,
      app,
      permissions,
      serverUrl: server.url,
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
      driver: { type: "memory" },
      initial: { backendSecret: server.backendSecret },
    });
    // Each user is a request scope verified from its own bearer JWT, carrying
    // the claims the policies compare.
    const connect = async (subject: string, claims: Record<string, unknown>) => {
      const backend = owner.getSnapshot().client!;
      const token = await issuer.jwtForUser(subject, claims);
      const db = await backend.forRequest(
        { headers: { authorization: `Bearer ${token}` } },
        { account: "login-or-register" },
      );
      // Learn the user's account from a row it wrote.
      const probe = db.insert(app.topics, { title: `${subject}'s first topic` });
      expect(await outcome(probe)).toBe("accepted");
      const { $createdBy } = (await backend.db.one(
        app.topics.select("$createdBy").where({ id: probe.value.id }),
      ))!;
      const user = db.insert(app.users, { accountId: $createdBy.account });
      expect(await outcome(user)).toBe("accepted");
      return { db, userId: user.value.id };
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
      await owner.close();
      await server.stop();
      await issuer.stop();
    }
  }, 60_000);
});
