import { describe, expect, it } from "vitest";
import { startTestJazz, testApp } from "../testing/index.js";

const app = testApp({
  tables: {
    users: { columns: { accountId: "uuid" } },
    topics: { columns: { title: "string" } },
    notes: {
      columns: { userId: "uuid", topicId: "uuid", text: "string" },
      relations: {
        user: { rel: "users", column: "userId" },
        topic: { rel: "topics", column: "topicId" },
      },
    },
  },
});

// The referenced table's read policy compares a session claim, and the insert
// policy of the referencing table checks the referenced row. No table allows
// update or delete.
const permissions = {
  format: 1,
  tables: {
    users: { read: "true", insert: "accountId = session.user" },
    topics: { read: "session.claims.role = 'editor'", insert: "true" },
    notes: {
      read: "true",
      insert: {
        allOf: [
          "exists users(id = this.userId, accountId = session.user)",
          "exists topics(id = this.topicId)",
        ],
      },
    },
  },
};

const outcome = (write: { wait(options: { tier: "global" }): Promise<unknown> }) =>
  write.wait({ tier: "global" }).then(
    () => "accepted",
    (error: { code?: string; message?: string }) => error.code ?? error.message,
  );

describe("deployed permissions with claims in referenced read policies", () => {
  it("authorizes inserts that check a row whose read policy compares a session claim", async () => {
    const server = await startTestJazz({ app, permissions });
    // Each user is a request scope the app's `authenticate` names, carrying
    // the claims the policies compare.
    const connect = async (subject: string, claims: Record<string, unknown>) => {
      await server.jazz.createAccountFor(subject);
      const db = await server.jazz.forReqUser(
        new Request("http://app.example/", {
          headers: { authorization: `Bearer ${server.bearer(subject, claims)}` },
        }),
      );
      // Learn the user's account from a row it wrote.
      const probe = db.insert(app.topics, { title: `${subject}'s first topic` });
      expect(await outcome(probe)).toBe("accepted");
      const { $createdBy } = (await server.jazz.db.one(
        app.topics.select("$createdBy").where({ id: probe.value.id }),
      ))!;
      const user = db.insert(app.users, { accountId: $createdBy.account });
      expect(await outcome(user)).toBe("accepted");
      return { db, userId: user.value.id };
    };
    try {
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

      // waitForSync reports a refusal that settled before it was called,
      // even when a later write was accepted (jazz#5728), and only once.
      editor.db.insert(app.notes, {
        userId: viewer.userId,
        topicId: topic.value.id,
        text: "refused, unwaited",
      });
      expect(await outcome(editor.db.insert(app.topics, { title: "accepted after" }))).toBe(
        "accepted",
      );
      await expect(editor.db.waitForSync()).rejects.toMatchObject({ code: "permission_denied" });
      await editor.db.waitForSync();
    } finally {
      await server.stop();
    }
  }, 60_000);
});
