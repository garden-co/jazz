import { Effect, Fiber, Layer, Schema, Stream } from "effect";
import { HttpServerRequest } from "effect/http";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazz, memory } from "../backend/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { startTestJazz } from "../testing/index.js";
import { Jazz, JazzBackend, JazzNoAccount, JazzWriteRejected } from "./backend/index.js";

/** A device account of its own (a fresh store) for each session a test opens. */
function memoryAccountStore() {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform: (current: string | null) => string | null) => {
      stored = transform(stored);
    },
  };
}

const app = s.defineApp(
  {
    notes: s.table({ text: s.string() }, {}),
    posts: s.table({ text: s.string() }, {}),
    diaries: s.table({ text: s.string() }, {}),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.diaries.allowRead.where({ $createdBy: session.user });
  policy.diaries.allowInsert.always();
  policy.posts.allowRead.always();
  policy.posts.allowInsert.always();
  policy.posts.allowUpdate.always();
  policy.posts.allowDelete.always();
  policy.notes.allowRead.always();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});

/** A request handler written once, against the plain Jazz service. */
const createPost = (text: string) =>
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    const post = yield* jazz.insert(app.posts, { text }, { wait: "global" });
    const withAuthor = yield* jazz.one(
      app.posts.select("text", "$createdBy").where({ id: post.id }),
    );
    return withAuthor;
  });

describe("JazzBackend Effect layer", () => {
  it("serves request handlers as the requesting user and syncs to Effect clients", async () => {
    // The app's Jazz decides who a request is (its `authenticate`); Effect
    // clients sync as guests through it.
    const server = await startTestJazz({ app, permissions });
    try {
      const backendLayer = JazzBackend.layerJazz(server.jazz);
      const clientLayer = Jazz.layer({
        scope: server.appId,
        serverUrl: server.url,
        initial: "device",
        client: async () => ({ createClient: createDb }),
        localFirst: "now",
        store: memoryAccountStore(),
        driver: { type: "memory" },
      });
      // The reader is a user the backend serves without a client of their own.
      await server.jazz.createAccountFor("effect-reader");
      const headers = { authorization: `Bearer ${server.bearer("effect-reader")}` };
      const request = () => new Request("http://localhost/posts", { headers });

      const result = await Effect.runPromise(
        Effect.gen(function* () {
          // A client subscribed before the backend writes sees the write arrive.
          const client = yield* Jazz;
          const seenByClient = yield* client.stream(app.posts, { tier: "remote" }).pipe(
            Stream.filter((rows) => rows.some((row) => row.text === "from request")),
            Stream.runHead,
            Effect.forkChild,
          );

          // The same handler runs with the user's identity from a plain request...
          const fromRequest = yield* createPost("from request").pipe(
            JazzBackend.forReqUser(request()),
          );
          // ...and from an `effect/http` request in the handler's context.
          const fromHttp = yield* createPost("from http").pipe(
            JazzBackend.forCurrentReqUser(),
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              HttpServerRequest.fromWeb(request()),
            ),
          );

          // Permissions are the user's: a denied write fails with a typed rejection.
          const denied = yield* Effect.gen(function* () {
            const jazz = yield* Jazz;
            return yield* jazz.insert(app.notes, { text: "denied" }, { wait: "global" });
          }).pipe(JazzBackend.forReqUser(request()), Effect.flip);

          return { fromRequest, fromHttp, denied, seenByClient: yield* Fiber.join(seenByClient) };
        }).pipe(Effect.provide(Layer.mergeAll(backendLayer, clientLayer))),
      );

      for (const created of [result.fromRequest, result.fromHttp]) {
        expect(created).toMatchObject({
          _tag: "Some",
          value: {
            $createdBy: {
              identity: { issuer: "jazz:backend", subject: "effect-reader" },
            },
          },
        });
      }
      expect(result.denied).toBeInstanceOf(JazzWriteRejected);
      expect(result.seenByClient).toMatchObject({ _tag: "Some" });
    } finally {
      await server.stop();
    }
  }, 60_000);

  it("fails with JazzNoAccount for a user without an account, until the backend creates one", async () => {
    const server = await startTestJazz({ app, permissions });
    try {
      const request = new Request("http://localhost/posts", {
        headers: { authorization: `Bearer ${server.bearer("newcomer")}` },
      });
      const write = createPost("first post").pipe(JazzBackend.forReqUser(request));
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          // Jazz creates no account on its own: the handler gets a typed failure...
          const refused = yield* write.pipe(
            Effect.flip,
            Effect.flatMap((error) =>
              error instanceof JazzNoAccount ? Effect.succeed(error) : Effect.die(error),
            ),
          );
          // ...until the backend creates the account, once, explicitly.
          const backend = yield* JazzBackend;
          const account = yield* backend.createAccountFor("newcomer");
          const created = yield* write;
          return { refused, account, created };
        }).pipe(Effect.provide(JazzBackend.layerJazz(server.jazz))),
      );
      expect(outcome.refused).toMatchObject({
        _tag: "JazzNoAccount",
        userId: "newcomer",
        operation: "forReqUser",
      });
      expect(outcome.created).toMatchObject({
        _tag: "Some",
        value: { $createdBy: { account: outcome.account } },
      });
    } finally {
      await server.stop();
    }
  }, 60_000);

  it("isolates concurrent request handlers to each user's own permissions", async () => {
    const server = await startTestJazz({ app, permissions });
    try {
      const backendLayer = JazzBackend.layerJazz(server.jazz);
      const users = ["diarist-a", "diarist-b"];
      for (const user of users) await server.jazz.createAccountFor(user);
      const requests = users.map(
        (user) =>
          new Request("http://localhost/diary", {
            headers: { authorization: `Bearer ${server.bearer(user)}` },
          }),
      );

      // One handler, written once: write an entry, then read every visible entry.
      const writeThenRead = (text: string) =>
        Effect.gen(function* () {
          const jazz = yield* Jazz;
          yield* jazz.insert(app.diaries, { text }, { wait: "global" });
          const rows = yield* jazz.all(app.diaries, { tier: "remote" });
          return rows.map((row) => row.text).sort();
        });

      const seen = await Effect.runPromise(
        Effect.all(
          users.map((user, index) =>
            writeThenRead(`${user} entry`).pipe(JazzBackend.forReqUser(requests[index]!)),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.provide(backendLayer)),
      );

      expect(seen).toEqual([["diarist-a entry"], ["diarist-b entry"]]);
    } finally {
      await server.stop();
    }
  }, 60_000);
  it("leaves the request body to the handler under forCurrentReqUser", async () => {
    const seenBodies: Array<ReadableStream | null> = [];
    const jazz = createJazz({
      app,
      permissions,
      store: memory(),
      authenticate: (request) => {
        seenBodies.push(request.body);
        return { userId: "body-reader", expiresAt: Math.floor(Date.now() / 1000) + 3600 };
      },
    });
    try {
      await jazz.createAccountFor("body-reader");
      const handler = Effect.gen(function* () {
        const body = yield* HttpServerRequest.schemaBodyJson(
          Schema.Struct({ text: Schema.String }),
        );
        const created = yield* createPost(body.text);
        return created;
      });
      const created = await Effect.runPromise(
        handler.pipe(
          JazzBackend.forCurrentReqUser(),
          Effect.provideService(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(
              new Request("http://localhost/posts", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ text: "from the body" }),
              }),
            ),
          ),
          Effect.provide(JazzBackend.layerJazz(jazz)),
        ),
      );
      expect(created).toMatchObject({ _tag: "Some", value: { text: "from the body" } });
      // authenticate decides from the method, URL and headers; the body stays unread.
      expect(seenBodies).toEqual([null]);
    } finally {
      await jazz.close();
    }
  }, 60_000);
});
