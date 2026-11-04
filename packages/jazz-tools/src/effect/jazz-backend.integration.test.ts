import { Effect, Fiber, Layer, Stream } from "effect";
import { HttpServerRequest } from "effect/http";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";
import { Jazz, JazzBackend, JazzWriteRejected } from "./backend/index.js";

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
    const server = await startTestJazz({
      app,
      permissions,
      coreOptions: { allowLocalFirstAuth: true },
    });
    try {
      const backendLayer = JazzBackend.layerJazz(server.jazz);
      const clientLayer = Jazz.layer(await localAccountConfig(server.appId, server.url));
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
            JazzBackend.forRequest(request()),
          );
          // ...and from an `effect/http` request in the handler's context.
          const fromHttp = yield* createPost("from http").pipe(
            JazzBackend.forCurrentRequest(),
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              HttpServerRequest.fromWeb(request()),
            ),
          );

          // Permissions are the user's: a denied write fails with a typed rejection.
          const denied = yield* Effect.gen(function* () {
            const jazz = yield* Jazz;
            return yield* jazz.insert(app.notes, { text: "denied" }, { wait: "global" });
          }).pipe(JazzBackend.forRequest(request()), Effect.flip);

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

  it("isolates concurrent request handlers to each user's own permissions", async () => {
    const server = await startTestJazz({ app, permissions });
    try {
      const backendLayer = JazzBackend.layerJazz(server.jazz);
      const users = ["diarist-a", "diarist-b"];
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
            writeThenRead(`${user} entry`).pipe(JazzBackend.forRequest(requests[index]!)),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.provide(backendLayer)),
      );

      expect(seen).toEqual([["diarist-a entry"], ["diarist-b entry"]]);
    } finally {
      await server.stop();
    }
  }, 60_000);
});
