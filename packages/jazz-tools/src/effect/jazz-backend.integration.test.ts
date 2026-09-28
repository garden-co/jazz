import { randomUUID } from "node:crypto";
import { Effect, Fiber, Layer, Stream } from "effect";
import { HttpServerRequest } from "effect/http";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { Jazz, JazzBackend, JazzWriteRejected } from "./backend/index.js";

const app = s.defineApp({
  notes: s.table({ text: s.string() }, {}),
  posts: s.table({ text: s.string() }, {}),
  diaries: s.table({ text: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy, isCreator }) => {
  policy.diaries.allowRead.where(isCreator);
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
    const issuer = await startTestJwtIssuer();
    const appId = randomUUID();
    const auth = {
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    };
    const server = await startLocalJazzServer({ appId, allowLocalFirstAuth: true, ...auth });
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      const backendLayer = JazzBackend.layer({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        ...auth,
        driver: { type: "memory" },
        initial: { backendSecret: server.backendSecret },
      });
      const clientLayer = Jazz.layer(await localAccountConfig(appId, server.url));
      const token = await issuer.jwtForUser("effect-reader");
      const headers = { authorization: `Bearer ${token}` };
      const account = { account: "login-or-register" } as const;

      const result = await Effect.runPromise(
        Effect.gen(function* () {
          // A client subscribed before the backend writes sees the write arrive.
          const client = yield* Jazz;
          const seenByClient = yield* client.stream(app.posts, { tier: "global" }).pipe(
            Stream.filter((rows) => rows.some((row) => row.text === "from request")),
            Stream.runHead,
            Effect.forkChild,
          );

          // The same handler runs with the user's identity from a plain request...
          const fromRequest = yield* createPost("from request").pipe(
            JazzBackend.forRequest({ headers }, account),
          );
          // ...and from an `effect/http` request in the handler's context.
          const fromHttp = yield* createPost("from http").pipe(
            JazzBackend.forCurrentRequest(account),
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              HttpServerRequest.fromWeb(new Request("http://localhost/posts", { headers })),
            ),
          );

          // Permissions are the user's: a denied write fails with a typed rejection.
          const denied = yield* Effect.gen(function* () {
            const jazz = yield* Jazz;
            return yield* jazz.insert(app.notes, { text: "denied" }, { wait: "global" });
          }).pipe(JazzBackend.forRequest({ headers }, account), Effect.flip);

          return { fromRequest, fromHttp, denied, seenByClient: yield* Fiber.join(seenByClient) };
        }).pipe(Effect.provide(Layer.mergeAll(backendLayer, clientLayer))),
      );

      for (const created of [result.fromRequest, result.fromHttp]) {
        expect(created).toMatchObject({
          _tag: "Some",
          value: { $createdBy: { identity: { issuer: issuer.issuer, subject: "effect-reader" } } },
        });
      }
      expect(result.denied).toBeInstanceOf(JazzWriteRejected);
      expect(result.seenByClient).toMatchObject({ _tag: "Some" });
    } finally {
      await server.stop();
      await issuer.stop();
    }
  }, 60_000);
  it("isolates concurrent request handlers to each user's own permissions", async () => {
    const issuer = await startTestJwtIssuer();
    const appId = randomUUID();
    const auth = {
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    };
    const server = await startLocalJazzServer({ appId, ...auth });
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      const backendLayer = JazzBackend.layer({
        appId,
        serverUrl: server.url,
        app,
        permissions,
        ...auth,
        driver: { type: "memory" },
        initial: { backendSecret: server.backendSecret },
      });
      const users = ["diarist-a", "diarist-b"];
      const requests = await Promise.all(
        users.map(async (user) => ({
          headers: { authorization: `Bearer ${await issuer.jwtForUser(user)}` },
        })),
      );
      const account = { account: "login-or-register" } as const;

      // One handler, written once: write an entry, then read every visible entry.
      const writeThenRead = (text: string) =>
        Effect.gen(function* () {
          const jazz = yield* Jazz;
          yield* jazz.insert(app.diaries, { text }, { wait: "global" });
          const rows = yield* jazz.all(app.diaries, { tier: "global" });
          return rows.map((row) => row.text).sort();
        });

      const seen = await Effect.runPromise(
        Effect.all(
          users.map((user, index) =>
            writeThenRead(`${user} entry`).pipe(JazzBackend.forRequest(requests[index]!, account)),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.provide(backendLayer)),
      );

      expect(seen).toEqual([["diarist-a entry"], ["diarist-b entry"]]);
    } finally {
      await server.stop();
      await issuer.stop();
    }
  }, 60_000);
});
