import { Config, Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { Jazz, JazzBackend } from "jazz-tools/effect/backend";
import { app } from "../schema";
import permissions from "../permissions";

// One Jazz backend session for the whole server. It opens on the first API
// request, when `pnpm dev` has already started the local Jazz server, and
// closes when the server stops.
const JazzLive = Layer.unwrap(
  Effect.gen(function* () {
    const appOrigin = yield* Config.String("APP_ORIGIN").pipe(
      Config.withDefault("http://localhost:3001"),
    );
    return JazzBackend.layer({
      appId: yield* Config.String("VITE_JAZZ_APP_ID"),
      serverUrl: yield* Config.String("VITE_JAZZ_SERVER_URL"),
      initial: { backendSecret: yield* Config.String("BACKEND_SECRET") },
      app,
      permissions,
      driver: { type: "memory" },
      // Requests carry the Better Auth JWT the browser also signs in to Jazz with.
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
      allowLocalFirstAuth: false,
    });
  }),
);

/**
 * Delete the current user's completed todos. It only depends on `Jazz`, so
 * it runs with whatever permissions the caller provides it with.
 */
export const clearCompletedTodos = Effect.gen(function* () {
  const jazz = yield* Jazz;
  // "remote" reads the server's current rows rather than this server's cache.
  const done = yield* jazz.all(app.todos.where({ done: true }), { tier: "remote" });
  yield* Effect.forEach(done, (todo) => jazz.delete(app.todos, todo.id, { wait: "global" }), {
    concurrency: "unbounded",
    discard: true,
  });
  return done.length;
});

// `forCurrentRequest` provides `Jazz` as the user whose bearer token sent the
// request, so row permissions apply exactly as they do in the browser.
const clearCompletedRoute = HttpRouter.add(
  "POST",
  "/api/todos/clear-completed",
  clearCompletedTodos.pipe(
    Effect.flatMap((cleared) => HttpServerResponse.json({ cleared })),
    JazzBackend.forCurrentRequest(),
    Effect.catchTags({
      JazzWriteRejected: (error) =>
        HttpServerResponse.json({ error: error.message }, { status: 403 }),
      // A missing or invalid bearer token also lands here (typed auth errors: #3655).
      JazzError: (error) => HttpServerResponse.json({ error: error.message }, { status: 500 }),
    }),
  ),
);

const Routes = Layer.mergeAll(clearCompletedRoute);

const jazzRuntime = ManagedRuntime.make(JazzLive);
const { handler, dispose } = HttpRouter.toWebHandler(Routes);

/** Answer a request for one of the Effect routes above. */
export async function handleJazzApi(request: Request): Promise<Response> {
  return handler(request, await jazzRuntime.context());
}

export async function closeJazzApi(): Promise<void> {
  await dispose();
  await jazzRuntime.dispose();
}
