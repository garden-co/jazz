// #region quickstart-server-setup-effect
import { serve } from "@hono/node-server";
import { Config, Effect, Layer, ManagedRuntime, Option, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { Jazz, JazzBackend } from "jazz-tools/effect/backend";
import { app as schemaApp } from "../schema.js";
import permissions from "../permissions.js";

// One backend session for the whole server, closed when the server stops.
const JazzLive = Layer.unwrap(
  Effect.gen(function* () {
    return JazzBackend.layer({
      appId: yield* Config.String("JAZZ_APP_ID").pipe(Config.withDefault("todo-server-effect")),
      app: schemaApp,
      permissions,
      driver: { type: "persistent", dataPath: "./data/jazz.db" },
      serverUrl: yield* Config.String("JAZZ_SERVER_URL"),
      initial: { backendSecret: yield* Config.String("JAZZ_BACKEND_SECRET") },
      jwksUrl: Option.getOrUndefined(yield* Config.option(Config.String("JAZZ_JWKS_URL"))),
      allowLocalFirstAuth: yield* Config.Boolean("JAZZ_ALLOW_LOCAL_FIRST_AUTH").pipe(
        Config.withDefault(true),
      ),
    });
  }),
);

const TodoInput = Schema.Struct({ title: Schema.String });
const TodoPatch = Schema.Struct({ done: Schema.Boolean });
const TodoParams = Schema.Struct({ id: Schema.String });
// #endregion quickstart-server-setup-effect

// #region quickstart-server-write-effect
// Each handler reads `Jazz`; `forCurrentRequest` provides it as the user who
// sent the request, with that user's permissions.
const createTodo = HttpRouter.add(
  "POST",
  "/api/todos",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    // A missing or invalid bearer token fails `forCurrentRequest` before this
    // runs, and currently surfaces as a 500 (typed auth errors: #3655).
    const account = jazz.db.getAuthState().session?.user.account;
    if (!account) {
      return yield* HttpServerResponse.json({ error: "Account required" }, { status: 401 });
    }
    const { title } = yield* HttpServerRequest.schemaBodyJson(TodoInput);

    const todo = yield* jazz.insert(schemaApp.todos, { title, done: false, owner_id: account });

    return yield* HttpServerResponse.json(todo, { status: 201 });
  }).pipe(JazzBackend.forCurrentRequest()),
);
// #endregion quickstart-server-write-effect

// #region quickstart-server-read-effect
const listTodos = HttpRouter.add(
  "GET",
  "/api/todos",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    const todos = yield* jazz.all(
      schemaApp.todos.where({ done: false }).orderBy("title", "asc").limit(100),
    );
    return yield* HttpServerResponse.json(todos);
  }).pipe(JazzBackend.forCurrentRequest()),
);
// #endregion quickstart-server-read-effect

// #region quickstart-server-update-effect
const updateTodo = HttpRouter.add(
  "PATCH",
  "/api/todos/:id",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    const { id } = yield* HttpRouter.schemaPathParams(TodoParams);
    const { done } = yield* HttpServerRequest.schemaBodyJson(TodoPatch);
    yield* jazz.update(schemaApp.todos, id, { done });
    return yield* HttpServerResponse.json({ ok: true });
  }).pipe(JazzBackend.forCurrentRequest()),
);

const deleteTodo = HttpRouter.add(
  "DELETE",
  "/api/todos/:id",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    const { id } = yield* HttpRouter.schemaPathParams(TodoParams);
    yield* jazz.delete(schemaApp.todos, id);
    return yield* HttpServerResponse.json({ ok: true });
  }).pipe(JazzBackend.forCurrentRequest()),
);
// #endregion quickstart-server-update-effect

// #region backend-request-handler-effect
// Written against `Jazz` only, so the same code runs in a browser, in a test
// against an in-memory database, or here as the requesting user.
export const listDoneTodos = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(schemaApp.todos.where({ done: true }));
});

// A standalone route: mount it with the quickstart routes below if you want it served.
export const listDoneTodosRoute = HttpRouter.add(
  "GET",
  "/api/todos/done",
  listDoneTodos.pipe(
    Effect.flatMap((rows) => HttpServerResponse.json(rows)),
    JazzBackend.forCurrentRequest(),
    Effect.catchTag("JazzError", () =>
      HttpServerResponse.json({ error: "Failed to query todos" }, { status: 500 }),
    ),
  ),
);
// #endregion backend-request-handler-effect

// #region quickstart-server-listen-effect
const Routes = Layer.mergeAll(createTodo, listTodos, updateTodo, deleteTodo);

// `toWebHandler` turns the routes into a fetch handler that any Node or edge
// server can host. The Jazz session is opened once and shared by every request.
const jazzRuntime = ManagedRuntime.make(JazzLive);
const services = await jazzRuntime.context();
const { handler, dispose } = HttpRouter.toWebHandler(Routes);

const server = serve({ fetch: (request) => handler(request, services), port: 3000 }, (info) => {
  console.log(`Server running on http://localhost:${info.port}`);
});
process.on("SIGTERM", () => server.close(() => void dispose().then(() => jazzRuntime.dispose())));
// #endregion quickstart-server-listen-effect
