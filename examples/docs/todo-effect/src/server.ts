// #region quickstart-server-setup-effect
import { serve } from "@hono/node-server";
import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { sqlite } from "jazz-tools/backend";
import { Jazz, JazzBackend } from "jazz-tools/effect/backend";
import { app as schemaApp } from "../schema.js";
import permissions from "../permissions.js";

// Your own auth decides who a request is (a session cookie, a header).
declare function currentUser(request: Request): Promise<{ id: string } | null>;

// The app's Jazz for the whole server, closed when the server stops.
const JazzLive = JazzBackend.layer({
  app: schemaApp,
  permissions,
  store: sqlite({ path: "./data/jazz" }),
  async authenticate(request) {
    const user = await currentUser(request);
    return user ? { userId: user.id } : null;
  },
});

const TodoInput = Schema.Struct({ title: Schema.String });
const TodoPatch = Schema.Struct({ done: Schema.Boolean });
const TodoParams = Schema.Struct({ id: Schema.String });
// #endregion quickstart-server-setup-effect

// #region quickstart-server-write-effect
// Each handler reads `Jazz`; `forCurrentReqUser` provides it as the user who
// sent the request, with that user's permissions.
const createTodo = HttpRouter.add(
  "POST",
  "/api/todos",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    // A request without a signed-in user fails `forCurrentReqUser` before
    // this runs.
    const account = jazz.db.getAuthState().session?.user;
    if (!account) {
      return yield* HttpServerResponse.json({ error: "Account required" }, { status: 401 });
    }
    const { title } = yield* HttpServerRequest.schemaBodyJson(TodoInput);

    const todo = yield* jazz.insert(schemaApp.todos, { title, done: false, owner_id: account });

    return yield* HttpServerResponse.json(todo, { status: 201 });
  }).pipe(JazzBackend.forCurrentReqUser()),
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
  }).pipe(JazzBackend.forCurrentReqUser()),
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
  }).pipe(JazzBackend.forCurrentReqUser()),
);

const deleteTodo = HttpRouter.add(
  "DELETE",
  "/api/todos/:id",
  Effect.gen(function* () {
    const jazz = yield* Jazz;
    const { id } = yield* HttpRouter.schemaPathParams(TodoParams);
    yield* jazz.delete(schemaApp.todos, id);
    return yield* HttpServerResponse.json({ ok: true });
  }).pipe(JazzBackend.forCurrentReqUser()),
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
    JazzBackend.forCurrentReqUser(),
    Effect.catchTag("JazzError", () =>
      HttpServerResponse.json({ error: "Failed to query todos" }, { status: 500 }),
    ),
  ),
);
// #endregion backend-request-handler-effect

// #region quickstart-server-listen-effect
const Routes = Layer.mergeAll(createTodo, listTodos, updateTodo, deleteTodo);

// `toWebHandler` turns the routes into a fetch handler that any Node or edge
// server can host. Jazz is opened once and shared by every request.
const jazzRuntime = ManagedRuntime.make(JazzLive);
const services = await jazzRuntime.context();
const { handler, dispose } = HttpRouter.toWebHandler(Routes);

const server = serve({ fetch: (request) => handler(request, services), port: 3000 }, (info) => {
  console.log(`Server running on http://localhost:${info.port}`);
});
// Close Jazz first: it ends the sync connections that server.close() waits for.
process.on("SIGTERM", () => {
  void dispose()
    .then(() => jazzRuntime.dispose())
    .then(() => server.close());
});
// #endregion quickstart-server-listen-effect
