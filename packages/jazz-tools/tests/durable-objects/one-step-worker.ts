// The all-Cloudflare deployment as the docs show it: `jazzWorker`, two
// Durable Object bindings, and no secret.
import { acceptDeviceAccounts, createJazz, durableObjects } from "jazz-tools/backend";
import { jazzWorker, type JazzWorkerEnv } from "jazz-tools/cloudflare";
import { app } from "./backend-app.generated.js";

const permissions = { format: 1, tables: { todos: { read: "true", insert: "true" } } };

const worker = jazzWorker(
  (env: JazzWorkerEnv) => {
    const jazz = createJazz({
      app,
      permissions,
      store: durableObjects({ binding: env.JAZZ }),
      authenticate: (request) => acceptDeviceAccounts(request),
    });
    return {
      jazz,
      async fetch(request: Request) {
        const url = new URL(request.url);
        try {
          if (url.pathname === "/todos" && request.method === "POST") {
            await jazz.db
              .insert(app.todos, { title: await request.text(), done: false })
              .wait({ tier: "global" });
            return new Response("ok");
          }
          if (url.pathname === "/app-id") return new Response((await jazz.ready()).appId);
          if (url.pathname === "/todos") {
            return Response.json((await jazz.db.all(app.todos)).map((todo) => todo.title).sort());
          }
        } catch (error) {
          return new Response(String((error as Error)?.stack ?? error), { status: 500 });
        }
        return new Response("Not found", { status: 404 });
      },
    };
  },
  { blobs: "sqlite" },
);

export const { JazzCore, JazzBackend } = worker;
export default worker;
