// An app's whole Cloudflare deployment in one Worker, as an app writes it:
// the core (`JazzCore`), the app's backend object running createJazz
// (`JazzBackend`) with a tiny todos API on `jazz.db`, and a `fetch` that
// forwards everything to the backend object.
import {
  acceptDeviceAccounts,
  createJazz,
  durableObjects,
  jazzBackend,
  routeToJazzBackend,
} from "jazz-tools/backend";
import { app } from "./backend-app.generated.js";

export { JazzCore } from "jazz-tools/durable-object";

interface Env {
  JAZZ: Parameters<typeof durableObjects>[0] extends { binding: infer B } ? B : never;
  JAZZ_CORE_SECRET: string;
}

const permissions = { format: 1, tables: { todos: { read: "true", insert: "true" } } };

export const JazzBackend = jazzBackend((env: Env) => {
  const jazz = createJazz({
    app,
    permissions,
    store: durableObjects({ binding: env.JAZZ, secret: env.JAZZ_CORE_SECRET }),
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
});

export default { fetch: routeToJazzBackend };
