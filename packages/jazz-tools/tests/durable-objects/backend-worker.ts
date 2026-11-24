// An app's whole Cloudflare deployment in one Worker, as an app writes it:
// the core (`JazzCore`), the app's backend object running createJazz
// (`JazzBackend`) with a tiny todos API on `jazz.db`, and a `fetch` that
// forwards everything to the backend object.
import {
  acceptDeviceAccounts,
  createJazz,
  durableObjects,
  jazzBackend,
  remote,
  routeToJazzBackend,
} from "jazz-tools/backend";
import { app } from "./backend-app.generated.js";

import { createJazzCore } from "jazz-tools/durable-object";
export const JazzCore = createJazzCore({ blobs: "sqlite" });

interface Env {
  JAZZ: Parameters<typeof durableObjects>[0] extends { binding: infer B } ? B : never;
  JAZZ_CORE_SECRET: string;
  /** Test switch: the backend presents this secret instead of the core's. */
  TEST_BACKEND_SECRET?: string;
  TEST_CORE_URL?: string;
  TEST_REMOTE_APP_ID?: string;
  /** Test switch: `authenticate` lets no one in. */
  TEST_REFUSE_ALL?: string;
}

const permissions = { format: 1, tables: { todos: { read: "true", insert: "true" } } };

export const JazzBackend = jazzBackend((env: Env) => {
  const secret = env.TEST_BACKEND_SECRET ?? env.JAZZ_CORE_SECRET;
  const jazz = createJazz({
    app,
    permissions,
    store: env.TEST_CORE_URL
      ? env.TEST_REMOTE_APP_ID
        ? remote({ url: env.TEST_CORE_URL, appId: env.TEST_REMOTE_APP_ID, secret })
        : durableObjects({ url: env.TEST_CORE_URL, secret })
      : durableObjects({ binding: env.JAZZ, secret }),
    authenticate: (request) => (env.TEST_REFUSE_ALL ? null : acceptDeviceAccounts(request)),
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
