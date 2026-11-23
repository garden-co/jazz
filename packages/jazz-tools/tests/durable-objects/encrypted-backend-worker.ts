// A Worker backend bound to a separately hosted core that supports encrypted apps.
import {
  acceptDeviceAccounts,
  createJazz,
  jazzBackend,
  remote,
  routeToJazzBackend,
} from "jazz-tools/backend";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";

interface Env {
  CORE_URL: string;
  APP_ID: string;
  CORE_SECRET: string;
}

export const JazzBackend = jazzBackend((env: Env) => {
  const jazz = createJazz({
    app,
    permissions,
    authenticate: acceptDeviceAccounts,
    store: remote({ url: env.CORE_URL, appId: env.APP_ID, secret: env.CORE_SECRET }),
  });
  return {
    jazz,
    async fetch(request: Request) {
      try {
        await jazz.ready();
        if (new URL(request.url).pathname === "/tasks") {
          if (request.method === "POST") {
            await jazz.db
              .insert(app.tasks, {
                projectId: await request.text(),
                title: "backend write",
              })
              .wait({ tier: "global" });
          }
          return Response.json(await jazz.db.all(app.tasks));
        }
        return new Response("ready");
      } catch (error) {
        return new Response((error as Error).message, { status: 500 });
      }
    },
  };
});

export default { fetch: routeToJazzBackend };
