// A Worker backend bound to a separately hosted core that supports encrypted apps.
import {
  acceptDeviceAccounts,
  createJazz,
  jazzBackend,
  remote,
  routeToJazzBackend,
} from "jazz-tools/backend";
import { JazzClient } from "../../src/runtime/client.js";
import { translateQuery } from "../../src/runtime/query-adapter.js";
import { transformRow } from "../../src/runtime/row-transformer.js";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";

interface Env {
  CORE_URL: string;
  APP_ID: string;
  CORE_SECRET: string;
}

// The thin SDK checks encrypted writes before sending them. Check the serving
// client's own hooks inside workerd, on the first real thin transaction.
async function checkThinClient(client: JazzClient) {
  const schema = client.getSchema();
  const query = translateQuery(app.tasks._build(), schema);
  const [row] = await client.query(query, { tier: "remote" });
  const task = transformRow<Record<string, unknown>>(row, schema, "tasks");
  let refusal: string | undefined;
  try {
    await client
      .update("tasks", row.id, { title: { type: "Bytea", value: new Uint8Array() } })
      .wait({ tier: "global" });
  } catch (error) {
    refusal = (error as Error).message;
  }
  return { id: row.id, hasTitle: Object.hasOwn(task, "title"), refusal };
}

let thinCheck: Awaited<ReturnType<typeof checkThinClient>> | undefined;
const serve = JazzClient.prototype.serveThinTransaction;
JazzClient.prototype.serveThinTransaction = async function (request) {
  JazzClient.prototype.serveThinTransaction = serve;
  thinCheck = await checkThinClient(this);
  return serve.call(this, request);
};

export const JazzBackend = jazzBackend((env: Env) => {
  const jazz = createJazz({
    app,
    permissions: {
      ...permissions,
      tables: {
        ...permissions.tables,
        tasks: { ...permissions.tables.tasks, update: "allowedTo.read(project)" },
      },
    },
    authenticate: acceptDeviceAccounts,
    store: remote({ url: env.CORE_URL, appId: env.APP_ID, secret: env.CORE_SECRET }),
  });
  return {
    jazz,
    async fetch(request: Request) {
      try {
        await jazz.ready();
        const path = new URL(request.url).pathname;
        if (path === "/thin-check") return Response.json(thinCheck ?? null);
        if (path === "/tasks") {
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
