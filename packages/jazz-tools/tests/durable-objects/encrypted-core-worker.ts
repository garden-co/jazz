// An encrypted app on Cloudflare as the docs show it: `jazzWorker`, its core
// in a Durable Object (`durableObjects({ binding })`), and a keyless backend.
import { acceptDeviceAccounts, createJazz, durableObjects } from "jazz-tools/backend";
import { jazzWorker, type JazzWorkerEnv } from "jazz-tools/cloudflare";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";

/** The code a refused call fails with, or `null` if it did not fail. */
async function refusal(call: () => unknown): Promise<string | null> {
  try {
    await call();
    return null;
  } catch (error) {
    return (error as { code?: string }).code ?? (error as Error).message;
  }
}

const worker = jazzWorker((env: JazzWorkerEnv) => {
  const jazz = createJazz({
    app,
    permissions: {
      ...permissions,
      tables: {
        ...permissions.tables,
        // Anyone may read tasks, so a removed member is still sent the
        // ciphertext of later writes: only the rotated key keeps them out.
        tasks: { ...permissions.tables.tasks, read: "true" },
      },
    },
    store: durableObjects({ binding: env.JAZZ }),
    authenticate: (request) => acceptDeviceAccounts(request),
  });
  return {
    jazz,
    async fetch(request: Request) {
      const url = new URL(request.url);
      try {
        if (url.pathname === "/app-id") return new Response((await jazz.ready()).appId);
        if (url.pathname === "/tasks" && request.method === "POST") {
          await jazz.db
            .insert(app.tasks, { projectId: await request.text(), title: "backend write" })
            .wait({ tier: "global" });
          return new Response("written");
        }
        if (url.pathname === "/tasks") return Response.json(await jazz.db.all(app.tasks));
        if (url.pathname === "/keyless") {
          const space = { table: "projects", id: url.searchParams.get("project")! };
          return Response.json({
            devices: await refusal(() => jazz.db.encryption.devices.list()),
            rotate: await refusal(() => jazz.db.encryption.keys.rotate(space)),
          });
        }
      } catch (error) {
        return new Response((error as Error).message, { status: 500 });
      }
      return new Response("Not found", { status: 404 });
    },
  };
});

export const { JazzCore, JazzBackend } = worker;
export default worker;
