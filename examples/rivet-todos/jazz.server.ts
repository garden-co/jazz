import type { Server } from "node:http";
import type { Plugin, PreviewServer, ViteDevServer } from "vite";
import { acceptLocalFirstAuth, createJazz } from "jazz-tools/backend";
import { app } from "./schema.js";
import permissions from "./permissions.js";
import { databaseStore } from "./rivet-store.js";

export function jazzBackend(env: Record<string, string>): Plugin {
  async function mount(server: ViteDevServer | PreviewServer) {
    const jazz = createJazz({
      app,
      permissions,
      authenticate: acceptLocalFirstAuth,
      store: await databaseStore(env),
    });
    // This provisions/wakes the actor and publishes the app's schema and policies.
    // User authentication and the browser-facing routes stay in this TS backend.
    await jazz.ready();
    server.middlewares.use((req, res, next) => jazz.nodeHandler(req, res, next));
    const http = server.httpServer as Server | null;
    if (http) {
      await jazz.attach(http);
      http.once("close", () => void jazz.close());
    }
  }
  return { name: "jazz-rivet-backend", configureServer: mount, configurePreviewServer: mount };
}
