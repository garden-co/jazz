// The stress app's backend: Jazz runs inside the Vite server (dev and
// preview) on the page's own origin under /jazz, and every browser is a
// local-first guest. A core admits only its app's backend, so the page never
// connects to a sync server itself.
import type { Server } from "node:http";
import type { Plugin, PreviewServer, ViteDevServer } from "vite";
import { acceptDeviceAccounts, createJazz, memory, sqlite } from "jazz-tools/backend";
import { app } from "./schema.js";
import permissions from "./permissions.js";

/**
 * Serve Jazz from the Vite server. Data stays in `dataDir`, or in memory.
 */
export function jazzBackend(options: { dataDir?: string } = {}): Plugin {
  async function mount(server: ViteDevServer | PreviewServer) {
    const jazz = createJazz({
      app,
      permissions,
      // No sign-in: every browser is a guest, so let guests sync.
      authenticate: acceptDeviceAccounts,
      store: options.dataDir ? sqlite({ path: options.dataDir }) : memory(),
    });
    server.middlewares.use((req, res, next) => jazz.nodeHandler(req, res, next));
    const http = server.httpServer as Server | null;
    if (http) {
      await jazz.attach(http);
      http.once("close", () => void jazz.close());
    }
  }
  return { name: "jazz-backend", configureServer: mount, configurePreviewServer: mount };
}
