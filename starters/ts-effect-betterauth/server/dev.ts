import { createServer } from "node:http";
import { getRequestListener } from "@hono/node-server";
import { createServer as createViteServer } from "vite";

// Development: one process on one origin. Vite serves the client with HMR and
// its `jazzPlugin` starts a local Jazz server (or uses Jazz Cloud from .env),
// then the API routes are loaded with that server's settings in process.env.
const PORT = Number(process.env.PORT ?? 3001);
const httpServer = createServer();
const vite = await createViteServer({
  appType: "spa",
  server: { middlewareMode: true, hmr: { server: httpServer } },
});
const { app } = await import("./app");
const api = getRequestListener(app.fetch);

httpServer.on("request", (req, res) => {
  const path = req.url ?? "/";
  if (path === "/health" || path.startsWith("/api/")) {
    void api(req, res);
  } else {
    vite.middlewares(req, res);
  }
});
httpServer.listen(PORT, () => {
  console.log(`Dev server on http://localhost:${PORT}`);
});
