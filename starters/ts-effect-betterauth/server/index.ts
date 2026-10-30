import { serve } from "@hono/node-server";
import { app } from "./app";
import { closeJazzApi } from "./jazz-api";

// Production: serve the built client from ./dist plus the API routes.
const PORT = Number(process.env.PORT ?? 3001);
const server = serve({ fetch: app.fetch, port: PORT }, (info) => {
  console.log(`Listening on http://127.0.0.1:${info.port}`);
});

process.once("SIGTERM", () => {
  server.close(() => void closeJazzApi().finally(() => process.exit(0)));
});
