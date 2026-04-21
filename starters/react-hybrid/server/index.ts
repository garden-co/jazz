import { serve } from "@hono/node-server";
import { app } from "./app.js";

const PORT = Number(process.env.PORT ?? 3001);
serve({ fetch: app.fetch, port: PORT }, (info) => {
  console.log(`Hono listening on http://127.0.0.1:${info.port}`);
});
