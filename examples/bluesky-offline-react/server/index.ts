import { serve } from "@hono/node-server";
import { server } from "./app.js";

const port = Number(process.env.PORT ?? 3001);
serve({ fetch: server.fetch, hostname: "127.0.0.1", port });
console.log(`Bluesky BFF listening on http://127.0.0.1:${port}`);
