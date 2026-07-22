import { serve } from "@hono/node-server";
import { createServer } from "./app.js";

const port = Number.parseInt(process.env.PORT ?? "3001", 10);
const server = createServer({ staticRoot: "./dist/client" });

serve({ fetch: server.fetch, hostname: "127.0.0.1", port }, ({ address, port }) => {
  console.log(`Bluesky BFF listening on http://${address}:${port}`);
});
