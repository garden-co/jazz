import { serve } from "@hono/node-server";
import { server } from "./app.js";

serve(
  { fetch: server.fetch, hostname: "127.0.0.1", port: 3001 },
  ({ address, port }) => {
    console.log(`Bluesky BFF listening on http://${address}:${port}`);
  },
);
