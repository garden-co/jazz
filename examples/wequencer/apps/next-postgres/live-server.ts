// The live WebSocket server, a small process next to `next start` (Next's
// route handlers can't hold WebSockets). The browser connects to it on the
// app's host at its own port, so it gets Better Auth's session cookie.
import { createServer } from "node:http";
import { attachLive } from "@/lib/live.server";

const port = Number(process.env.LIVE_PORT ?? 3001);
const hostname = process.env.HOST ?? "127.0.0.1";
const server = createServer((_req, res) => res.writeHead(426).end("WebSocket only"));
await attachLive(server);
server.listen(port, hostname, () => console.log(`Wequencer live on ws://${hostname}:${port}`));
