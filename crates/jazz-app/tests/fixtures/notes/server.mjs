import { createServer } from "node:http";
import { createJazz, memory } from "jazz-tools/backend";
import { app } from "./schema.ts";
import permissions from "./permissions.ts";

// Stand-in for an app's own auth (e.g. an identity provider's access token): an opaque
// bearer "user:<id>[:<ttl seconds>:<issued at, unix seconds>]".
function authenticate(request) {
  const header = request.headers.get("authorization") ?? "";
  const m = /^Bearer user:([a-z0-9-]+)(?::(\d+):(\d+))?$/.exec(header);
  const path = new URL(request.url).pathname;
  if (!m) { console.error(`authenticate: none ${request.method} ${path}`); return null; }
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = m[2] ? Number(m[3]) + Number(m[2]) : now + 3600;
  if (expiresAt <= now) { console.error(`authenticate: expired ${m[1]} ${path}`); return null; }
  console.error(`authenticate ${m[1]} ${request.method} ${path} (${expiresAt - now}s left)`);
  return { userId: m[1], expiresAt };
}

const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
const server = createServer((req, res) => jazz.nodeHandler(req, res));
await jazz.attach(server);
await new Promise((r) => server.listen(Number(process.env.PORT ?? 4555), "127.0.0.1", r));
console.log(JSON.stringify({ appId: jazz.appId, url: `http://127.0.0.1:${server.address().port}/jazz` }));
