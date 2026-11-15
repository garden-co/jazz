// A createJazz backend for the notes app, for tests/end_user.rs.
//
//   node server.mjs <jazz-tools package dir>
//
// Prints `serving {"appId", "url"}` on a line once it serves, and exits when its
// stdin closes (the test process ended).
import { createServer } from "node:http";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const tools = process.argv[2];
if (!tools) throw new Error("usage: node server.mjs <jazz-tools package dir>");
const load = (entry) => import(pathToFileURL(join(tools, "dist", entry)).href);
const { schema: s } = await load("index.js");
const { createJazz, memory } = await load("backend/index.js");

// What jazz.app.json and jazz.permissions.json describe.
const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ $createdBy: session.user });
  policy.notes.allowInsert.always();
});

// Stands in for an app's own auth (an identity provider's access token): an
// opaque bearer "user:<id>", or "user:<id>:<ttl>:<issued at>" for one that
// expires `ttl` seconds after `issued at` (Unix seconds).
function authenticate(request) {
  const header = request.headers.get("authorization") ?? "";
  const match = /^Bearer user:([a-z0-9-]+)(?::(\d+):(\d+))?$/.exec(header);
  if (!match) return null;
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = match[2] ? Number(match[3]) + Number(match[2]) : now + 3600;
  if (expiresAt <= now) return null;
  return { userId: match[1], expiresAt };
}

const jazz = await createJazz({ app, permissions, store: memory(), authenticate }).ready();
const server = createServer((req, res) => jazz.nodeHandler(req, res));
await jazz.attach(server);
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const { port } = server.address();
console.log(
  "serving " + JSON.stringify({ appId: jazz.appId, url: `http://127.0.0.1:${port}/jazz` }),
);

process.stdin.resume();
process.stdin.on("end", async () => {
  server.closeAllConnections();
  server.close();
  await jazz.close();
  process.exit(0);
});
