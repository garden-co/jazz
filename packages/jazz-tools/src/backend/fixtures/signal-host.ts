// A backend process for the shutdown-signal test: it serves Jazz on a plain
// Node server and prints `<port> <appId>` once open. With `close-server`, it
// stops on SIGTERM the way `next start` does: it closes its server and exits
// once every connection has ended.
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { schema as s } from "../../index.js";
import { createJazz, memory } from "../index.js";

const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.never();
});
const jazz = createJazz({ app, permissions, store: memory(), authenticate: () => null });
const server = createServer((req, res) => jazz.nodeHandler(req, res));
await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
await jazz.ready();
if (process.argv[2] === "close-server") {
  process.on("SIGTERM", () => server.close(() => process.exit(0)));
}
console.log(`${(server.address() as AddressInfo).port} ${jazz.appId}`);
