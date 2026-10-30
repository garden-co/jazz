// A local Jazz sync server for one example app:
//
//   node sync.mjs <appDir> <port> <json options>
//
// Production builds don't start one (withJazz / jazzPlugin only do in
// development), so this starts JazzServer (jazz-napi, via jazz-tools/dev) on
// 127.0.0.1:<port> with a fresh data dir and the app's own server options,
// publishes the app's schema and permissions from <appDir> with the same
// deploy call the dev runtime makes, prints "SYNC READY <url>", and runs until
// it is sent SIGTERM.
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";

const [appDir, port, json] = process.argv.slice(2);
const options = JSON.parse(json);
// Resolve jazz-tools the way the app does.
const require = createRequire(join(appDir, "package.json"));
const devEntry = require.resolve("jazz-tools/dev");
const { startLocalJazzServer } = await import(pathToFileURL(devEntry).href);
const { deploy } = await import(
  pathToFileURL(join(dirname(devEntry), "catalogue-project.js")).href
);

const dataDir = await mkdtemp(join(tmpdir(), "load-times-sync-"));
const server = await startLocalJazzServer({
  host: "127.0.0.1",
  port: Number(port),
  dataDir,
  ...options,
});
await deploy({
  serverUrl: server.url,
  appId: server.appId,
  adminSecret: server.adminSecret,
  schemaDir: appDir,
});
console.log(`SYNC READY ${server.url} app=${server.appId}`);

const shutdown = async () => {
  await server.stop().catch(() => {});
  await rm(dataDir, { recursive: true, force: true }).catch(() => {});
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
setInterval(() => {}, 1 << 30);
