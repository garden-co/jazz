// A local Jazz sync server for one example app:
//
//   node sync.mjs <appDir> <port> <json options>
//
// Production builds don't start one, so this starts JazzServer (jazz-napi, via jazz-tools/dev) on
// 127.0.0.1:<port> with a fresh data dir and the app's own server options,
// publishes the app's jazz.app.json and jazz.permissions.json from <appDir>
// (as its createJazz would), prints "SYNC READY <url>", and runs until it is
// sent SIGTERM.
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [appDir, port, json] = process.argv.slice(2);
const options = JSON.parse(json);
// Resolve jazz-tools the way the app does.
const require = createRequire(join(appDir, "package.json"));
const devEntry = require.resolve("jazz-tools/dev");
const { startLocalJazzServer, deploy } = await import(pathToFileURL(devEntry).href);
const { testApp } = await import(pathToFileURL(require.resolve("jazz-tools/testing")).href);

// The app's files, where `jazz-tools generate` looks for them.
const filesDir = [".", "src", join("src", "lib")]
  .map((root) => join(appDir, root))
  .find((dir) => existsSync(join(dir, "jazz.app.json")));
if (!filesDir) throw new Error(`no jazz.app.json in ${appDir} (looked in ., src, src/lib)`);
const app = testApp(await readFile(join(filesDir, "jazz.app.json"), "utf8"));
const permissions = JSON.parse(await readFile(join(filesDir, "jazz.permissions.json"), "utf8"));

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
  app,
  permissions,
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
