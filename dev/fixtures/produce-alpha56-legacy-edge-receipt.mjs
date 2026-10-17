// Explicit maintainer action only. Never invoke this producer in CI.
// Arguments: an npm prefix whose node_modules holds the published, integrity-
// verified jazz-tools, jazz-napi (2.0.0-alpha.56) and undici packages, and a
// fresh output directory. No workspace Jazz code is imported: every runtime,
// server and codec used here is the distributed 2.0.0-alpha.56 package.
//
// Topology: Core server <- TCP proxy <- Edge server <- persistent NAPI client.
// A first write reaches Core (control: Accepted/Global). For the second write
// the proxy stops forwarding edge -> Core bytes the moment the client observes
// the edge acknowledgement, so the client durably stores the retired edge
// receipt (fate Accepted, durability tag 2) with no global time. The client's
// RocksDB directory is the fixture.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { createConnection, createServer } from "node:net";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const [prefix, outputPath] = process.argv.slice(2);
if (!outputPath)
  throw new Error("usage: node produce-alpha56-legacy-edge-receipt.mjs NPM_PREFIX NEW_OUTPUT");
const modules = resolve(prefix, "node_modules");
for (const name of ["jazz-tools", "jazz-napi"]) {
  const pkg = JSON.parse(await readFile(resolve(modules, name, "package.json"), "utf8"));
  if (pkg.version !== "2.0.0-alpha.56") throw new Error(`${name} must be 2.0.0-alpha.56`);
}
const load = (file) => import(pathToFileURL(resolve(modules, "jazz-tools/dist", file)).href);
const { schema: s } = await load("schema-namespace.js");
const { startLocalJazzServer } = await load("testing/index.js");
const { JazzClient } = await load("runtime/client.js");
const { canonicalAuthorSubject } = await load("runtime/author-id.js");
const { localFirstAccountId } = await load("accounts/local-first.js");
const { NativeRuntimeAdapter } = await load("runtime/native-runtime/native-runtime-adapter.js");
const require = createRequire(resolve(modules, "jazz-napi/package.json"));
const { NapiDb, mintLocalFirstToken, verifyLocalFirstIdentityProof } = require("jazz-napi");
const { WebSocket } = createRequire(resolve(modules, "undici/package.json"))("undici");
globalThis.WebSocket ??= WebSocket;

await mkdir(outputPath); // Never reuse an existing output.
const within = (promise, what) =>
  Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`timed out: ${what}`)), 20_000)),
  ]);

const appId = "00000000-0000-0000-0000-0000000a1056";
const schema = {
  todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
};
const grants = s.definePermissions(s.defineApp({ fixture: s.table({}, {}) }), ({ policy }) => {
  policy.fixture.allowRead.always();
  policy.fixture.allowInsert.always();
  policy.fixture.allowUpdate.always();
  policy.fixture.allowDelete.always();
}).fixture;
const common = {
  appId,
  inMemory: true,
  adminSecret: "alpha56-fixture-admin",
  backendSecret: "alpha56-fixture-backend",
  allowLocalFirstAuth: true,
};
const core = await startLocalJazzServer({ ...common, schema, permissions: { todos: grants } });

let frozen = false;
const sockets = new Set();
const proxy = createServer((edgeSide) => {
  const coreSide = createConnection(core.port, "127.0.0.1");
  sockets.add(edgeSide).add(coreSide);
  edgeSide.on("data", (chunk) => frozen || coreSide.write(chunk));
  coreSide.on("data", (chunk) => edgeSide.write(chunk));
  edgeSide.on("error", () => {});
  coreSide.on("error", () => {});
});
await new Promise((r) => proxy.listen(0, "127.0.0.1", r));
const upstream = new URL(core.url);
upstream.port = String(proxy.address().port);
const edge = await startLocalJazzServer({
  ...common,
  upstreamUrl: upstream.href.replace(/\/$/, ""),
});
const deadline = Date.now() + 20_000;
while ((await fetch(`${edge.url}/health`)).status !== 200) {
  if (Date.now() > deadline) throw new Error("edge never became ready");
  await new Promise((r) => setTimeout(r, 50));
}

// A fixed local-first seed gives the fixture one stable, verifiable author.
const token = mintLocalFirstToken(Buffer.alloc(32, 0xa6).toString("base64url"), appId, 3600);
const verified = verifyLocalFirstIdentityProof(token, appId);
if (!verified.ok) throw new Error(`local-first proof rejected: ${verified.error}`);
const account = localFirstAccountId(appId, verified.id);
const claimedAuthor = canonicalAuthorSubject("urn:jazz:local-first", verified.id, account);
const node = new Uint8Array(16).fill(0x56);
const database = resolve(outputPath, "rocksdb-alpha56-client");
const runtime = new NativeRuntimeAdapter(
  NapiDb,
  schema,
  node,
  new TextEncoder().encode(claimedAuthor),
  1,
  false,
  { persistentPath: database, selfSignedClientProof: { token, appId, claimedAuthor } },
);
const client = JazzClient.connectWithRuntime(runtime, { appId, schema, serverUrl: edge.url });
client.connectTransport(edge.url, { jwt_token: token });

const confirmedRowId = "a6a6a6a6-0000-4000-8000-0000000c0056";
const confirmed = client.insert(
  "todos",
  { title: { type: "Text", value: "published alpha.56 core-confirmed write" } },
  { id: confirmedRowId },
);
await within(confirmed.wait({ tier: "global" }), "control write reaching Core");

const edgeRowId = "a6a6a6a6-0000-4000-8000-0000000e0056";
const edgeWrite = client.insert(
  "todos",
  { title: { type: "Text", value: "published alpha.56 edge-accepted write" } },
  { id: edgeRowId },
);
// The edge admits a write after several Core permission-scope round trips,
// then forwards the admitted commit to Core tens of milliseconds after its
// acknowledgement. Freezing edge -> Core at the acknowledgement means the
// commit never reaches Core, so no global time can ever come back.
await within(
  edgeWrite.wait({ tier: "edge" }).then(() => (frozen = true)),
  "edge acknowledgement",
);
const reachedGlobal = await Promise.race([
  edgeWrite.wait({ tier: "global" }).then(
    () => true,
    () => false,
  ),
  new Promise((r) => setTimeout(() => r(false), 2_000)),
]);
if (reachedGlobal) throw new Error("edge write unexpectedly reached Core; rerun the producer");

const receipt = {
  producer: "dev/fixtures/produce-alpha56-legacy-edge-receipt.mjs",
  packages: { "jazz-tools": "2.0.0-alpha.56", "jazz-napi": "2.0.0-alpha.56" },
  appId,
  node: "56".repeat(16),
  author: claimedAuthor,
  confirmed: { rowId: confirmedRowId, txId: String(await confirmed.txId) },
  edgeAccepted: { rowId: edgeRowId, txId: String(await edgeWrite.txId) },
};
await client.shutdown().catch((error) => {
  // Shutdown reports the edge write as not globally observed; that is the point.
  if (!String(error).includes("reached Global")) throw error;
});
for (const socket of sockets) socket.destroy();
proxy.close();
await edge.stop();
await core.stop();
await writeFile(resolve(outputPath, "receipt.json"), JSON.stringify(receipt, null, 2) + "\n", {
  flag: "wx",
});
console.log("Published alpha.56 legacy edge receipt producer completed");
process.exit(0);
