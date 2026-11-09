import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const { createJazz, remote } = await tsImport(
  "../../../../packages/jazz-tools/src/backend/index.ts",
  import.meta.url,
);
const { schema: s } = await tsImport(
  "../../../../packages/jazz-tools/src/index.ts",
  import.meta.url,
);
const wire = await tsImport(
  "../../../../packages/jazz-tools/src/runtime/native-runtime/websocket.ts",
  import.meta.url,
);
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
const appId = randomUUID();
const secret = "create-jazz-do-test";
const persist = await mkdtemp(join(tmpdir(), "jazz-do-ready-"));
const deadline = setTimeout(() => {
  console.error("createJazz DO integration timed out");
  process.exit(1);
}, 90000);
let mf, jazz;
try {
  mf = createServerHarness(appId, secret, { durableObjectsPersist: persist });
  let url = String(await mf.ready).replace(/\/$/, "");
  jazz = createJazz({ app, permissions, store: remote({ url, appId, secret }) });
  await jazz.ready();
  console.log("createJazz ready");
  for (const headers of [
    {},
    { "x-jazz-backend-credential": secret },
    { "x-jazz-backend-credential": "wrong", "x-jazz-backend-access": "backend" },
  ]) {
    const refused = await fetch(`${url}/apps/${appId}/backend/admit`, { method: "POST", headers });
    assert.equal(refused.status, 401);
  }
  const core = await remote({ url, appId, secret }).open();
  const inventory = await core.handle(
    "GET",
    `/apps/${appId}/admin/bundles`,
    [],
    new Uint8Array(),
    "admin",
  );
  const bundle = JSON.parse(new TextDecoder().decode(inventory.body)).bundles[0].bundle;
  const auth = {
    backend_secret: "stand-in",
    backend_session: { issuer: "https://issuer.example", user_id: "alice", claims: {} },
  };
  const peer = new TextDecoder().decode(
    wire.peerIdentityForWebSocketAuth(JSON.stringify(auth), new Uint8Array()),
  );
  const prelude = { peer_identity: peer, auth, bundle };
  async function handshake(access, opening) {
    let socket;
    try {
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("handshake probe timed out")), 5000);
        socket = core.openSocket(
          [],
          (error, event) => {
            if (error) {
              clearTimeout(timer);
              reject(error);
            } else if (event.kind === "message") {
              clearTimeout(timer);
              resolve(wire.decodeWebSocketFrameBatch(event.data)[0]);
            } else if (event.kind === "close") {
              clearTimeout(timer);
              reject(new Error(`closed: ${event.reason}`));
            }
          },
          access,
        );
        socket.send(new TextEncoder().encode(JSON.stringify(opening)));
        socket.send(wire.encodeWebSocketFrameBatch([wire.encodeWireClientHello()]));
      });
    } finally {
      socket?.close();
    }
  }
  const admittedUser = await handshake("backend", prelude);
  assert.ok(
    wire.isWireHello(admittedUser),
    wire.isWireError(admittedUser)
      ? JSON.stringify(wire.decodeWireError(admittedUser))
      : "not a hello",
  );
  // Prelude admin_secret must not elevate a session-only upgrade.
  assert.ok(
    wire.isWireError(
      await handshake("session", { ...prelude, auth: { ...auth, admin_secret: secret } }),
    ),
  );
  assert.ok(wire.isWireError(await handshake("backend", { ...prelude, bundle: "0".repeat(64) })));
  assert.ok(wire.isWireError(await handshake("backend", { ...prelude, peer_identity: "forged" })));
  await core.close();
  const row = await jazz.db.insert(app.notes, { title: "persisted" }).wait({ tier: "global" });
  assert.deepEqual(
    (await jazz.db.all(app.notes.where({ id: row.id }))).map((r) => r.title),
    ["persisted"],
  );
  // Keep this Node backend alive: its existing transport must reconnect to
  // the reconstructed object and settle a new write, not only read its cache.
  const port = Number(new URL(url).port);
  await mf.dispose();
  mf = createServerHarness(appId, secret, { durableObjectsPersist: persist, port });
  await mf.ready;
  await jazz.db.insert(app.notes, { title: "live reconnect" }).wait({ tier: "global" });
  await jazz.close();
  jazz = undefined;
  await mf.dispose();
  mf = createServerHarness(appId, secret, { durableObjectsPersist: persist });
  url = String(await mf.ready).replace(/\/$/, "");
  jazz = await createJazz({ app, permissions, store: remote({ url, appId, secret }) }).ready();
  assert.deepEqual(
    (await jazz.db.all(app.notes.where({ id: row.id }))).map((r) => r.title),
    ["persisted"],
  );
  await jazz.db.insert(app.notes, { title: "after restart" }).wait({ tier: "global" });
  assert.equal((await jazz.db.all(app.notes)).length, 3);
  console.log(
    "createJazz(remote): readiness, authenticated backend read/write, and persisted restart passed.",
  );
} finally {
  clearTimeout(deadline);
  await jazz?.close();
  await mf?.dispose();
  await rm(persist, { recursive: true, force: true });
}
