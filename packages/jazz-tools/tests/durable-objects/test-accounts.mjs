// Real packaged DO host: account HTTP decisions, signed guest admission and revocation.
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const require = createRequire(new URL("../../package.json", import.meta.url));
const { mintLocalFirstToken } = require("jazz-napi");
const { createJazz, remote } = await tsImport("../../src/backend/index.ts", import.meta.url);
const { schema: s } = await tsImport("../../src/index.ts", import.meta.url);
const wire = await tsImport("../../src/runtime/native-runtime/websocket.ts", import.meta.url);
const { canonicalAuthorSubject } = await tsImport(
  "../../src/runtime/author-id.ts",
  import.meta.url,
);
const { localFirstAccountId } = await tsImport(
  "../../src/accounts/local-first.ts",
  import.meta.url,
);
process.env.JAZZ_DO_PACKAGED = "1";
const appId = randomUUID(),
  secret = randomBytes(32).toString("hex");
const persist = await mkdtemp(join(tmpdir(), "jazz-do-accounts-"));
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
let mf, jazz, core, url;
const sockets = [];
const deadline = setTimeout(() => {
  console.error("account integration timed out");
  process.exit(1);
}, 90_000);
async function start() {
  mf = createServerHarness(appId, secret, { durableObjectsPersist: persist });
  url = String(await mf.ready).replace(/\/$/, "");
  jazz = await createJazz({
    app,
    permissions,
    store: remote({ url, appId, secret }),
    authenticate: async () => ({ userId: "alice" }),
  }).ready();
  core = await remote({ url, appId, secret }).open();
}
async function post(operation, body, access = "admin") {
  const reply = await core.handle(
    "POST",
    `/apps/${appId}/admin/accounts/${operation}`,
    [["content-type", "application/json"]],
    new TextEncoder().encode(JSON.stringify(body)),
    access,
  );
  const text = new TextDecoder().decode(reply.body);
  return { status: reply.status, body: text.startsWith("{") ? JSON.parse(text) : text };
}
const external = (subject) => ({ issuer: "https://issuer.example", subject });
try {
  await start();
  // Public createJazz entry point must now reach the registry rather than 404.
  const created = await jazz.createAccountFor("alice");
  assert.equal(await jazz.accountFor(new Request("https://app.example/test")), created);
  assert.equal((await jazz.linkIdentity({ from: "alice", to: "linked-alice" })).account, created);
  await jazz.db.insert(app.notes, { title: "backend-only" }).wait({ tier: "global" });
  const userDb = await jazz.forReqUser(new Request("https://app.example/test"));
  assert.deepEqual(await userDb.all(app.notes), []);
  await assert.rejects(() =>
    userDb.insert(app.notes, { title: "forbidden" }).wait({ tier: "global" }),
  );
  // The backend client is thin; authority and attribution survive that change.
  assert.equal(jazz.db.currentRuntime().constructor.name, "ThinRuntime");
  // A backend thin transaction commits atomically on the DO authority.
  const receipt = await jazz.db.transaction((tx) => {
    tx.insert(app.notes, { title: "backend transaction" });
  });
  await receipt.wait({ tier: "global" });
  assert.equal((await jazz.db.all(app.notes.where({ title: "backend transaction" }))).length, 1);
  const attributed = await jazz.withAttribution(new Request("https://app.example/test"));
  const deliveries = [];
  const userDeliveries = [];
  const stopUser = userDb.subscribe(app.notes, (rows) => userDeliveries.push(rows));
  const stop = jazz.db.subscribe(app.notes, (rows) => deliveries.push(rows));
  try {
    const row = await attributed
      .insert(app.notes, { title: "attributed" })
      .wait({ tier: "global" });
    const [stored] = await jazz.db.all(app.notes.where({ id: row.id }).select("$createdBy"));
    assert.equal(stored.$createdBy.account, created);
    // Attribution does not turn a backend read into a user-scoped read.
    assert.equal((await attributed.all(app.notes.where({ id: row.id }))).length, 1);
    assert.deepEqual(await userDb.all(app.notes), []);
    const until = Date.now() + 5000;
    while (
      !userDeliveries.length ||
      !deliveries.some((rows) => rows.some((candidate) => candidate.id === row.id))
    ) {
      assert.ok(Date.now() < until, "thin backend subscription did not receive the write");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.ok(userDeliveries.every((rows) => rows.length === 0));
  } finally {
    stop();
    stopUser();
  }
  const alice = external("alice"),
    bob = external("bob"),
    mallory = external("mallory");
  assert.equal((await post("resolve", { identity: alice }, "backend")).status, 404);
  const concurrent = await Promise.all(
    Array.from({ length: 4 }, () => post("login-or-register", { identity: alice }, "backend")),
  );
  for (const r of concurrent) assert.equal(r.status, 200, JSON.stringify(r));
  const account = concurrent[0].body.account;
  for (const r of concurrent) assert.equal(r.body.account, account);
  assert.equal((await post("login-or-register", { identity: mallory }, "session")).status, 401);
  assert.equal((await post("link", { from: alice, to: bob }, "backend")).status, 401);
  assert.equal((await post("revoke", { approver: alice, target: alice }, "backend")).status, 401);
  assert.equal(
    (await post("login-or-register", { identity: external("bad"), extra: true })).status,
    400,
  );
  const nonce = randomUUID();
  assert.equal((await post("link", { from: alice, to: bob, nonce })).body.account, account);
  assert.equal((await post("link", { from: alice, to: mallory, nonce })).status, 400);
  assert.equal((await post("resolve", { identity: mallory })).status, 404);
  assert.equal((await post("login-or-register", { identity: mallory })).status, 200);
  assert.equal((await post("revoke", { approver: mallory, target: bob })).status, 403);

  const seed = randomBytes(32).toString("base64url");
  const token = mintLocalFirstToken(seed, appId, 300);
  const claims = JSON.parse(Buffer.from(token.split(".")[1], "base64url"));
  const guest = { issuer: claims.iss, subject: claims.sub };
  const guestAccount = localFirstAccountId(appId, guest.subject);
  const peer = canonicalAuthorSubject(guest.issuer, guest.subject, guestAccount);
  const bundles = await core.handle(
    "GET",
    `/apps/${appId}/admin/bundles`,
    [],
    new Uint8Array(),
    "admin",
  );
  const bundle = JSON.parse(new TextDecoder().decode(bundles.body)).bundles[0].bundle;
  function connect(
    auth = { jwt_token: token },
    identity = peer,
    access = "session",
    extra = {},
    features,
  ) {
    const frames = [];
    let socket, resolveClose, resolveRefusal;
    const refusal = new Promise((resolve) => {
      resolveRefusal = resolve;
    });
    const closed = new Promise((resolve) => {
      resolveClose = resolve;
    });
    const hello = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("guest handshake timed out")), 5000);
      socket = core.openSocket(
        [],
        (error, event) => {
          if (error) {
            clearTimeout(timer);
            reject(error);
            resolveClose();
          } else if (event.kind === "message") {
            clearTimeout(timer);
            const frame = wire.decodeWebSocketFrameBatch(event.data)[0];
            frames.push(frame);
            if (wire.isWireError(frame)) resolveRefusal(wire.decodeWireError(frame));
            resolve(frame);
          } else if (event.kind === "close") {
            clearTimeout(timer);
            resolveClose();
            reject(new Error("closed before hello"));
          }
        },
        access,
      );
      socket.send(
        new TextEncoder().encode(
          JSON.stringify({ peer_identity: identity, auth, bundle, ...extra }),
        ),
      );
      socket.send(wire.encodeWebSocketFrameBatch([wire.encodeWireClientHello(features)]));
    });
    sockets.push(socket);
    return {
      hello,
      closed,
      refusal,
      frames,
      send: (frame) => socket.send(wire.encodeWebSocketFrameBatch([frame])),
    };
  }
  const thinWire = await tsImport("../../src/thin/wire.ts", import.meta.url);
  const publicThin = connect(
    { jwt_token: token },
    peer,
    "session",
    {},
    thinWire.FEATURE_THIN_RESULTS,
  );
  assert.ok(wire.isWireHello(await publicThin.hello));
  publicThin.send(thinWire.encodeThinRequest(7, { type: "captureSnapshot", backendContext: "{}" }));
  const until = Date.now() + 5000;
  while (publicThin.frames.length < 2) {
    assert.ok(Date.now() < until, "public thin scope refusal timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  const refusedScope = thinWire.decodeThinResponse(publicThin.frames[1]);
  assert.equal(refusedScope.body.type, "error");
  assert.match(refusedScope.body.message, /authenticated SYSTEM/);
  const live = connect();
  const hello = await live.hello;
  assert.ok(
    wire.isWireHello(hello),
    wire.isWireError(hello) ? JSON.stringify(wire.decodeWireError(hello)) : "not hello",
  );
  assert.equal((await post("resolve", { identity: guest })).body.account, guestAccount);
  assert.ok(
    wire.isWireError(
      await connect({ jwt_token: mintLocalFirstToken(seed, randomUUID(), 300) }).hello,
    ),
  );
  assert.ok(
    wire.isWireError(
      await connect(
        { jwt_token: token },
        canonicalAuthorSubject(guest.issuer, guest.subject, randomUUID()),
      ).hello,
    ),
  );
  assert.ok(wire.isWireError(await connect({ admin_secret: secret }).hello));
  assert.ok(
    wire.isWireError(
      await connect({ jwt_token: token }, peer, "session", { local_first_audience: appId }).hello,
    ),
  );
  const parts = token.split(".");
  const tampered = { ...claims, exp: claims.exp + 1 };
  const forged = `${parts[0]}.${Buffer.from(JSON.stringify(tampered)).toString("base64url")}.${parts[2]}`;
  assert.ok(wire.isWireError(await connect({ jwt_token: forged }).hello));
  const signedIn = external("signed-in");
  assert.equal((await post("link", { from: guest, to: signedIn })).body.account, guestAccount);
  assert.equal((await post("revoke", { approver: signedIn, target: guest })).status, 204);
  await Promise.race([
    live.closed,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("revoked socket remained open")), 5000),
    ),
  ]);
  assert.match(JSON.stringify(await live.refusal), /account identity revoked/);
  assert.ok(wire.isWireError(await connect().hello));
  assert.equal((await post("found-local-first", { identity: guest })).status, 403);
  for (const socket of sockets) socket.close();
  await core.close();
  await jazz.close();
  jazz = undefined;
  await mf.dispose();
  await start();
  assert.equal((await post("resolve", { identity: bob })).body.account, account);
  assert.equal((await post("resolve", { identity: signedIn })).body.account, guestAccount);
  assert.equal((await post("resolve", { identity: guest })).status, 403);
  assert.equal((await post("link", { from: alice, to: mallory, nonce })).status, 400);
  console.log(
    "Accounts: createJazz enrollment, atomic registration, link/revoke permissions, guest proof admission, active revocation and SQLite restart passed.",
  );
} finally {
  clearTimeout(deadline);
  for (const socket of sockets) socket.close();
  await core?.close();
  await jazz?.close();
  await mf?.dispose();
  await rm(persist, { recursive: true, force: true });
}
