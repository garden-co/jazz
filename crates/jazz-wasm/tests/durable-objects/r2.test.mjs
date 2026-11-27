// Large values in R2 (`JAZZ_BLOBS`) from the Jazz core on a Durable Object,
// in local workerd (Miniflare simulates the bucket):
//
//   write a large note ──► its leaves move to jazz/<app id>/<sha256> ──► restart ──► read from R2
//   reclaimed ──► deleted from the bucket
//   a second store on a claimed prefix ──► refused (/ready 503), its values stay in its SQL
//   failed puts ──► retried with backoff until they land
//   a store with inline large values ──► binding added ──► moves them to R2
import { createHash, createHmac } from "node:crypto";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";
import { blobStore } from "../../durable-object/blob-stores.js";
import * as fixture from "./pkg/jazz_wasm.js";

const here = fileURLToPath(new URL(".", import.meta.url));
fixture.initSync({ module: readFileSync(join(here, "pkg/jazz_wasm_bg.wasm")) });

const APP = "my-app";
const SECRET = "s".repeat(40);
const SCHEMA = fixture.testNotesSchema();
const admin = { "X-Jazz-Admin-Secret": SECRET };
const backend = { "X-Jazz-Backend-Credential": SECRET, "X-Jazz-Backend-Access": "backend" };
const prelude = JSON.stringify({ peer_identity: fixture.testSystemIdentity(), auth: {} });

/**
 * A title of `bytes` pseudo-random letters (xorshift32 from `seed`): a large
 * value of several distinct leaves.
 */
function largeTitle(seed, bytes) {
  let state = seed * 2654435761 || 1;
  const letters = Array.from({ length: bytes });
  for (let i = 0; i < bytes; i++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    letters[i] = String.fromCharCode(97 + ((state >>> 0) % 26));
  }
  return letters.join("");
}

/**
 * A Miniflare over `dirs` (`{ objects, bucket }`, persisted): with `blobs`,
 * the R2 binding `JAZZ_BLOBS`; `vars` adds bindings (`JAZZ_BLOBS_PREFIX`,
 * `FAIL_PUTS`).
 */
function start(
  dirs,
  { blobs = true, bindBucket = blobs, vars = {}, script, outboundService } = {},
) {
  return new Miniflare({
    modules: true,
    outboundService,
    scriptPath: join(here, "r2-worker.mjs"),
    ...(script && { script }),
    modulesRoot: join(here, "../.."),
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
    durableObjectsPersist: dirs.objects,
    r2Buckets: bindBucket ? ["JAZZ_BLOBS"] : [],
    r2Persist: dirs.bucket,
    bindings: {
      JAZZ_CORE_SECRET: SECRET,
      JAZZ_SCHEMA: SCHEMA,
      BLOB_MODE: blobs ? "r2" : "sqlite",
      ...vars,
    },
  });
}

function scratch() {
  const root = mkdtempSync(join(tmpdir(), "jazz-do-r2-"));
  return {
    root,
    objects: join(root, "objects"),
    bucket: join(root, "bucket"),
    other: join(root, "other-objects"),
  };
}

test("omitting blobs fails even when an R2 bucket is bound", async () => {
  const dirs = scratch();
  const mf = start(dirs, {
    script: `
    import { createJazzCore, r2 } from "./do/index.js";
    export const JazzCore = createJazzCore({blobs: (env) => r2(env.JAZZ_BLOBS)});
    export default { fetch(request, env) {
      if (!env.JAZZ_BLOBS) throw new Error("test needs an R2 binding");
      try { createJazzCore({}); } catch (error) { return new Response(error.message); }
      return new Response("accepted missing destination");
    } };
  `,
  });
  try {
    assert.match(
      await (await mf.dispatchFetch("http://core/check")).text(),
      /createJazzCore\(\): blobs/,
    );
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

/** A socket to the core, its answers queued as they arrive. */
async function connect(mf) {
  const response = await mf.dispatchFetch(`http://core/apps/${APP}/ws`, {
    headers: { Upgrade: "websocket", ...backend },
  });
  assert.equal(response.status, 101);
  const ws = response.webSocket;
  const answers = [];
  const waiting = [];
  ws.addEventListener("message", (event) => {
    for (const answer of JSON.parse(fixture.testDecodeAnswers(new Uint8Array(event.data)))) {
      (waiting.shift() ?? ((a) => answers.push(a)))(answer);
    }
  });
  ws.accept();
  return {
    send: (message) => ws.send(message),
    next: () =>
      answers.length
        ? Promise.resolve(answers.shift())
        : new Promise((resolve, reject) => {
            waiting.push(resolve);
            setTimeout(() => reject(new Error("no answer within 10 s")), 10_000);
          }),
    close: () => ws.close(),
  };
}

/** A backend socket that said Hello and declared the schema. */
async function session(mf) {
  const socket = await connect(mf);
  socket.send(prelude);
  socket.send(fixture.testThinHello());
  assert.deepEqual(await socket.next(), { hello: true, heartbeat: false });
  socket.send(fixture.testThinSchema(1n, SCHEMA));
  assert.deepEqual(await socket.next(), { id: 1, ok: true });
  return socket;
}

async function write(mf, title) {
  const socket = await session(mf);
  socket.send(fixture.testThinInsertNote(2n, title));
  assert.deepEqual(await socket.next(), { id: 2, written: true });
  socket.close();
}

async function titles(mf) {
  const socket = await session(mf);
  socket.send(fixture.testThinQueryNotes(2n));
  const answer = await socket.next();
  socket.close();
  return answer.titles ?? answer;
}

async function appId(mf) {
  const response = await mf.dispatchFetch("http://core/app", { headers: backend });
  assert.equal(response.status, 200);
  return (await response.json()).appId;
}

/** What the object's SQL still holds of large values (a test-only route). */
async function inline(mf) {
  const response = await mf.dispatchFetch(`http://core/apps/${APP}/test/inline-large-values`, {
    method: "POST",
    headers: admin,
  });
  assert.equal(response.status, 200);
  return response.json();
}

/** Object names under `prefix`, without it. */
async function names(mf, prefix) {
  const bucket = await mf.getR2Bucket("JAZZ_BLOBS");
  const found = [];
  let cursor;
  do {
    const page = await bucket.list({ prefix, cursor });
    found.push(...page.objects.map((object) => object.key.slice(prefix.length)));
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor !== undefined);
  return found.sort();
}

/** `check()` until it is truthy (it is retried while it throws or is falsy). */
async function eventually(check, ms = 20_000) {
  const until = Date.now() + ms;
  for (;;) {
    let last;
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    if (Date.now() > until) throw last ?? new Error("never held");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

const nodeName = /^[0-9a-f]{64}$/;

test("a large value moves to R2 under the store's prefix, checksummed, and is read back from there after a restart", async () => {
  const dirs = scratch();
  const big = largeTitle(1, 300 * 1024);
  let mf = start(dirs);
  try {
    assert.equal((await mf.dispatchFetch("http://core/ready")).status, 200);
    const id = await appId(mf);
    const prefix = `jazz/${id}/`;
    // The store claimed its prefix.
    const bucket = await mf.getR2Bucket("JAZZ_BLOBS");
    assert.equal((await (await bucket.get(`${prefix}.jazz-owner`)).text()).trim(), id);

    await write(mf, big);
    // Its leaves land under the prefix, named by their SHA-256, and leave the SQL.
    const moved = await eventually(async () => {
      const leaves = (await names(mf, prefix)).filter((name) => nodeName.test(name));
      return leaves.length >= 2 && (await inline(mf)).leafBytes === 0 && leaves;
    });
    let total = 0;
    for (const name of moved) {
      const object = await bucket.get(prefix + name);
      const body = new Uint8Array(await object.arrayBuffer());
      total += body.byteLength;
      const digest = Buffer.from(await crypto.subtle.digest("SHA-256", body)).toString("hex");
      assert.equal(digest, name, "an object is named by its bytes' SHA-256");
      // The put carried that checksum, which R2 checked.
      assert.equal(Buffer.from(object.checksums.sha256).toString("hex"), name);
    }
    assert.ok(total >= 300 * 1024, `the leaves hold the value (${total} bytes)`);
    await mf.dispose();

    // A fresh object over the same storage reads it back from the bucket.
    mf = start(dirs);
    assert.equal((await inline(mf)).leafBytes, 0);
    assert.deepEqual(await titles(mf), [big]);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("reclaimed large values are deleted from the bucket", async () => {
  const dirs = scratch();
  const mf = start(dirs);
  try {
    const prefix = `jazz/${await appId(mf)}/`;
    await write(mf, largeTitle(2, 200 * 1024));
    // Only the bucket is watched: the object moves the value on its own.
    await eventually(
      async () => (await names(mf, prefix)).filter((name) => nodeName.test(name)).length >= 2,
    );
    const response = await mf.dispatchFetch(
      `http://core/apps/${APP}/test/unreference-large-values`,
      { method: "POST", headers: admin },
    );
    assert.equal(response.status, 204);
    // Every node is reclaimed, and the bucket keeps only the owner object.
    await eventually(async () => {
      const left = await names(mf, prefix);
      return left.length === 1 && left[0] === ".jazz-owner" && (await inline(mf)).nodes === 0;
    });
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("a second store is refused on a prefix another store claimed, and keeps its values inline", async () => {
  const dirs = scratch();
  const vars = { JAZZ_BLOBS_PREFIX: "shared" };
  let mf = start(dirs, { vars });
  try {
    assert.equal((await mf.dispatchFetch("http://core/ready")).status, 200);
    const first = await appId(mf);
    await mf.dispose();

    // Another object (its own storage, so its own app id), the same bucket and prefix.
    mf = start({ ...dirs, objects: dirs.other }, { vars });
    const second = await appId(mf);
    assert.notEqual(second, first);
    const ready = await mf.dispatchFetch("http://core/ready");
    assert.equal(ready.status, 503);
    const { error } = await ready.json();
    assert.equal(error.code, "large_value_bucket_refused");
    assert.match(
      error.message,
      new RegExp(`shared/ in this bucket belongs to app ${first}, not this one \\(${second}\\)`),
    );

    // It still serves its large values, from its own SQL; nothing of it reaches the bucket.
    const big = largeTitle(3, 200 * 1024);
    await write(mf, big);
    assert.deepEqual(await titles(mf), [big]);
    await new Promise((resolve) => setTimeout(resolve, 1000));
    assert.deepEqual(await names(mf, "shared/"), [".jazz-owner"]);
    assert.ok((await inline(mf)).leafBytes >= 200 * 1024);
    const bucket = await mf.getR2Bucket("JAZZ_BLOBS");
    assert.equal((await (await bucket.get("shared/.jazz-owner")).text()).trim(), first);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("failed puts keep the value in SQL and are retried until they land", async () => {
  const dirs = scratch();
  const mf = start(dirs, { vars: { FAIL_PUTS: "6" } });
  try {
    const prefix = `jazz/${await appId(mf)}/`;
    const big = largeTitle(4, 300 * 1024);
    await write(mf, big);
    // The first batch fails, and reads are served from SQL meanwhile.
    await eventually(async () => (await names(mf, "test-failed-puts/")).length > 0);
    assert.ok((await inline(mf)).leafBytes >= 300 * 1024);
    assert.deepEqual(await titles(mf), [big]);
    // Every injected failure is used up, then the retry lands.
    await eventually(async () => {
      const leaves = (await names(mf, prefix)).filter((name) => nodeName.test(name));
      return leaves.length >= 2 && (await inline(mf)).leafBytes === 0;
    }, 60_000);
    assert.equal((await names(mf, "test-failed-puts/")).length, 6);
    assert.deepEqual(await titles(mf), [big]);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("a store with inline large values moves them to R2 once the binding is selected, and needs it from then on", async () => {
  const dirs = scratch();
  const big = largeTitle(5, 300 * 1024);
  let mf = start(dirs, { blobs: false, bindBucket: true });
  try {
    await write(mf, big);
    const before = await inline(mf);
    assert.ok(before.leafBytes >= 300 * 1024, "without the binding, values stay in SQL");
    await mf.dispose();

    mf = start(dirs);
    const prefix = `jazz/${await appId(mf)}/`;
    // What it held moves out, and is still read.
    await eventually(async () => {
      const leaves = (await names(mf, prefix)).filter((name) => nodeName.test(name));
      return leaves.length >= 2 && (await inline(mf)).leafBytes === 0;
    });
    const second = largeTitle(6, 200 * 1024);
    await write(mf, second);
    await eventually(async () => (await inline(mf)).leafBytes === 0);
    await mf.dispose();

    mf = start(dirs);
    assert.deepEqual(await titles(mf), [big, second].sort());
    await mf.dispose();

    // A store whose values moved can't be opened without the bucket.
    mf = start(dirs, { blobs: false });
    const ready = await mf.dispatchFetch("http://core/ready");
    assert.equal(ready.status, 500);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("a store is refused on another prefix or bucket than the one it claimed, and works again once set back", async () => {
  const dirs = scratch();
  const big = largeTitle(7, 300 * 1024);
  let mf = start(dirs);
  try {
    const id = await appId(mf);
    const prefix = `jazz/${id}/`;
    await write(mf, big);
    await eventually(async () => (await inline(mf)).leafBytes === 0);
    await mf.dispose();

    // Another prefix: its values are under the one it claimed.
    mf = start(dirs, { vars: { JAZZ_BLOBS_PREFIX: "elsewhere" } });
    let ready = await mf.dispatchFetch("http://core/ready");
    assert.equal(ready.status, 503);
    let { error } = await ready.json();
    assert.equal(error.code, "large_value_bucket_refused");
    assert.match(
      error.message,
      new RegExp(`are under ${prefix}, but JAZZ_BLOBS_PREFIX now names elsewhere/`),
    );
    assert.deepEqual(await names(mf, "elsewhere/"), [], "nothing is claimed or written there");
    await mf.dispose();

    // Another bucket: the store's owner object isn't in it.
    mf = start({ ...dirs, bucket: join(dirs.root, "other-bucket") });
    ready = await mf.dispatchFetch("http://core/ready");
    assert.equal(ready.status, 503);
    ({ error } = await ready.json());
    assert.match(error.message, /is missing from the bound bucket, though this store claimed/);
    assert.deepEqual(await names(mf, ""), [], "nothing is claimed or written there");
    await mf.dispose();

    // Its own bucket and prefix again.
    mf = start(dirs);
    assert.equal((await mf.dispatchFetch("http://core/ready")).status, 200);
    assert.deepEqual(await titles(mf), [big]);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("a read whose object is missing or corrupt in the bucket answers an error, and reads again once it is back", async () => {
  const dirs = scratch();
  const big = largeTitle(8, 300 * 1024);
  let mf = start(dirs);
  try {
    const prefix = `jazz/${await appId(mf)}/`;
    await write(mf, big);
    await eventually(async () => (await inline(mf)).leafBytes === 0);
    await mf.dispose();

    mf = start(dirs);
    const bucket = await mf.getR2Bucket("JAZZ_BLOBS");
    const [leaf] = (await names(mf, prefix)).filter((name) => nodeName.test(name));
    const original = new Uint8Array(await (await bucket.get(prefix + leaf)).arrayBuffer());
    // Corrupt: R2 holds other bytes under the leaf's name (written without its checksum).
    await bucket.put(prefix + leaf, "not the leaf");
    const corrupt = await titles(mf);
    assert.ok(
      corrupt.error,
      `a corrupt leaf answers an error, not the value: ${JSON.stringify(corrupt).slice(0, 200)}`,
    );
    // Missing.
    await bucket.delete(prefix + leaf);
    const missing = await titles(mf);
    assert.ok(
      missing.error,
      `a missing leaf answers an error: ${JSON.stringify(missing).slice(0, 200)}`,
    );
    // Back: once the backoff is over, the value reads again.
    await bucket.put(prefix + leaf, original);
    await eventually(async () => {
      const answer = await titles(mf);
      return Array.isArray(answer) && answer[0] === big;
    }, 40_000);
  } finally {
    await mf.dispose();
    rmSync(dirs.root, { recursive: true, force: true });
  }
});

test("incomplete explicit credentials fail even when Worker secrets are available", () => {
  const env = {
    AWS_ACCESS_KEY_ID: "ambient-id",
    AWS_SECRET_ACCESS_KEY: "ambient-secret",
    BLOB_READ_WRITE_TOKEN: "ambient-token",
  };
  const s3 = { kind: "s3", bucket: "chunks", region: "us-east-1" };
  assert.throws(
    () => blobStore({ ...s3, credentials: { accessKeyId: "explicit-id" } }, env),
    /credentials.secretAccessKey/,
  );
  assert.throws(
    () =>
      blobStore(
        { ...s3, credentials: { accessKeyId: "id", secretAccessKey: "secret", sessionToken: "" } },
        env,
      ),
    /credentials.sessionToken/,
  );
  assert.throws(
    () => blobStore({ kind: "vercel-blob", credentials: {} }, env),
    /credentials.readWriteToken/,
  );
});

// Verify the actual Worker fetches at the HTTP boundary, including the
// canonical S3 path, body hash, signed headers and temporary credentials.
function checkSignature(request, body) {
  const headers = request.headers;
  const url = new URL(request.url);
  const hash = (v) => createHash("sha256").update(v).digest("hex");
  const mac = (k, v) => createHmac("sha256", k).update(v).digest();
  const [, scope, names, signature] =
    /^AWS4-HMAC-SHA256 Credential=test-id\/([^,]+), SignedHeaders=([^,]+), Signature=([a-f0-9]+)$/.exec(
      headers.get("authorization"),
    ) ?? [];
  const date = headers.get("x-amz-date");
  assert.equal(scope, date.slice(0, 8) + "/auto/s3/aws4_request");
  assert.equal(headers.get("x-amz-security-token"), "test-session");
  assert.equal(headers.get("x-amz-content-sha256"), hash(body));
  const canonical = names
    .split(";")
    .map((n) => n + ":" + (n === "host" ? url.host : headers.get(n)) + "\n")
    .join("");
  const key = scope.split("/").reduce(mac, "AWS4test-secret");
  const digest = hash([request.method, url.pathname, "", canonical, names, hash(body)].join("\n"));
  assert.equal(
    signature,
    mac(key, ["AWS4-HMAC-SHA256", date, scope, digest].join("\n")).toString("hex"),
  );
}

for (const mode of ["s3", "vercel-blob"]) {
  test(
    mode + " offloads, retries, reads after restart, checks ownership and reclaims",
    async () => {
      const objects = new Map();
      let failUploads = true;
      let failed = 0;
      const outboundService = async (request) => {
        const url = new URL(request.url);
        const body = Buffer.from(await request.arrayBuffer());
        let key;
        let exclusive;
        if (mode === "s3") {
          assert.equal(url.host, "objects.example");
          assert.ok(url.pathname.startsWith("/chunks/"));
          key = decodeURIComponent(url.pathname.slice("/chunks/".length));
          checkSignature(request, body);
          exclusive = request.headers.get("if-none-match") === "*";
        } else {
          assert.equal(request.headers.get("authorization"), "Bearer vercel_blob_rw_Store123_test");
          if (request.method === "GET") {
            assert.equal(url.host, "store123.private.blob.vercel-storage.com");
            key = decodeURIComponent(url.pathname.slice(1));
            if (key.endsWith("/.jazz-owner")) assert.equal(url.search, "?cache=0");
          } else {
            assert.equal(url.origin, "https://vercel.com");
            assert.equal(request.headers.get("x-api-version"), "13");
            assert.equal(request.headers.get("x-vercel-blob-store-id"), "Store123");
            if (request.method === "POST") {
              assert.equal(url.pathname, "/api/blob/delete");
              for (const name of JSON.parse(body).urls) objects.delete(name);
              return new Response();
            }
            assert.equal(url.pathname, "/api/blob/");
            assert.equal(request.headers.get("x-vercel-blob-access"), "private");
            assert.equal(request.headers.get("x-add-random-suffix"), "0");
            assert.equal(request.headers.get("x-content-length"), String(body.length));
            key = url.searchParams.get("pathname");
            exclusive = request.headers.get("x-allow-overwrite") === "0";
          }
        }
        assert.ok(key.startsWith("shared chunks/"));
        if (request.method === "GET")
          return objects.has(key)
            ? new Response(objects.get(key))
            : new Response(null, { status: 404 });
        if (request.method === "DELETE") {
          objects.delete(key);
          return new Response();
        }
        assert.equal(request.method, "PUT");
        assert.equal(exclusive, key.endsWith("/.jazz-owner"));
        if (exclusive && objects.has(key)) return new Response(null, { status: 412 });
        if (!exclusive) {
          const hash = createHash("sha256").update(body).digest();
          assert.equal(key.split("/").at(-1), hash.toString("hex"));
          if (mode === "s3")
            assert.equal(request.headers.get("x-amz-checksum-sha256"), hash.toString("base64"));
          if (failUploads) {
            failed++;
            return new Response(null, { status: 503 });
          }
        }
        objects.set(key, body);
        return new Response();
      };
      const dirs = scratch();
      const vars = {
        AWS_ACCESS_KEY_ID: "test-id",
        AWS_SECRET_ACCESS_KEY: "test-secret",
        AWS_SESSION_TOKEN: "test-session",
        BLOB_READ_WRITE_TOKEN: "vercel_blob_rw_Store123_test",
      };
      const config =
        mode === "s3"
          ? { kind: mode, bucket: "chunks", endpoint: "https://objects.example" }
          : { kind: mode };
      const credentials =
        mode === "s3"
          ? {
              accessKeyId: vars.AWS_ACCESS_KEY_ID,
              secretAccessKey: vars.AWS_SECRET_ACCESS_KEY,
              sessionToken: vars.AWS_SESSION_TOKEN,
            }
          : { readWriteToken: vars.BLOB_READ_WRITE_TOKEN };
      const boot = (dir, explicit) =>
        start(dir, {
          blobs: false,
          outboundService,
          vars: {
            ...(explicit ? Object.fromEntries(Object.keys(vars).map((k) => [k, "unused"])) : vars),
            TEST_DESTINATION: {
              ...config,
              prefix: "shared chunks",
              ...(explicit && { credentials }),
            },
          },
          script: `
        import { createJazzCore, routeJazzRequest } from "./do/index.js";
        export const JazzCore = createJazzCore({ blobs: env => env.TEST_DESTINATION });
        export default { fetch: routeJazzRequest };
      `,
        });
      let mf = boot(dirs, true);
      try {
        const ready = await mf.dispatchFetch("http://core/ready");
        assert.equal(ready.status, 200, await ready.text());
        const big = largeTitle(9, 300 * 1024);
        await write(mf, big);
        await eventually(() => failed > 0);
        assert.ok((await inline(mf)).leafBytes >= big.length);
        assert.deepEqual(await titles(mf), [big]);
        failUploads = false;
        await eventually(async () => (await inline(mf)).leafBytes === 0);
        await mf.dispose();
        mf = boot({ ...dirs, objects: dirs.other }, false);
        const refused = await mf.dispatchFetch("http://core/ready");
        assert.equal(refused.status, 503);
        assert.equal((await refused.json()).error.code, "large_value_bucket_refused");
        await mf.dispose();
        mf = boot(dirs, false);
        const key = [...objects.keys()].find((k) => nodeName.test(k.split("/").at(-1)));
        const original = objects.get(key);
        objects.set(key, Buffer.from("wrong bytes"));
        assert.ok((await titles(mf)).error);
        objects.set(key, original);
        await eventually(async () => (await titles(mf))?.[0] === big);
        const reclaimed = await mf.dispatchFetch(
          `http://core/apps/${APP}/test/unreference-large-values`,
          { method: "POST", headers: admin },
        );
        assert.equal(reclaimed.status, 204);
        await eventually(() => objects.size === 1 && objects.has("shared chunks/.jazz-owner"));
      } finally {
        await mf.dispose();
        rmSync(dirs.root, { recursive: true, force: true });
      }
    },
  );
}
