// The Jazz core a Cloudflare Durable Object hosts, around the Rust core in
// jazz-wasm's `durable-object` build (`DurableObjectCore`). The Worker
// sends everything to one object, which holds the app's store in its own
// SQL and serves the native server's routes for it:
//
//   GET  /ready                        200 once the store is open
//   GET  /app                          {"appId"}: for the backend's credential
//   GET  /apps/{x}/ws                  WebSocket: prelude, Hello, then wire frames
//   POST /apps/{x}/backend/admit       204 for the app's backend
//   GET  /apps/{x}/admin/migrations/graph, GET|POST /apps/{x}/admin/bundles
//   POST /apps/{x}/admin/accounts/{resolve,login-or-register,link,
//        found-local-first,revoke,revoke-sessions}
//
// Nobody configures an app id: the object makes one on its first open and
// keeps it in its SQL (as a native core keeps `<data-dir>/app-id`), and
// serves its app whatever `{x}` is. Routes are checked, with the request's
// secrets, before a body (at most 8 MiB) is read.
//
// Bindings: JAZZ (this class's namespace), JAZZ_CORE_SECRET, and optionally
// JAZZ_SCHEMA (a public schema's JSON, served until one is published),
// JAZZ_BLOBS (an R2 bucket for large values, recommended) and
// JAZZ_BLOBS_PREFIX (where in it, default `jazz/<app id>/`).
//
// Large values: with inline storage their bytes stay in the object's SQL,
// which grows with every one. With R2 selected, each settled large-value node moves
// to the bucket under the prefix, named by its SHA-256 (which every put
// carries, so R2 refuses a corrupted body), and is deleted from there once
// reclaimed. The engine hands out one batch of that work at a time; this
// host runs it against the bucket and hands the outcome back, and a failed
// op is retried in a later batch, with backoff. Bytes leave the SQL only
// once their put succeeded, so a failing bucket never loses a value. A
// store that holds large values inline moves them out once the bucket is
// selected; one that moved them can't be opened without it. The prefix
// belongs to one store: the first claims it with `.jazz-owner` (written
// only if absent), and any other store is refused there (`/ready` answers
// 503 saying so, and its values stay in its SQL).
//
// What a deployment should know:
// - One secret, JAZZ_CORE_SECRET, admits the app's backend in every role a
//   native core keeps separate secrets for: backend links, admin routes
//   (publication, accounts) and authority links. The admin/backend split
//   (`X-Jazz-Backend-Access`, or which secret header is sent) is a
//   convention between the backend's own calls, not a boundary: whoever
//   holds the secret is admin. Keep it to the app's backend.
// - By default, sockets keep the object resident. In hibernate mode the
//   host restores watches and sends a fresh snapshot on the first matching change.
//   A peer must send its prelude and Hello within
//   HANDSHAKE_TIMEOUT_MS, as on a native core.
// - If the Rust core panics, the trap leaves the wasm instance unusable:
//   the object drops its core, closes every socket (1011) and replaces the
//   instance with a fresh one; the next request opens a fresh core on it,
//   over the same storage.
import { DurableObject } from "cloudflare:workers";

const decoder = new TextDecoder();

/** Bucket operations of one kind in flight at once (as the engine's own runner). */
const BUCKET_CONCURRENCY = 16;

/** Backoff after a failed bucket batch: from the first, doubling to the last. */
const BUCKET_RETRY_MS = [500, 30_000];

/** Under a store's prefix: the app id of the store that owns it. */
const OWNER_OBJECT = ".jazz-owner";

/** Why an op failed, as the core takes it. */
function reason(error) {
  return String(error?.message ?? error);
}

/** `run(item)` for every item, `limit` at a time; never rejects. */
async function each(items, limit, run) {
  const results = Array.from({ length: items.length });
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const index = next++;
      try {
        results[index] = await run(items[index]);
      } catch (error) {
        results[index] = reason(error) || "failed";
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

/**
 * A store's large values in an R2 bucket (the `JAZZ_BLOBS` binding), under
 * its own prefix, which it claims for its app id before its first op.
 */
export class LargeValueBucket {
  /**
   * `claimed` records the prefix this store claimed, in its own SQL:
   * `claimed.get()` is it (or `null` before the first claim), and
   * `claimed.set(prefix)` records it.
   */
  constructor(bucket, prefix, appId, claimed) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.appId = appId;
    this.claimed = claimed;
    this.claiming = null;
  }

  /** `env.JAZZ_BLOBS_PREFIX`, ending in `/`, else `jazz/<app id>/`. */
  static prefix(configured, appId) {
    if (configured === undefined || configured === "") return `jazz/${appId}/`;
    return configured.endsWith("/") ? configured : `${configured}/`;
  }

  /**
   * Check that this store owns the prefix, claiming it if no store does: a
   * put of `.jazz-owner` only if it is absent (R2's `onlyIf`), which answers
   * `null` when another put got there first. Resolves to `null` once owned,
   * or to why the store is refused (for good: another store owns it, or
   * the bucket or prefix is not the one this store claimed, where its
   * values are). A failure to reach the bucket rejects and is tried again
   * next time.
   */
  claim() {
    this.claiming ??= this.claimNow().catch((error) => {
      this.claiming = null;
      throw error;
    });
    return this.claiming;
  }

  async claimNow() {
    const key = this.prefix + OWNER_OBJECT;
    const refused = (owner) =>
      `JAZZ_BLOBS: ${this.prefix} in this bucket belongs to app ${owner}, not this one (${this.appId}); ` +
      "give each store its own bucket or JAZZ_BLOBS_PREFIX";
    // Once a store claimed a prefix, its moved values are there: another
    // prefix, or a bucket without its owner object (another bucket bound),
    // would split them, so it is refused until the binding is set back.
    const recorded = this.claimed.get();
    if (recorded !== null && recorded !== this.prefix) {
      return (
        `JAZZ_BLOBS: this store's large values are under ${recorded}, but JAZZ_BLOBS_PREFIX now names ${this.prefix}; ` +
        "set it back (moving a store's large values to another prefix isn't supported yet)"
      );
    }
    const owner = async () => (await (await this.bucket.get(key))?.text())?.trim();
    const found = await owner();
    if (found !== undefined) {
      if (found !== this.appId) return refused(found);
      if (recorded === null) this.claimed.set(this.prefix);
      return null;
    }
    if (recorded !== null) {
      return (
        `JAZZ_BLOBS: ${key} is missing from the bound bucket, though this store claimed ${recorded} there: ` +
        "the binding names another bucket (bind the store's own again), or the owner object was deleted"
      );
    }
    const put = await this.bucket.put(key, `${this.appId}\n`, {
      onlyIf: { etagDoesNotMatch: "*" },
    });
    if (put !== null) {
      this.claimed.set(this.prefix);
      return null;
    }
    // Another store claimed it first.
    const winner = await owner();
    if (winner === undefined)
      throw new Error("JAZZ_BLOBS: the owner object vanished while claiming");
    if (winner !== this.appId) return refused(winner);
    this.claimed.set(this.prefix);
    return null;
  }

  /**
   * Run a batch the core handed out (`{ fetches, puts, deletes }`, keys
   * in hex): one result per op, `undefined` for success, a string for why
   * it failed; a fetch's bytes, or `null` for no such object. A put's
   * bytes are copied out of the core (`bytesOf(index)`) only as it starts,
   * so at most `BUCKET_CONCURRENCY` of them are in JS at once.
   */
  async run({ fetches = [], puts = [], deletes = [] }, bytesOf) {
    let refusal;
    try {
      refusal = await this.claim();
    } catch (error) {
      refusal = `JAZZ_BLOBS: claim ${this.prefix}: ${reason(error)}`;
    }
    if (refusal !== null) {
      return {
        fetched: fetches.map(() => refusal),
        put: puts.map(() => refusal),
        deleted: deletes.map(() => refusal),
      };
    }
    const [fetched, put, deleted] = await Promise.all([
      each(fetches, BUCKET_CONCURRENCY, async (key) => {
        const object = await this.bucket.get(this.prefix + key);
        return object === null ? null : new Uint8Array(await object.arrayBuffer());
      }),
      each(
        puts.map((key, index) => ({ key, index })),
        BUCKET_CONCURRENCY,
        async ({ key, index }) => {
          const bytes = bytesOf(index);
          if (bytes === undefined) throw new Error("the core that handed this put out is gone");
          // A node's name is its bytes' SHA-256: R2 refuses a body that differs.
          await this.bucket.put(this.prefix + key, bytes, { sha256: key });
        },
      ),
      this.deleteAll(deletes),
    ]);
    return { fetched, put, deleted };
  }

  /** One bulk delete (a batch holds at most 256): every key's result is its call's. */
  async deleteAll(keys) {
    if (keys.length === 0) return [];
    try {
      await this.bucket.delete(keys.map((key) => this.prefix + key));
      return keys.map(() => undefined);
    } catch (error) {
      const why = reason(error) || "failed";
      return keys.map(() => why);
    }
  }
}

/**
 * The prefix a store claimed in its bucket, kept in the object's own SQL
 * (`_jazz_host_blobs`, one row) from its first claim on: see
 * `LargeValueBucket`.
 */
function claimedPrefix(sql) {
  return {
    get() {
      sql.exec("CREATE TABLE IF NOT EXISTS _jazz_host_blobs (prefix TEXT NOT NULL)");
      return sql.exec("SELECT prefix FROM _jazz_host_blobs LIMIT 1").toArray()[0]?.prefix ?? null;
    },
    set(prefix) {
      sql.exec("CREATE TABLE IF NOT EXISTS _jazz_host_blobs (prefix TEXT NOT NULL)");
      sql.exec("DELETE FROM _jazz_host_blobs");
      sql.exec("INSERT INTO _jazz_host_blobs (prefix) VALUES (?)", prefix);
    },
  };
}

/** The longest request body read (native's catalogue limit). */
const MAX_REQUEST_BYTES = 8 << 20;

/** How long a new socket may take to send its prelude and Hello (native's). */
const HANDSHAKE_TIMEOUT_MS = 2000;

function failure(status, code, message) {
  return Response.json({ error: { code, message } }, { status });
}

/** A binary message's bytes (an ArrayBuffer, a view, or a Blob), else null. */
async function bytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data))
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof Blob !== "undefined" && data instanceof Blob)
    return new Uint8Array(await data.arrayBuffer());
  return null;
}

/** `request`'s body, or null when it is longer than `limit` bytes. */
async function readBody(request, limit) {
  if (Number(request.headers.get("content-length") ?? 0) > limit) return null;
  if (request.body === null) return new Uint8Array();
  const chunks = [];
  let length = 0;
  const reader = request.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function close(ws, code, reason) {
  try {
    ws.close(code, String(reason).slice(0, 120));
  } catch {}
}

// Bumped whenever the instance is replaced: a core made on an older one
// is dropped, never called.
let instance = 0;

/**
 * The Durable Object class over `wasm`: jazz-wasm's durable-object build
 * (initialised), as `{ DurableObjectCore, reinstantiate }`, where
 * `reinstantiate()` replaces its wasm instance after a trap. `index.js`
 * exports it ready over the packaged build.
 */
export function jazzCoreClass(wasm, blobs) {
  return class JazzCore extends DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      this.env = env;
      const destination = typeof blobs === "function" ? blobs(env) : blobs;
      if (destination !== "sqlite" && !(destination?.kind === "r2" && destination.bucket)) {
        throw new Error('createJazzCore(): select "sqlite" or r2(bucket)');
      }
      this.blobBucket = destination === "sqlite" ? undefined : destination.bucket;
      this.core = null;
      this.sockets = new Map();
      this.flushTimer = undefined;
      this.alarmSet = false;
      this.handlers = new WeakMap();
      // Large values in the R2 bucket, if bound (see the top of this file).
      this.bucket = null;
      this.chunkIo = { out: false, failures: 0, retryAt: 0, logged: false };
      this.chunkFetches = { out: false, failures: 0, retryAt: 0, logged: false };
      this.bucketTimer = undefined;
      for (const ws of ctx.getWebSockets()) {
        if (ws.readyState !== WebSocket.OPEN) continue;
        try {
          const saved = ws.deserializeAttachment();
          if (!saved?.length) throw new Error("no resumable session");
          const core = this.open();
          const socket = this.guard(() => core.restoreSocket(saved));
          this.sockets.set(socket, ws);
          this.bindSocket(ws, core, true, socket);
        } catch {
          close(ws, 1012, "Jazz session must reconnect");
        }
      }
      if (this.sockets.size) {
        this.flush();
        const core = this.current();
        if (core) this.guard(() => core.finishSocketRestore());
      }
    }

    open() {
      if (this.current() === null) {
        this.core = this.guard(() =>
          wasm.DurableObjectCore.open(
            this.ctx.storage,
            this.env.JAZZ_CORE_SECRET,
            this.env.JAZZ_SCHEMA,
            this.blobBucket !== undefined,
          ),
        );
        this.instance = instance;
        this.chunkIo.out = false;
        this.chunkFetches.out = false;
        if (this.blobBucket !== undefined && this.bucket === null) {
          const appId = this.core.appId();
          this.bucket = new LargeValueBucket(
            this.blobBucket,
            LargeValueBucket.prefix(this.env.JAZZ_BLOBS_PREFIX, appId),
            appId,
            claimedPrefix(this.ctx.storage.sql),
          );
        }
        // What an earlier life left to move or delete goes on now.
        this.pumpBucket(this.core);
      }
      return this.core;
    }

    /**
     * Hand the bucket the core's next batch, unless one is out or the last
     * failed too recently; and, beside it, the fetches reads wait on. Each
     * comes back to `finishBucket`, which ticks the core and pumps again.
     */
    pumpBucket(core) {
      if (this.bucket === null) return;
      const now = Date.now();
      const holdFetches = now < this.chunkFetches.retryAt;
      if (!this.chunkIo.out && now >= this.chunkIo.retryAt) {
        let batch;
        try {
          batch = this.guard(() => core.takeChunkIo(holdFetches));
        } catch (error) {
          this.logBucketFailure(this.chunkIo, reason(error));
        }
        if (batch !== undefined) {
          this.chunkIo.out = true;
          const bytesOf = (index) =>
            this.current() === core ? this.guard(() => core.chunkPutBytes(index)) : undefined;
          this.bucket
            .run(batch, bytesOf)
            .then((done) => this.finishBucket(core, done, false))
            .catch((error) => this.bucketBroke(core, this.chunkIo, error));
        }
      }
      if (!this.chunkFetches.out && !holdFetches) {
        let fetches;
        try {
          fetches = this.guard(() => core.takeChunkFetches());
        } catch (error) {
          this.logBucketFailure(this.chunkFetches, reason(error));
        }
        if (fetches !== undefined) {
          this.chunkFetches.out = true;
          this.bucket
            .run(fetches, () => undefined)
            .then((done) => this.finishBucket(core, done, true))
            .catch((error) => this.bucketBroke(core, this.chunkFetches, error));
        }
      }
    }

    /** A batch (or fetches) came back from the bucket: hand it to its core. */
    finishBucket(core, done, fetchLane) {
      // A core replaced since dropped what it handed out; the new one hands
      // the same work out again.
      if (this.current() !== core) return;
      const lane = fetchLane ? this.chunkFetches : this.chunkIo;
      lane.out = false;
      let failure;
      try {
        failure = this.guard(() =>
          fetchLane
            ? core.finishChunkFetches(done.fetched)
            : core.finishChunkIo(done.fetched, done.put, done.deleted),
        );
      } catch (error) {
        failure = reason(error);
      }
      // Failed fetches (an error, or no object where one was moved) back
      // fetches off in both lanes, as a failed batch backs off batches.
      if (done.fetched.length > 0) {
        this.backOff(
          this.chunkFetches,
          done.fetched.some((fetched) => !(fetched instanceof Uint8Array)),
        );
      }
      if (!fetchLane) this.backOff(this.chunkIo, failure !== undefined);
      if (failure !== undefined) this.logBucketFailure(lane, failure);
      else lane.logged = false;
      this.flush();
      this.rearmBucket();
    }

    backOff(lane, failed) {
      if (!failed) {
        lane.failures = 0;
        lane.retryAt = 0;
        return;
      }
      const [first, last] = BUCKET_RETRY_MS;
      lane.retryAt = Date.now() + Math.min(last, first * 2 ** lane.failures);
      lane.failures += 1;
    }

    /**
     * Running a batch threw past its own handling (a bug, never the
     * bucket): the batch was dropped, so its work goes to the next. Back
     * off as for a failed batch, so a fault that repeats doesn't spin.
     */
    bucketBroke(core, lane, error) {
      if (this.current() !== core) return;
      lane.out = false;
      this.backOff(lane, true);
      this.logBucketFailure(lane, reason(error));
      this.rearmBucket();
    }

    /**
     * Wake for whatever waits out a backoff: at the earliest lane's retry,
     * then again for the other's, so re-arming for one lane never drops the
     * other's wake.
     */
    rearmBucket() {
      const now = Date.now();
      const due = [this.chunkIo.retryAt, this.chunkFetches.retryAt].filter((at) => at > now);
      clearTimeout(this.bucketTimer);
      if (due.length === 0) return;
      this.bucketTimer = setTimeout(
        () => {
          const current = this.current();
          if (current === null) return;
          this.pumpBucket(current);
          this.rearmBucket();
        },
        Math.min(...due) - now,
      );
    }

    /** A lane's first failure of a run, then every tenth while it lasts. */
    logBucketFailure(lane, failure) {
      if (lane.logged && lane.failures % 10 !== 0) return;
      lane.logged = true;
      console.error(`Jazz large-value bucket: ${failure}; retrying with backoff`);
    }

    /**
     * The core, if it is on the current wasm instance: one made on an
     * instance replaced since (after a panic in any object of this
     * isolate) is dropped, never called.
     */
    current() {
      if (this.core !== null && this.instance !== instance) this.drop();
      return this.core;
    }

    /**
     * Forget the core without calling into its instance (which may have
     * trapped, or been replaced): its memory goes with the instance.
     */
    drop() {
      try {
        this.core?.__destroy_into_raw();
      } catch {}
      this.core = null;
      for (const ws of this.sockets.values()) close(ws, 1011, "the core restarted");
      this.sockets.clear();
    }

    /**
     * Run `call` on the core. A Rust panic traps the wasm instance and
     * leaves the core unusable: drop it, close every socket, and reset the
     * object (the next request starts afresh on the same storage).
     */
    guard(call) {
      try {
        return call();
      } catch (error) {
        if (error instanceof WebAssembly.RuntimeError) {
          this.drop();
          instance += 1;
          wasm.reinstantiate();
        }
        throw error;
      }
    }

    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/ready") {
        this.open();
        if (this.bucket !== null) {
          let refusal;
          try {
            refusal = await this.bucket.claim();
          } catch (error) {
            const why = `JAZZ_BLOBS: claim ${this.bucket.prefix}: ${reason(error)}`;
            return failure(503, "large_value_bucket_unavailable", why);
          }
          if (refusal !== null) return failure(503, "large_value_bucket_refused", refusal);
        }
        return new Response("ok");
      }
      if (!/^\/apps\/[^/]+\/ws$/.test(path)) {
        const header = (name) => request.headers.get(name) ?? undefined;
        const secrets = [
          header("X-Jazz-Admin-Secret"),
          header("X-Jazz-Backend-Secret"),
          header("X-Jazz-Backend-Credential"),
          header("X-Jazz-Backend-Access"),
        ];
        const core = this.open();
        // The route and secrets are checked before the body is read.
        let answer = this.guard(() => core.authorize(request.method, path, ...secrets));
        if (answer === undefined) {
          const body = await readBody(request, MAX_REQUEST_BYTES);
          if (body === null) {
            return failure(413, "payload_too_large", "the request body is too large");
          }
          const current = this.open();
          answer = this.guard(() => current.http(request.method, path, ...secrets, body));
          // A deployment may end sessions pinned to older bundles: tell them.
          this.flush();
        }
        return answer.contentType === ""
          ? new Response(null, { status: answer.status })
          : new Response(answer.body, {
              status: answer.status,
              // Answers to the app's backend (accounts, its signing key,
              // deployments): never kept by a cache on the way.
              headers: { "content-type": answer.contentType, "cache-control": "no-store" },
            });
      }
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return failure(426, "upgrade_required", "WebSocket upgrade required");
      }
      const core = this.open();
      const [client, server] = Object.values(new WebSocketPair());
      // Binary messages as ArrayBuffers, not Blobs (each Blob is one more
      // async read), whatever the Worker's compatibility date.
      server.binaryType = "arraybuffer";
      const hibernate = request.headers.get("X-Jazz-Socket-Lifecycle") === "hibernate";
      if (hibernate) this.acceptHibernatingWebSocket(server);
      else server.accept();
      const credential = request.headers.get("X-Jazz-Backend-Credential") ?? undefined;
      const access = request.headers.get("X-Jazz-Backend-Access") ?? undefined;
      this.bindSocket(server, core, hibernate, null, credential, access);
      return new Response(null, { status: 101, webSocket: client });
    }

    acceptHibernatingWebSocket(server) {
      this.ctx.acceptWebSocket(server);
    }

    bindSocket(server, core, hibernate, socket, credential, access) {
      let prelude = null;
      let ended = false;
      const end = (code, reason) => {
        if (ended) return;
        ended = true;
        clearTimeout(handshake);
        // A core replaced since (after a panic) numbers its own sockets:
        // only the core this socket was accepted by is told.
        if (socket !== null && this.current() === core) {
          // What the socket's session still had, its error frame included.
          try {
            for (const batch of this.guard(() => core.output(socket)).batches) {
              server.send(batch);
            }
          } catch {}
          this.sockets.delete(socket);
          try {
            if (this.current() === core) core.close(socket);
          } catch {}
          // What the session held (its writes' large values) may settle
          // now: tick, so it moves to the bucket.
          if (this.bucket !== null) this.flush();
        }
        socket = null;
        close(server, code, reason);
      };
      const handshake =
        socket === null
          ? setTimeout(() => {
              if (socket === null) end(1008, "the handshake timed out");
            }, HANDSHAKE_TIMEOUT_MS)
          : undefined;
      // A socket's messages are taken in order, one at a time (reading a
      // Blob's bytes waits); a failure ends this socket alone, and the
      // queue never rejects, so later messages are not silently dropped.
      let queue = Promise.resolve();
      const take = async (data) => {
        if (ended) return;
        try {
          if (socket === null && prelude === null) {
            prelude = typeof data === "string" ? data : decoder.decode(await bytes(data));
            return;
          }
          const message = await bytes(data);
          if (message === null) return end(1003, "wire frames are binary");
          if (this.current() !== core) return end(1011, "the core restarted");
          if (socket === null) {
            const accepted = this.guard(() =>
              hibernate
                ? core.acceptHibernating(prelude, message, credential, access)
                : core.accept(prelude, message, credential, access),
            );
            for (const frame of accepted.frames) server.send(frame);
            if (accepted.socket === undefined) return end(1008, "refused");
            clearTimeout(handshake);
            socket = accepted.socket;
            this.sockets.set(socket, server);
          } else {
            this.guard(() => core.receive(socket, message));
          }
          this.flush();
        } catch (error) {
          end(1011, error?.message ?? error);
        }
      };
      const message = (data) => (queue = queue.then(() => take(data)));
      if (hibernate) {
        this.handlers.set(server, { message, end });
      } else {
        server.addEventListener("message", (event) => message(event.data));
        server.addEventListener("close", () => end(1000, "closed"));
        server.addEventListener("error", () => end(1011, "socket error"));
      }
    }

    webSocketMessage(ws, data) {
      const handler = this.handlers.get(ws);
      if (handler) return handler.message(data);
      // The transport survived eviction, but its WASM session did not.
      close(ws, 1012, "Jazz session must reconnect");
    }

    webSocketClose(ws) {
      const handler = this.handlers.get(ws);
      if (handler) handler.end(1000, "closed");
      else close(ws, 1000, "closed");
    }

    webSocketError(ws) {
      const handler = this.handlers.get(ws);
      if (handler) handler.end(1011, "socket error");
      else close(ws, 1011, "socket error");
    }

    /**
     * Tick the engine once and send every socket what it has. While the core
     * wants another tick (more input waits, or taken output freed held
     * answers), the next one is queued behind the event loop, so output goes
     * out as it is made and no event runs the whole backlog. While sockets
     * are open, an alarm keeps the core's idle tick.
     */
    flush() {
      const core = this.current();
      if (core === null) return;
      try {
        this.guard(() => core.tick());
      } catch {
        return;
      }
      let needsIdleTick = false;
      for (const [socket, ws] of this.sockets) {
        try {
          const out = this.guard(() => core.output(socket));
          for (const batch of out.batches) ws.send(batch);
          if (this.handlers.has(ws) && !out.ended) {
            try {
              const attachment = this.guard(() => core.changedAttachment(socket));
              if (attachment !== undefined) ws.serializeAttachment(attachment);
            } catch (error) {
              this.handlers.get(ws).end(1011, error?.message ?? error);
              continue;
            }
          }
          if (out.ended) {
            this.sockets.delete(socket);
            close(ws, 1000, "session ended");
          } else if (!this.handlers.has(ws)) {
            needsIdleTick = true;
          }
        } catch {
          // A partially sent batch must never be resumed.
          close(ws, 1011, "socket send failed");
          this.sockets.delete(socket);
          try {
            if (this.current() === core) core.close(socket);
          } catch {}
        }
      }
      clearTimeout(this.flushTimer);
      if (this.current() === core) {
        const delay = core.wantsTick() ? 0 : core.nextChunkPullTickMs();
        if (delay !== undefined) this.flushTimer = setTimeout(() => this.flush(), delay);
      }
      if (needsIdleTick && !this.alarmSet) {
        this.alarmSet = true;
        this.ctx.storage.setAlarm(Date.now() + wasm.DurableObjectCore.idleTickMs());
      }
      if (this.current() === core) this.pumpBucket(core);
    }

    /** The core's idle tick: time-based engine work, while sockets are open. */
    async alarm() {
      this.alarmSet = false;
      if (this.current() !== null && this.sockets.size > 0) this.flush();
    }
  };
}

/** Answer liveness at the Worker and forward core requests to the object. */
export async function routeJazzRequest(request, env) {
  if (new URL(request.url).pathname === "/health") return new Response("ok");
  return env.JAZZ.get(env.JAZZ.idFromName("jazz")).fetch(request);
}
