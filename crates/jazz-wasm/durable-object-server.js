// Production Durable Object host adapter. The deployment entry point supplies its built
// WASM module; this file neither imports browser SQLite nor owns fixture setup.
const MAX_BODY_BYTES = 8 << 20; // Native MAX_CATALOGUE_REQUEST_BODY_BYTES.
const MAX_PENDING_BYTES = 16 << 20;
const MAX_PENDING_REQUESTS = 16;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const json = (body, status = 200) => Response.json(body, { status });
const failure = (status, code, error) => json({ code, error }, status);

function route(request, env) {
  const app = env.JAZZ_APP_ID;
  if (
    typeof app !== "string" ||
    !UUID.test(app) ||
    typeof env.JAZZ_ADMIN_SECRET !== "string" ||
    !env.JAZZ_ADMIN_SECRET
  ) {
    return failure(503, "unavailable", "Jazz host configuration is incomplete");
  }
  const prefix = `/apps/${app}/`;
  const path = new URL(request.url).pathname;
  if (!path.startsWith(prefix)) return failure(404, "not_found", "app not found");
  return path.slice(prefix.length);
}

// One namespace per deployed host, one stable object name per configured app.
// The object checks the app and credentials again, including direct stub calls.
export function routeJazzRequest(request, env) {
  const path = route(request, env);
  if (path instanceof Response) return path;
  return env.JAZZ.get(env.JAZZ.idFromName(env.JAZZ_APP_ID)).fetch(request);
}

export function createJazzDurableObject({
  WasmServerCore,
  authorizeDurableObjectAdmin,
  authorizeDurableObjectAccess,
}) {
  return class JazzDurableObject {
    constructor(ctx, env) {
      this.ctx = ctx;
      this.env = env;
      this.core = undefined;
      this.tail = Promise.resolve();
      this.pendingRequests = 0;
      this.pendingBytes = 0;
      this.tickScheduled = false;
      this.failed = false;
      this.sessions = new Set();
      this.socketQueuedBytes = 0;
    }

    enqueue(operation) {
      const result = this.tail.then(operation);
      this.tail = result.catch(() => {});
      return result;
    }

    schedule() {
      if (this.tickScheduled || this.failed) return;
      this.tickScheduled = true;
      // Do not re-enter Rust from its waker, nor join an initialization scope.
      queueMicrotask(() => {
        const task = this.enqueue(async () => {
          this.tickScheduled = false;
          if (this.core && !this.failed) {
            await this.core.tick();
            this.flushSockets();
          }
        }).catch((error) => {
          this.failed = true;
          for (const session of this.sessions) session.close(1011, "authority failed");
          console.error("Jazz authority tick failed", error);
        });
        this.ctx.waitUntil(task);
      });
    }

    async ensureOpen() {
      if (!this.core)
        this.core = await WasmServerCore.openDurableObject(
          this.ctx.storage,
          this.env.JAZZ_APP_ID,
          () => this.schedule(),
          undefined,
        );
      return this.core;
    }

    flushSockets() {
      for (const session of this.sessions) {
        if (!session.transport) continue;
        try {
          for (const frame of session.transport.recvWireFrames()) session.ws.send(frame);
          if (session.transport.isClosed()) session.close(1000, "session ended");
        } catch {
          session.close(1011, "session output failed");
        }
      }
    }

    openSocket(credential, access) {
      if (this.sessions.size >= 64) return failure(503, "backpressure", "socket capacity exceeded");
      const [client, ws] = Object.values(new WebSocketPair());
      ws.accept();
      ws.binaryType = "arraybuffer";
      let stopped = false,
        draining = false,
        prelude,
        queuedBytes = 0;
      const queue = [];
      const session = {
        ws,
        transport: undefined,
        close: (code, reason) => {
          if (stopped) return;
          cleanup();
          try {
            ws.close(code, reason);
          } catch {
            /* Already closed. */
          }
        },
      };
      const cleanup = () => {
        if (stopped) return;
        stopped = true;
        clearTimeout(deadline);
        this.socketQueuedBytes -= queuedBytes;
        queuedBytes = 0;
        queue.length = 0;
        this.sessions.delete(session);
        session.transport?.close();
        session.transport?.free();
        session.transport = undefined;
      };
      const deadline = setTimeout(() => session.close(1008, "handshake timeout"), 2000);
      this.sessions.add(session);
      const drain = async () => {
        if (draining) return;
        draining = true;
        try {
          while (!stopped && queue.length) {
            const frame = queue[0];
            if (!prelude && !session.transport) {
              prelude = frame;
              queue.shift();
              // Retained prelude bytes remain charged until admission finishes.
              continue;
            } else if (!session.transport) {
              // Admission shares the owner queue with publication and ticks.
              await this.enqueue(async () => {
                if (stopped) return;
                if (!(await this.ensureOpen())) throw new Error("no published schema");
                const admitted = this.core.acceptRemoteSocket(
                  prelude,
                  frame,
                  credential,
                  access,
                  this.env.JAZZ_ADMIN_SECRET,
                );
                try {
                  session.transport = admitted.takeTransport();
                  ws.send(admitted.response());
                } finally {
                  admitted.free();
                }
                if (!session.transport) {
                  session.close(1008, "admission refused");
                  return;
                }
                clearTimeout(deadline);
                queuedBytes -= prelude.byteLength;
                this.socketQueuedBytes -= prelude.byteLength;
                prelude = undefined;
                this.schedule();
              });
            } else {
              // SessionInput accepts frames without borrowing the owner, even
              // while a tick is awaiting engine progress. Never queue these
              // behind that tick: it may be waiting for precisely this input.
              session.transport.sendWireFrame(frame);
              this.schedule();
            }
            if (stopped) return;
            queue.shift();
            queuedBytes -= frame.byteLength;
            this.socketQueuedBytes -= frame.byteLength;
          }
        } catch {
          session.close(1008, "session failed");
        } finally {
          draining = false;
        }
      };
      ws.addEventListener("message", (event) => {
        if (stopped) return;
        if (typeof event.data === "string" && (prelude || session.transport || queue.length)) {
          session.close(1008, "binary wire message required");
          return;
        }
        const frame =
          typeof event.data === "string"
            ? new TextEncoder().encode(event.data)
            : new Uint8Array(event.data);
        if (
          frame.byteLength > 2 * 1024 * 1024 ||
          queuedBytes + frame.byteLength > 8 * 1024 * 1024 ||
          this.socketQueuedBytes + frame.byteLength > MAX_PENDING_BYTES
        ) {
          session.close(1009, "socket input budget exceeded");
          return;
        }
        queue.push(frame);
        queuedBytes += frame.byteLength;
        this.socketQueuedBytes += frame.byteLength;
        this.ctx.waitUntil(drain());
      });
      ws.addEventListener("close", cleanup);
      ws.addEventListener("error", cleanup);
      return new Response(null, { status: 101, webSocket: client });
    }

    async readBody(request, reservation) {
      if (!request.body) return "";
      const reader = request.body.getReader();
      const chunks = [];
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (reservation.bytes + value.byteLength > MAX_BODY_BYTES) {
            throw {
              status: 413,
              body: { code: "bad_request", error: "catalogue request is too large" },
            };
          }
          if (this.pendingBytes + value.byteLength > MAX_PENDING_BYTES) {
            throw {
              status: 503,
              body: { code: "backpressure", error: "catalogue input budget exceeded" },
            };
          }
          reservation.bytes += value.byteLength;
          this.pendingBytes += value.byteLength;
          chunks.push(value);
        }
      } catch (error) {
        await reader.cancel().catch(() => {});
        throw error;
      } finally {
        reader.releaseLock();
      }
      const bytes = new Uint8Array(reservation.bytes);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      try {
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      } catch {
        throw { status: 400, body: { code: "bad_request", error: "request body is not UTF-8" } };
      }
    }

    async handle(method, path, body) {
      if (this.failed) return failure(503, "unavailable", "Jazz authority must restart");
      if (!this.core) {
        // Recovery uses only the existing catalogue, never the submitted schema.
        this.core = await WasmServerCore.openDurableObject(
          this.ctx.storage,
          this.env.JAZZ_APP_ID,
          () => this.schedule(),
          undefined,
        );
      }
      if (method === "POST") {
        if (this.core) {
          const reply = JSON.parse(await this.core.publishCatalogue(body));
          this.flushSockets();
          return json(reply.body, reply.status);
        }
        let candidate;
        let reply;
        try {
          // This scope does no network I/O and serves no sessions. A refused
          // first deployment must not leave an admitted genesis behind.
          await this.ctx.storage.transaction(async () => {
            candidate = await WasmServerCore.openDurableObject(
              this.ctx.storage,
              this.env.JAZZ_APP_ID,
              () => this.schedule(),
              body,
            );
            reply = JSON.parse(await candidate.publishCatalogue(body));
            if (reply.status !== 200) throw reply;
          });
        } catch (error) {
          candidate?.free();
          throw error;
        }
        this.core = candidate;
        return json(reply.body, reply.status);
      }
      if (path.startsWith("schema/")) {
        const hash = path.slice("schema/".length);
        if (!/^[0-9a-fA-F]{64}$/.test(hash))
          return failure(400, "bad_request", "invalid schema hash");
        const schema = this.core?.catalogueSchema(hash);
        return schema
          ? new Response(schema, { headers: { "content-type": "application/json" } })
          : failure(404, "not_found", `schema catalogue not found for hash ${hash}`);
      }
      const snapshot = this.core
        ? JSON.parse(this.core.catalogueSnapshot())
        : {
            schemas: { hashes: [], schemas: [] },
            migrationGraph: { latestBundleSchemaHash: null, schemas: [], migrations: [] },
            bundles: { bundles: [] },
          };
      return json(
        path === "schemas"
          ? snapshot.schemas
          : path === "admin/migrations/graph"
            ? snapshot.migrationGraph
            : snapshot.bundles,
      );
    }

    async fetch(request) {
      const path = route(request, this.env);
      if (path instanceof Response) return path;
      const credential = request.headers.get("x-jazz-backend-credential") ?? "";
      const access = request.headers.get("x-jazz-backend-access") ?? "";
      if (path === "backend/admit" || path === "ws") {
        if (!authorizeDurableObjectAccess(credential, access, this.env.JAZZ_ADMIN_SECRET))
          return failure(401, "unauthorized", "backend credential required");
        if (path === "backend/admit") {
          if (access !== "backend" && access !== "admin")
            return failure(401, "unauthorized", "backend access required");
          return request.method === "POST"
            ? new Response(null, { status: 204 })
            : failure(405, "method_not_allowed", "method not allowed");
        }
        if (
          request.method !== "GET" ||
          request.headers.get("upgrade")?.toLowerCase() !== "websocket"
        )
          return failure(426, "upgrade_required", "WebSocket upgrade required");
        if (this.failed) return failure(503, "unavailable", "Jazz authority must restart");
        return this.openSocket(credential, access);
      }
      if (
        !authorizeDurableObjectAdmin(
          request.headers.get("x-jazz-backend-credential") ?? "",
          request.headers.get("x-jazz-backend-access") ?? "",
          this.env.JAZZ_ADMIN_SECRET,
        )
      )
        return failure(401, "unauthorized", "admin backend credential required");
      if (path === "admin/host" && request.method === "GET")
        return Response.json({
          appId: this.env.JAZZ_APP_ID,
          artifact: this.env.JAZZ_HOST?.artifact,
        });
      const known =
        path === "schemas" ||
        path.startsWith("schema/") ||
        path === "admin/migrations/graph" ||
        path === "admin/bundles";
      if (!known) return failure(404, "not_found", "route not found");
      if (request.method !== "GET" && !(request.method === "POST" && path === "admin/bundles")) {
        return failure(405, "method_not_allowed", "method not allowed");
      }
      if (this.pendingRequests >= MAX_PENDING_REQUESTS)
        return failure(503, "backpressure", "catalogue queue is full");
      this.pendingRequests += 1;
      const reservation = { bytes: 0 };
      const operation = (async () => {
        try {
          const body = request.method === "POST" ? await this.readBody(request, reservation) : "";
          return await this.enqueue(() => this.handle(request.method, path, body));
        } catch (error) {
          if (Number.isInteger(error?.status) && error?.body) return json(error.body, error.status);
          console.error("Jazz catalogue request failed", error);
          return failure(500, "internal", "catalogue operation failed");
        } finally {
          this.pendingRequests -= 1;
          this.pendingBytes -= reservation.bytes;
        }
      })();
      // Once accepted, publication is independent of the HTTP caller's lifetime.
      this.ctx.waitUntil(operation.then(() => {}));
      return operation;
    }
  };
}
