// A stand-in for `JazzCore` (the Jazz core in a Durable Object), for tests
// that need the deployment's shape but no Jazz: `/health`, `GET /app` (the
// app id the object keeps), the native core's routes under `/apps/<x>`
// (served for the object's app whatever `<x>` is), admission by the core
// secret, and a Worker that forwards everything to its one object. Sockets
// it admits only echo; other routes answer 501. `GET /__mock/seen` lists what
// it was asked (test-only).
import { DurableObject } from "cloudflare:workers";

const REFUSED =
  "this core admits only its app's backend: connect through the app's createJazz handler";

export class JazzCore extends DurableObject {
  seen = [];

  async appId() {
    let id = await this.ctx.storage.get("appId");
    if (!id) await this.ctx.storage.put("appId", (id = crypto.randomUUID()));
    return id;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__mock/seen") return Response.json(this.seen);
    const upgrade = request.headers.get("upgrade")?.toLowerCase() === "websocket";
    const credential =
      request.headers.get("x-jazz-backend-credential") === this.env.JAZZ_CORE_SECRET;
    this.seen.push({
      method: request.method,
      path: url.pathname,
      upgrade,
      credential,
      access: request.headers.get("x-jazz-backend-access"),
    });
    if (url.pathname === "/health") return new Response("ok");
    if (!credential) return Response.json({ error: REFUSED }, { status: 401 });
    if (url.pathname === "/app") return Response.json({ appId: await this.appId() });

    const route = /^\/apps\/[^/]+(\/.*)$/.exec(url.pathname);
    if (!route) return Response.json({ error: "not found" }, { status: 404 });
    if (route[1] === "/ws") {
      if (!upgrade) return new Response("Upgrade Required", { status: 426 });
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (event) => server.send(event.data));
      return new Response(null, { status: 101, webSocket: client });
    }
    return Response.json({ error: "not implemented by the mock core" }, { status: 501 });
  }
}

export function routeJazzRequest(request, env) {
  return env.JAZZ.get(env.JAZZ.idFromName("jazz")).fetch(request);
}

export default { fetch: routeJazzRequest };
