// A stand-in for `JazzCore` (the Jazz core in a Durable Object), until the
// jazz-wasm Durable Object host lands. It keeps the host API's shape:
// `/health`, the native core's routes under `/apps/{appId}`, admission by
// the core secret, and a Worker that forwards everything to the app's one
// object. It runs no Jazz: sockets it admits only echo, and other routes
// answer 501. `GET /__mock/seen` lists what it was asked (test-only).
import { DurableObject } from "cloudflare:workers";

const REFUSED =
  "this core admits only its app's backend: connect through the app's createJazz handler";

export class JazzCore extends DurableObject {
  seen = [];

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__mock/seen") return Response.json(this.seen);
    const upgrade = request.headers.get("upgrade")?.toLowerCase() === "websocket";
    this.seen.push({
      method: request.method,
      path: url.pathname,
      upgrade,
      credential: request.headers.get("x-jazz-backend-credential") === this.env.JAZZ_CORE_SECRET,
      access: request.headers.get("x-jazz-backend-access"),
    });
    if (url.pathname === "/health") return new Response("ok");

    const route = /^\/apps\/([^/]+)(\/.*)$/.exec(url.pathname);
    if (!route || decodeURIComponent(route[1]) !== this.env.JAZZ_APP_ID) {
      return Response.json({ error: "not found" }, { status: 404 });
    }
    if (request.headers.get("x-jazz-backend-credential") !== this.env.JAZZ_CORE_SECRET) {
      return Response.json({ error: REFUSED }, { status: 401 });
    }
    if (route[2] === "/ws") {
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
  return env.JAZZ.get(env.JAZZ.idFromName(env.JAZZ_APP_ID)).fetch(request);
}

export default { fetch: routeJazzRequest };
