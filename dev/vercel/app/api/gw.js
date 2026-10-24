// Thin proxy in front of a shard core (a persistent Sandbox).
//
// - The first connection wakes the core's sandbox through the Sandbox SDK.
// - Client activity (any data frame) keeps the core's session alive by
//   extending its timeout. Idle clients don't, so the core stops (and
//   snapshots) once every client has been idle for IDLE_MS.
// - When the core stops, clients stay connected here ("parked"). Once the
//   core runs again, or a parked client sends a frame, the proxy closes the
//   client's socket so the Jazz client reconnects and resubscribes itself.
import http from "node:http";
import { Sandbox } from "@vercel/sandbox";
import { WebSocket, WebSocketServer } from "ws";
import { ensureCore } from "../lib/core.js";

const SHARD = process.env.SPIKE_SHARD ?? "core1";
const IDLE_MS = Number(process.env.SPIKE_IDLE_MS ?? 120_000);
const POLL_MS = Number(process.env.SPIKE_POLL_MS ?? 2_000);
// Close code that tells the client the core restarted; it reconnects.
const RECONNECT = 4100;

let core = null; // Promise<{ sbx, base }>
// Timers run outside any request, where the SDK finds no OIDC context, so
// keep the newest token a request carried and pass it explicitly.
let oidc = null;
const remember = (req) => {
  const token = req.headers["x-vercel-oidc-token"];
  if (token)
    oidc = {
      token,
      teamId: process.env.VERCEL_TEAM_ID ?? "team_xiRKInFi7lBd1gTqyAGuzmkC",
      projectId: process.env.VERCEL_PROJECT_ID ?? "prj_Hbz2OviWtHTATVbJCAcILadJaAXw",
    };
};
let lastActivity = 0;
const parked = new Set();
const log = (o) => console.log(JSON.stringify({ t: Date.now(), ...o }));

function wake(reason) {
  if (!core) {
    const t0 = performance.now();
    core = ensureCore({
      name: SHARD,
      data: `${SHARD}-data`,
      timeoutMs: IDLE_MS + 60_000,
      creds: oidc ?? {},
    })
      .then((r) => {
        log({
          event: "core",
          reason,
          started: r.started,
          ok: r.ok,
          ms: Math.round(performance.now() - t0),
          timings: r.timings,
        });
        return r;
      })
      .catch((e) => {
        log({ event: "core_fail", reason, error: String(e) });
        core = null;
        throw e;
      });
  }
  return core;
}

// Keep the session alive while clients are active. A session's timeout is
// counted from its start, so extend once less than IDLE_MS of it is left.
setInterval(async () => {
  if (!core || performance.now() - lastActivity > IDLE_MS) return;
  try {
    const { sbx } = await core;
    const s = sbx.currentSession();
    const left = s.startedAt.getTime() + s.timeout - Date.now();
    if (left < IDLE_MS) {
      await s.extendTimeout(IDLE_MS);
      log({ event: "extend", left_ms: left });
    }
  } catch (e) {
    log({ event: "extend_fail", error: String(e) });
  }
}, 15_000).unref?.();

// While clients are parked, watch for the core to run again.
setInterval(async () => {
  if (parked.size === 0) return;
  try {
    if (!oidc) return;
    const sbx = await Sandbox.get({ name: SHARD, resume: false, ...oidc });
    if (sbx.status === "running") {
      log({ event: "unpark", clients: parked.size, cause: "core_running" });
      for (const c of parked) c.close(RECONNECT, "core restarted");
      parked.clear();
    }
  } catch (e) {
    log({ event: "poll_fail", error: String(e) });
  }
}, POLL_MS).unref?.();

// The platform drops a WebSocket after about 5 minutes without frames, and
// the core's own pings stop at this hop, so ping clients here. Pongs are
// not counted as activity, so they don't keep the core awake.
setInterval(() => {
  for (const c of wss.clients) c.ping();
}, 30_000).unref?.();

const server = http.createServer(async (req, res) => {
  remember(req);
  try {
    const { base } = await wake("http");
    lastActivity = performance.now();
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const headers = { ...req.headers };
    delete headers.host;
    const up = await fetch(base + req.url, {
      method: req.method,
      headers,
      body: ["GET", "HEAD"].includes(req.method) ? undefined : Buffer.concat(chunks),
    });
    const out = Object.fromEntries(up.headers);
    delete out["content-encoding"];
    delete out["content-length"];
    res.writeHead(up.status, out);
    res.end(Buffer.from(await up.arrayBuffer()));
  } catch (e) {
    res.writeHead(502).end(String(e));
  }
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 * 1024 });
wss.on("connection", async (client, req) => {
  remember(req);
  const t0 = performance.now();
  const pending = [];
  let upstream = null;
  let up = 0;
  let down = 0;
  let cpu = process.cpuUsage();
  client.on("message", (data, isBinary) => {
    up++;
    lastActivity = performance.now();
    if (parked.has(client)) {
      // The core's session for this client is gone. Wake the core and let
      // the client reconnect; it re-sends whatever was not acknowledged.
      parked.delete(client);
      log({ event: "unpark", clients: 1, cause: "client_frame" });
      wake("parked_client").finally(() => client.close(RECONNECT, "core restarted"));
      return;
    }
    if (upstream?.readyState === WebSocket.OPEN) upstream.send(data, { binary: isBinary });
    else pending.push([data, isBinary]);
  });
  client.on("close", () => {
    parked.delete(client);
    const used = process.cpuUsage(cpu);
    log({
      event: "client_close",
      up,
      down,
      cpu_us: used.user + used.system,
      ms: Math.round(performance.now() - t0),
    });
    upstream?.close();
  });
  try {
    lastActivity = performance.now();
    const { base } = await wake("connect");
    const woke = performance.now() - t0;
    upstream = new WebSocket(base.replace(/^http/, "ws") + req.url, {
      maxPayload: 64 * 1024 * 1024,
    });
    upstream.on("open", () => {
      log({
        event: "ws_open",
        woke_ms: Math.round(woke),
        open_ms: Math.round(performance.now() - t0),
      });
      for (const [d, b] of pending.splice(0)) upstream.send(d, { binary: b });
    });
    upstream.on("message", (d, b) => {
      down++;
      if (client.readyState === WebSocket.OPEN) client.send(d, { binary: b });
    });
    upstream.on("close", (code) => {
      log({ event: "upstream_close", code, up, down });
      upstream = null;
      if (client.readyState !== WebSocket.OPEN) return;
      // The core stopped (idle timeout, rollover, crash): park the client
      // instead of closing it, so it doesn't wake the core by reconnecting.
      core = null;
      parked.add(client);
    });
    upstream.on("error", (e) => log({ event: "upstream_error", error: String(e) }));
  } catch (e) {
    log({ event: "ws_fail", error: String(e) });
    client.close(1011);
  }
});

export default server;
