// Owns the shard core: one persistent Sandbox per shard, with the shard's
// SQLite on a read-write Drive and the jazz-tools binary on a read-only
// snapshot of another Drive.
import { Drive, Sandbox } from "@vercel/sandbox";

export const APP_ID = process.env.SPIKE_APP_ID ?? "00000000-0000-0000-0000-00000000f00d";
const PORT = 1625;

export async function drive(name, maxGiB = 10, creds = {}) {
  return Drive.getOrCreate({ name, region: "iad1", maxSize: maxGiB * 1024 ** 3, ...creds });
}

function since(t0) {
  return Math.round(performance.now() - t0);
}

async function healthy(base, timeoutMs) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    try {
      const r = await fetch(`${base}/health`, { signal: AbortSignal.timeout(2000) });
      if (r.ok) return true;
    } catch {}
    // Short gap: every 10 ms a waiting client spends here is wake latency.
    await new Promise((r) => setTimeout(r, 10));
  }
  return false;
}

/** Start the server process inside a running session. */
async function startServer(sbx, exe, shell = true) {
  if (!shell) {
    // Images without a shell: run the binary directly. Its output stays in
    // the Sandbox's command log.
    return sbx.runCommand({
      cmd: exe,
      args: ["server", APP_ID, "--port", String(PORT), "--data-dir", "/data/jazz"],
      env: { JAZZ_ADMIN_SECRET: process.env.SPIKE_ADMIN_SECRET, RUST_LOG: "info" },
      detached: true,
    });
  }
  return sbx.runCommand({
    cmd: "bash",
    args: [
      "-lc",
      `mkdir -p /data/jazz && exec ${exe} server ${APP_ID} --port ${PORT} --data-dir /data/jazz >>/data/server.log 2>&1`,
    ],
    env: { JAZZ_ADMIN_SECRET: process.env.SPIKE_ADMIN_SECRET, RUST_LOG: "info" },
    detached: true,
  });
}

/**
 * Resume (or create) the shard's sandbox and make sure the server answers.
 * Returns the public base URL and where the time went. `creds` ({ token,
 * teamId, projectId }) is needed when called outside a request.
 *
 * With `image`, the Sandbox boots from that VCR image, which carries the
 * binary at /usr/local/bin/jazz-tools; otherwise it boots the default image
 * and reads the binary from the `bin` Drive.
 */
export async function ensureCore({
  name,
  data,
  bin = "jazz-bin",
  vcpus = 1,
  timeoutMs = 3_600_000,
  creds = {},
  image,
  shell = true,
}) {
  const exe = image ? "/usr/local/bin/jazz-tools" : "/opt/jazz/jazz-tools";
  // Mount the data Drive by name: it exists already, and looking it up
  // first would add a round trip to every wake.
  const mounts = { "/data": { drive: data, mode: "read-write" } };
  if (!image) mounts["/opt/jazz"] = { drive: bin, mode: "snapshot" };
  const t0 = performance.now();
  // Wall-clock marks, to line up with the server's own startup timeline.
  const epochs = { call: Date.now() };
  const timings = {};
  let started = false;
  let command = null;
  const sbx = await Sandbox.getOrCreate({
    name,
    region: "iad1",
    ports: [PORT],
    resources: { vcpus },
    timeout: timeoutMs,
    persistent: true,
    keepLastSnapshots: { count: 1 },
    ...(image ? { image } : {}),
    mounts,
    resume: true,
    ...creds,
    onResume: async (s) => {
      timings.resumed = since(t0);
      epochs.resumed = Date.now();
      command = await startServer(s, exe, shell);
      epochs.spawned = Date.now();
      started = true;
    },
  });
  timings.handle = since(t0);
  const base = sbx.domain(PORT);
  // A session that was already running keeps its server; a fresh one
  // (created, not resumed) still needs it started.
  if (!started && !(await healthy(base, 300))) {
    epochs.resumed = Date.now();
    command = await startServer(sbx, exe, shell);
    epochs.spawned = Date.now();
    started = true;
  }
  timings.spawned = since(t0);
  const ok = await healthy(base, 60_000);
  timings.healthy = since(t0);
  epochs.healthy = Date.now();
  return {
    sbx,
    base,
    ok,
    started,
    timings,
    epochs,
    command,
    session: sbx.currentSession?.()?.sessionId,
  };
}

/**
 * Read the server's startup timeline (`jazz_server::startup` lines) from
 * its command log, for at most `waitMs`.
 */
export async function startupTimeline(command, waitMs = 3000) {
  const steps = {};
  let main = null;
  const signal = AbortSignal.timeout(waitMs);
  let buffer = "";
  try {
    for await (const log of command.logs({ signal })) {
      buffer += log.data;
      let newline;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline).replace(/\x1b\[[0-9;]*m/g, "");
        buffer = buffer.slice(newline + 1);
        const m = line.match(
          /step="?([a-z_.]+)"? us=(\d+) main_epoch_us=(\d+) main_uptime_ms=(\d+)/,
        );
        if (!m) continue;
        steps[m[1]] = Number(m[2]);
        main = { epoch_ms: Number(m[3]) / 1000, uptime_ms: Number(m[4]) };
      }
      if ("bound" in steps) break;
    }
  } catch {}
  return { steps, main };
}
