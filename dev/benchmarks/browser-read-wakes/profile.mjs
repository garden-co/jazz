import { spawn, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";

const { values } = parseArgs({
  options: {
    "out-dir": { type: "string" },
    root: { type: "string" },
    storage: { type: "string", default: "persistent" },
    rows: { type: "string", default: "1500" },
    iterations: { type: "string", default: "10" },
    scheduling: { type: "string", default: "control" },
    cpu: { type: "boolean", default: false },
    trace: { type: "boolean", default: false },
    "vite-port": { type: "string", default: "4279" },
    "cdp-port": { type: "string", default: "9439" },
    help: { type: "boolean", default: false },
  },
});
if (values.help) {
  console.log(`Usage: node dev/benchmarks/browser-read-wakes/profile.mjs [options]
  --root PATH          Checkout whose own release WASM and TypeScript are profiled
  --out-dir PATH       Receipt, CPU profiles and Chrome trace output
  --storage TYPE       memory or persistent (default persistent)
  --rows N             Task rows sharing 15 folders (default 1500)
  --iterations N       Reads per measured lane after three warmups (default 10)
  --scheduling MODE    control, timer-polling, message-channel, or host-yield (default control)
  --cpu                Sample foreground and shared-worker CPU
  --trace              Capture Chrome timer/task events
  --vite-port N        Local Vite port (default 4279)
  --cdp-port N         Owned Chromium CDP port (default 9439)

Use release WASM built in this checkout with verified fingerprints.
The scheduling overrides are serial diagnostic experiments, not runtime fixes.
The host-yield experiment can time out on persistent coverage; that is evidence.
No Core server, network dataset, application credentials, or real data is used.`);
  process.exit(0);
}
const ROOT_DIR = path.resolve(values.root ?? fileURLToPath(new URL("../../..", import.meta.url)));
const JAZZ_TOOLS_DIR = path.join(ROOT_DIR, "packages/jazz-tools");
const storage = values.storage;
const count = Number(values.rows);
const repetitions = Number(values.iterations);
const scheduling = values.scheduling;
const cpuProfile = values.cpu;
const trace = values.trace;
const vitePort = Number(values["vite-port"]);
const cdpPort = Number(values["cdp-port"]);
const outDir = path.resolve(
  values["out-dir"] ?? path.join(ROOT_DIR, "target/browser-read-wakes", String(Date.now())),
);
if (!["memory", "persistent"].includes(storage)) throw new Error("Invalid storage");
if (!["control", "timer-polling", "message-channel", "host-yield"].includes(scheduling))
  throw new Error("Invalid scheduling experiment");
for (const [name, value] of Object.entries({ count, repetitions, vitePort, cdpPort }))
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid ${name}`);

async function resolveChromiumExecutable() {
  if (process.env.JAZZ_CHROMIUM_EXECUTABLE) {
    await access(process.env.JAZZ_CHROMIUM_EXECUTABLE);
    return process.env.JAZZ_CHROMIUM_EXECUTABLE;
  }
  const cacheDir = path.join(homedir(), "Library", "Caches", "ms-playwright");
  const entries = await readdir(cacheDir, { withFileTypes: true });
  const chromiumDirs = entries
    .filter((entry) => entry.isDirectory() && /^chromium-\d+$/.test(entry.name))
    .map((entry) => entry.name)
    .sort((a, b) => Number.parseInt(b.split("-")[1], 10) - Number.parseInt(a.split("-")[1], 10));

  for (const dir of chromiumDirs) {
    const candidate = path.join(
      cacheDir,
      dir,
      "chrome-mac-arm64",
      "Google Chrome for Testing.app",
      "Contents",
      "MacOS",
      "Google Chrome for Testing",
    );
    try {
      await access(candidate);
      return candidate;
    } catch {}
  }

  throw new Error(`Could not find Chromium executable under ${cacheDir}`);
}

async function resolveViteBinary() {
  const candidates = [
    path.join(JAZZ_TOOLS_DIR, "node_modules", ".bin", "vite"),
    path.join(ROOT_DIR, "packages", "inspector", "node_modules", ".bin", "vite"),
  ];

  for (const candidate of candidates) {
    try {
      await access(candidate);
      return candidate;
    } catch {}
  }

  throw new Error("Could not find a Vite binary in packages/jazz-tools or packages/inspector");
}

class CDPClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.nextId = 1;
    this.pending = new Map();
    this.listeners = new Set();
  }

  async connect() {
    this.ws = new WebSocket(this.wsUrl);
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("CDP connect timeout")), 10_000);
      this.ws.onopen = () => {
        clearTimeout(timeout);
        resolve();
      };
      this.ws.onerror = (event) => {
        clearTimeout(timeout);
        reject(new Error(`CDP websocket error: ${event?.message ?? "unknown"}`));
      };
    });

    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data.toString());
      if (msg.id) {
        const pending = this.pending.get(msg.id);
        if (!pending) return;
        this.pending.delete(msg.id);
        if (msg.error) pending.reject(new Error(`${pending.method}: ${msg.error.message}`));
        else pending.resolve(msg.result);
        return;
      }
      for (const listener of this.listeners) listener(msg);
    };

    this.ws.onclose = () => {
      for (const pending of this.pending.values()) {
        pending.reject(new Error(`CDP closed before response to ${pending.method}`));
      }
      this.pending.clear();
    };

    return this;
  }

  send(method, params = {}, sessionId) {
    const id = this.nextId++;
    const payload = { id, method, params };
    if (sessionId) payload.sessionId = sessionId;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, method });
      this.ws.send(JSON.stringify(payload));
    });
  }

  on(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  waitFor(check, timeoutMs = 10_000) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        off();
        reject(new Error("CDP event wait timeout"));
      }, timeoutMs);
      const off = this.on((msg) => {
        try {
          const result = check(msg);
          if (result) {
            clearTimeout(timeout);
            off();
            resolve(result);
          }
        } catch (error) {
          clearTimeout(timeout);
          off();
          reject(error);
        }
      });
    });
  }

  async close() {
    if (!this.ws) return;
    this.ws.close();
    await sleep(50);
  }
}

function summarizeProfile(profileData) {
  const nodeById = new Map(profileData.nodes.map((node) => [node.id, node]));
  const selfMicros = new Map();
  const samples = profileData.samples ?? [];
  const deltas = profileData.timeDeltas ?? [];

  for (let i = 0; i < samples.length; i += 1) {
    const nodeId = samples[i];
    const delta = deltas[i] ?? 0;
    selfMicros.set(nodeId, (selfMicros.get(nodeId) ?? 0) + delta);
  }

  return [...selfMicros.entries()]
    .map(([nodeId, micros]) => {
      const node = nodeById.get(nodeId);
      const frame = node?.callFrame ?? {};
      return {
        selfMs: micros / 1000,
        functionName: frame.functionName || "(anonymous)",
        url: frame.url || "(native)",
        line: (frame.lineNumber ?? 0) + 1,
      };
    })
    .sort((a, b) => b.selfMs - a.selfMs)
    .slice(0, 20);
}

function printSummary(title, entries) {
  console.log(`\n=== ${title} ===`);
  for (const entry of entries.slice(0, 15)) {
    console.log(`${entry.selfMs.toFixed(1)} ms  ${entry.functionName}  ${entry.url}:${entry.line}`);
  }
}

async function waitForJson(url, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return await res.json();
      lastError = new Error(`HTTP ${res.status}`);
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }
  throw lastError ?? new Error(`Timed out waiting for ${url}`);
}

async function waitForHttp(url, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }
  throw lastError ?? new Error(`Timed out waiting for ${url}`);
}

await mkdir(outDir, { recursive: true });
const manifest = JSON.parse(
  await readFile(path.join(ROOT_DIR, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json"), "utf8"),
);
const wasmSha256 = createHash("sha256")
  .update(await readFile(path.join(ROOT_DIR, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm")))
  .digest("hex");
if (manifest.kind !== "wasm" || manifest.profile !== "release")
  throw new Error("Profiling requires this checkout's release WASM");
for (const artifact of manifest.artifacts) {
  const actual = createHash("sha256")
    .update(await readFile(path.join(ROOT_DIR, "crates/jazz-wasm/pkg", artifact.file)))
    .digest("hex");
  if (actual !== artifact.sha256) throw new Error(`WASM artifact changed: ${artifact.file}`);
}
const revision = execFileSync("git", ["rev-parse", "HEAD"], {
  cwd: ROOT_DIR,
  encoding: "utf8",
}).trim();
const vite = spawn(
  await resolveViteBinary(),
  [
    "--config",
    "vitest.config.browser.ts",
    "--host",
    "127.0.0.1",
    "--port",
    String(vitePort),
    "--strictPort",
  ],
  {
    cwd: JAZZ_TOOLS_DIR,
    env: { ...process.env, JAZZ_ABSTRACT_BENCH: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  },
);
const logs = [];
vite.stdout.on("data", (x) => logs.push(x.toString()));
vite.stderr.on("data", (x) => logs.push(x.toString()));
let chrome, userDataDir, cdp;
try {
  const pageUrl = `http://127.0.0.1:${vitePort}/tests/browser/remote-db-harness.html`;
  await waitForHttp(pageUrl, 30000);
  userDataDir = await mkdtemp(path.join(tmpdir(), "jazz-include-profile-"));
  chrome = spawn(
    await resolveChromiumExecutable(),
    [
      `--remote-debugging-port=${cdpPort}`,
      `--user-data-dir=${userDataDir}`,
      "--headless=new",
      "--disable-background-timer-throttling",
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
      "--disable-extensions",
      "--no-first-run",
      "--no-default-browser-check",
      "about:blank",
    ],
    { stdio: "ignore" },
  );
  const { webSocketDebuggerUrl } = await waitForJson(`http://127.0.0.1:${cdpPort}/json/version`);
  cdp = await new CDPClient(webSocketDebuggerUrl).connect();
  const sessions = new Map();
  cdp.on((msg) => {
    if (msg.method === "Target.attachedToTarget")
      sessions.set(msg.params.sessionId, msg.params.targetInfo);
    if (msg.method === "Target.detachedFromTarget") sessions.delete(msg.params.sessionId);
  });
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId: pageId } = await cdp.send("Target.attachToTarget", {
    targetId,
    flatten: true,
  });
  sessions.set(pageId, { type: "page", url: pageUrl });
  await cdp.send("Page.enable", {}, pageId);
  await cdp.send("Runtime.enable", {}, pageId);
  await cdp.send(
    "Target.setAutoAttach",
    { autoAttach: true, waitForDebuggerOnStart: false, flatten: true },
    pageId,
  );
  const loaded = cdp.waitFor(
    (msg) => msg.sessionId === pageId && msg.method === "Page.loadEventFired",
    30000,
  );
  await cdp.send("Page.navigate", { url: pageUrl }, pageId);
  await loaded;
  const evaluate = async (expression) => {
    const r = await cdp.send(
      "Runtime.evaluate",
      { expression, awaitPromise: true, returnByValue: true },
      pageId,
    );
    if (r.exceptionDetails) throw new Error(JSON.stringify(r.exceptionDetails));
    return r.result?.value;
  };
  await evaluate(
    `globalThis.fixtureConfig=${JSON.stringify({ storage, count, repetitions, scheduling })}`,
  );
  await evaluate(await readFile(new URL("./fixture.js", import.meta.url), "utf8"));
  const { targetInfos } = await cdp.send("Target.getTargets");
  for (const info of targetInfos) {
    if (
      ["worker", "shared_worker", "service_worker"].includes(info.type) &&
      ![...sessions.values()].some((x) => x.targetId === info.targetId)
    ) {
      const { sessionId } = await cdp.send("Target.attachToTarget", {
        targetId: info.targetId,
        flatten: true,
      });
      sessions.set(sessionId, info);
    }
  }
  const setup = await evaluate("fixture.setup");
  console.log("ready", {
    storage,
    count,
    repetitions,
    targets: [...sessions.values()].map((x) => x.type),
  });
  const results = [];
  for (const included of [true, false]) {
    const lane = included ? "include" : "flat";
    const before = await evaluate(`fixture.run(${included},3)`);
    await evaluate(`fixture.check(${included})`);
    const targets = [...sessions].filter(([, info]) =>
      ["page", "worker", "shared_worker"].includes(info.type),
    );
    for (const [id] of cpuProfile ? targets : []) {
      await cdp.send("Profiler.enable", {}, id);
      await cdp.send("Profiler.setSamplingInterval", { interval: 250 }, id);
      await cdp.send("Profiler.start", {}, id);
    }
    const traceEvents = [];
    const offTrace = cdp.on((msg) => {
      if (msg.method === "Tracing.dataCollected") traceEvents.push(...msg.params.value);
    });
    if (trace)
      await cdp.send("Tracing.start", {
        categories: "devtools.timeline,v8.execute,blink.user_timing",
        transferMode: "ReportEvents",
      });
    const times = await evaluate(`fixture.run(${included},${repetitions})`);
    const profiles = [];
    for (const [id, info] of cpuProfile ? targets : []) {
      const { profile } = await cdp.send("Profiler.stop", {}, id);
      const file = `${storage}-${count}-${lane}-${info.type}-${profiles.length}.cpuprofile`;
      await writeFile(path.join(outDir, file), JSON.stringify(profile));
      const summary = summarizeProfile(profile);
      profiles.push({ file, type: info.type, summary });
      printSummary(`${lane} ${info.type}`, summary);
    }
    if (trace) {
      const traceDone = cdp.waitFor((msg) => msg.method === "Tracing.tracingComplete", 30000);
      await cdp.send("Tracing.end");
      await traceDone;
      await writeFile(path.join(outDir, `${lane}-trace.json`), JSON.stringify({ traceEvents }));
    }
    offTrace();
    await evaluate(`fixture.check(${included})`);
    results.push({ lane, before, measured: times, profiles });
    console.log("measured", { lane, before, measured: times });
  }
  await evaluate("fixture.close()");
  await writeFile(
    path.join(outDir, "receipt.json"),
    JSON.stringify(
      {
        revision,
        browser: await cdp.send("Browser.getVersion"),
        host: { platform: process.platform, architecture: process.arch, node: process.version },
        wasmSha256,
        manifest,
        storage,
        count,
        repetitions,
        scheduling,
        cpuProfile,
        trace,
        setup,
        scriptSha256: createHash("sha256")
          .update(await readFile(new URL(import.meta.url)))
          .digest("hex"),
        fixtureSha256: createHash("sha256")
          .update(await readFile(new URL("./fixture.js", import.meta.url)))
          .digest("hex"),
        adapterSha256: createHash("sha256")
          .update(
            await readFile(
              path.join(JAZZ_TOOLS_DIR, "src/runtime/native-runtime/native-runtime-adapter.ts"),
            ),
          )
          .digest("hex"),
        results,
      },
      null,
      2,
    ),
  );
} catch (error) {
  await writeFile(
    path.join(outDir, "failure.json"),
    JSON.stringify(
      { revision, wasmSha256, storage, count, repetitions, scheduling, error: String(error) },
      null,
      2,
    ),
  );
  throw error;
} finally {
  cdp?.ws?.close();
  chrome?.kill("SIGTERM");
  vite.kill("SIGTERM");
  await writeFile(path.join(outDir, "vite.log"), logs.join(""));
  if (userDataDir) {
    await sleep(500);
    await rm(userDataDir, { recursive: true, force: true }).catch(() => {});
  }
}
