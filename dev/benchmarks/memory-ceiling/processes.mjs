// Processes the memory-ceiling harness owns: the shipped `jazz-tools server`
// binary, and forked Jazz client workers. Every process it starts gets a
// pidfile, and the harness only ever signals the pids it wrote.
import { fork, spawn } from "node:child_process";
import { createWriteStream, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { join } from "node:path";

const here = new URL("./", import.meta.url);

/** Resident and peak resident set of a process, in bytes, from /proc. */
export function memoryOf(pid) {
  try {
    const status = readFileSync(`/proc/${pid}/status`, "utf8");
    const kb = (key) => Number(new RegExp(`^${key}:\\s+(\\d+) kB`, "m").exec(status)?.[1] ?? 0);
    return { rss: kb("VmRSS") * 1024, hwm: kb("VmHWM") * 1024 };
  } catch {
    return { rss: 0, hwm: 0 };
  }
}

/** User plus system CPU seconds a process has used, from /proc. */
export function cpuSecondsOf(pid) {
  try {
    const fields = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
    return (Number(fields[11]) + Number(fields[12])) / 100;
  } catch {
    return 0;
  }
}

/** Samples a process's RSS until stopped; `peak` is the highest seen. */
export function sampleMemory(pid, intervalMs = 50, onSample) {
  let peak = 0;
  const timer = setInterval(() => {
    const { rss } = memoryOf(pid);
    if (rss > peak) peak = rss;
    onSample?.(rss);
  }, intervalMs);
  return {
    get peak() {
      return Math.max(peak, memoryOf(pid).rss);
    },
    stop() {
      clearInterval(timer);
      return this.peak;
    },
  };
}

export async function freePort() {
  return await new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Start the server binary the way tenant pods run it
 * (`jazz-tools server <appId> --data-dir …`, MIMALLOC_ARENA_RESERVE=0), with
 * heap sampling at its shipped default unless overridden.
 */
export async function startServer({
  bin,
  appId,
  dataDir,
  outDir,
  adminSecret,
  backendSecret,
  env,
}) {
  const port = await freePort();
  const log = createWriteStream(join(outDir, "server.log"), { flags: "a" });
  const child = spawn(bin, ["server", appId, "--data-dir", dataDir, "--port", String(port)], {
    env: {
      ...process.env,
      NODE_ENV: "production",
      RUST_LOG: process.env.RUST_LOG ?? "warn",
      JAZZ_ADMIN_SECRET: adminSecret,
      JAZZ_BACKEND_SECRET: backendSecret,
      MIMALLOC_ARENA_RESERVE: "0",
      ...env,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.pipe(log);
  child.stderr.pipe(log);
  const pidfile = join(outDir, "server.pid");
  writeFileSync(pidfile, String(child.pid));
  let exit = null;
  const exited = new Promise((resolve) =>
    child.once("exit", (code, signal) => {
      exit = { code, signal };
      rmSync(pidfile, { force: true });
      resolve(exit);
    }),
  );
  const url = `http://127.0.0.1:${port}`;
  const started = Date.now();
  for (;;) {
    if (exit) throw new Error(`server exited during startup: ${JSON.stringify(exit)}`);
    try {
      const res = await fetch(`${url}/health`);
      if (res.ok) break;
    } catch {
      // not listening yet
    }
    if (Date.now() - started > 120_000) throw new Error("server did not become healthy");
    await sleep(50);
  }
  return {
    pid: child.pid,
    url,
    readyMs: Date.now() - started,
    get exit() {
      return exit;
    },
    exited,
    memory: () => memoryOf(child.pid),
    cpuSeconds: () => cpuSecondsOf(child.pid),
    /** The server's sampled heap profile (gzipped pprof), or null. */
    async heapProfile() {
      try {
        const res = await fetch(`${url}/debug/pprof/heap`, {
          headers: { "X-Jazz-Admin-Secret": adminSecret },
        });
        if (!res.ok) return null;
        return Buffer.from(await res.arrayBuffer());
      } catch {
        return null;
      }
    },
    async stop() {
      if (exit) return exit;
      child.kill("SIGTERM");
      const timeout = sleep(30_000).then(() => "timeout");
      if ((await Promise.race([exited, timeout])) === "timeout") child.kill("SIGKILL");
      return await exited;
    },
  };
}

/** Fork a Jazz client worker (worker.mjs) and talk to it by request/response. */
export async function startWorker({ name, outDir, init, packagesDir }) {
  const log = createWriteStream(join(outDir, `${name}.log`), { flags: "a" });
  const child = fork(new URL("worker.mjs", here), [], {
    execArgv: ["--import", new URL("register.mjs", here).href, "--no-warnings"],
    env: { ...process.env, ...(packagesDir ? { JAZZ_MEMCEIL_PACKAGES: packagesDir } : {}) },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  child.stdout.pipe(log);
  child.stderr.pipe(log);
  const pidfile = join(outDir, `${name}.pid`);
  writeFileSync(pidfile, String(child.pid));
  let nextId = 1;
  const pending = new Map();
  let exit = null;
  const ready = new Promise((resolve, reject) => {
    child.on("message", (message) => {
      if (message.ready) return resolve();
      const waiter = pending.get(message.id);
      if (!waiter) return;
      pending.delete(message.id);
      if (message.ok) waiter.resolve(message.result);
      else waiter.reject(new Error(`${name}: ${message.error}`));
    });
    child.once("exit", (code, signal) => {
      exit = { code, signal };
      rmSync(pidfile, { force: true });
      const error = new Error(`${name} exited (${code ?? signal})`);
      reject(error);
      for (const waiter of pending.values()) waiter.reject(error);
      pending.clear();
    });
  });
  const call = (message) =>
    new Promise((resolve, reject) => {
      if (exit) return reject(new Error(`${name} already exited`));
      const id = nextId++;
      pending.set(id, { resolve, reject });
      child.send({ ...message, id });
    });
  await ready;
  await call({ op: "init", ...init });
  return {
    pid: child.pid,
    call,
    memory: () => memoryOf(child.pid),
    async stop() {
      if (exit) return;
      const done = new Promise((resolve) => child.once("exit", resolve));
      child.send({ op: "exit", id: 0 });
      const timeout = sleep(10_000).then(() => "timeout");
      if ((await Promise.race([done, timeout])) === "timeout") child.kill("SIGKILL");
    },
  };
}
