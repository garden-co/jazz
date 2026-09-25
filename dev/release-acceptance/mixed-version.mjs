#!/usr/bin/env node
// Mixed-version wire acceptance: runs real `jazz-tools server` binaries and
// real Node clients from two installed package versions against each other.
//
//   node dev/release-acceptance/mixed-version.mjs <config.json>
//
// config.json (keep outside the repository):
//   {
//     "output": "/abs/new/output-dir",
//     "versions": {
//       "old": { "project": "/abs/project-with-jazz-tools@current", "cli": "/abs/jazz-tools-binary" },
//       "new": { "project": "/abs/project-with-jazz-tools@candidate", "cli": "/abs/jazz-tools-binary" }
//     },
//     "only": ["cell-name", ...],  // optional
//     "skipLarge": false           // optional: skip the 800KB value checks
//   }
//
// Each `project` is an external npm project with `jazz-tools` (and its native
// payload) installed; each `cli` is the matching native `jazz-tools` binary.
// Every cell uses a fresh server data dir and fresh client stores. Results go
// to <output>/results.json and one JSON line per check on stdout.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { spawn } from "node:child_process";
import {
  readFileSync,
  writeFileSync,
  mkdirSync,
  existsSync,
  openSync,
  closeSync,
  unlinkSync,
  realpathSync,
} from "node:fs";
import { createRequire } from "node:module";
import { createInterface } from "node:readline";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { createProcessOwner } from "./process-owner.mjs";

const ownDir = dirname(fileURLToPath(import.meta.url));
const input = JSON.parse(readFileSync(process.argv[2], "utf8"));
const output = resolve(input.output);
assert(!existsSync(output), "output must be a new directory");
mkdirSync(output, { recursive: true, mode: 0o700 });
const V = {};
for (const key of ["old", "new"]) {
  const v = input.versions?.[key];
  assert(v?.project && v?.cli, `versions.${key}.project and .cli are required`);
  const project = realpathSync(v.project);
  const require = createRequire(join(project, "package.json"));
  const pkg = JSON.parse(
    readFileSync(join(project, "node_modules", "jazz-tools", "package.json"), "utf8"),
  );
  V[key] = {
    key,
    project,
    cli: realpathSync(v.cli),
    cliJs: join(dirname(require.resolve("jazz-tools")), "cli.js"),
    version: pkg.version,
  };
}

const processes = createProcessOwner();
const results = [];
function record(cell, check, status, detail = {}) {
  const entry = { cell, check, status, ...detail };
  results.push(entry);
  console.log(JSON.stringify(entry));
}

const FIXTURE_SCHEMA = `import {schema as s} from 'jazz-tools';
export const app=s.defineApp({docs:s.table({label:s.string(),body:s.string(),author:s.string()},{})});
export default app;
`;
const FIXTURE_PERMISSIONS = `import {schema as s} from 'jazz-tools';
import {app} from './schema';
export default s.definePermissions(app,({policy})=>{policy.docs.allowRead.always();policy.docs.allowInsert.always();policy.docs.allowUpdate.always();policy.docs.allowDelete.always();});
`;

function launch(dir, command, args, log, env = {}, cwd) {
  const fd = openSync(join(dir, log), "a", 0o600);
  const child = processes.spawn(command, args, {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", fd, fd],
  });
  closeSync(fd);
  return child;
}

async function run(dir, command, args, log, env, cwd, ms = 120000) {
  const child = launch(dir, command, args, log, env, cwd);
  const timer = setTimeout(() => void processes.stop(child), ms);
  const [code, signal] = await once(child, "exit");
  clearTimeout(timer);
  return { code, signal, log: readFileSync(join(dir, log), "utf8") };
}

class Server {
  constructor(cell, dir, ctx) {
    Object.assign(this, { cell, dir, ctx });
    this.dataDir = join(dir, "server-data");
    this.port = 0;
  }
  async start(version, { upstreamUrl, log = "server.log" } = {}) {
    const portPath = join(this.dir, "port");
    if (existsSync(portPath)) unlinkSync(portPath);
    this.version = version;
    const args = [
      "server",
      this.ctx.appId,
      "--port",
      String(this.port),
      "--data-dir",
      this.dataDir,
      "--bound-port-file",
      portPath,
      "--allow-local-first-auth",
    ];
    if (upstreamUrl) args.push("--upstream-url", upstreamUrl);
    this.child = launch(this.dir, version.cli, args, log, {
      NODE_ENV: "production",
      JAZZ_ADMIN_SECRET: this.ctx.adminSecret,
      JAZZ_BACKEND_SECRET: this.ctx.backendSecret,
    });
    for (let i = 0; i < 400; i++) {
      if (this.child.exitCode !== null)
        throw new Error(
          `${version.key} server exited (${this.child.exitCode}): ${readFileSync(join(this.dir, log), "utf8").slice(-2000)}`,
        );
      if (existsSync(portPath)) {
        const port = Number(readFileSync(portPath, "utf8").trim());
        if (port) {
          this.url = `http://127.0.0.1:${port}`;
          try {
            const r = await fetch(`${this.url}/health`, {
              signal: AbortSignal.timeout(1000),
            });
            if (r.status < 500) {
              this.port = port; // restarts reuse the port so clients reconnect in place
              return this.url;
            }
          } catch {}
        }
      }
      await delay(50);
    }
    throw new Error("server readiness timeout");
  }
  async stop() {
    if (this.child) await processes.stop(this.child);
    this.child = undefined;
  }
}

class Client {
  constructor(cell, dir, version, name) {
    Object.assign(this, { cell, version, name, next: 0, pending: new Map() });
    const fd = openSync(join(dir, `client-${name}.stderr.log`), "a", 0o600);
    this.child = processes.spawn(
      process.execPath,
      [join(ownDir, "mixed-version-client.mjs"), version.project, join(dir, `client-${name}`)],
      { stdio: ["pipe", "pipe", fd] },
    );
    closeSync(fd);
    createInterface({ input: this.child.stdout }).on("line", (line) => {
      let msg;
      try {
        msg = JSON.parse(line);
      } catch {
        return; // library logging on stdout
      }
      this.pending.get(msg.rpc)?.(msg);
      this.pending.delete(msg.rpc);
    });
    this.child.on("exit", () => {
      this.exited = true;
      for (const done of this.pending.values()) done({ ok: false, error: `client ${name} exited` });
      this.pending.clear();
    });
  }
  call(op, args = {}) {
    const rpc = ++this.next;
    if (this.exited)
      return Promise.reject(
        new Error(`[${this.version.key} client ${this.name}] ${op}: client exited`),
      );
    return new Promise((resolve, reject) => {
      this.pending.set(rpc, (msg) =>
        msg.ok
          ? resolve(msg.value)
          : reject(new Error(`[${this.version.key} client ${this.name}] ${op}: ${msg.error}`)),
      );
      this.child.stdin.write(`${JSON.stringify({ rpc, op, ...args })}\n`);
    });
  }
  async close() {
    if (this.closed) return;
    this.closed = true;
    try {
      await this.call("close", { ms: 10000 });
    } catch {}
    this.child.stdin.end();
    await processes.stop(this.child);
  }
}

// Runs `fn` as one named check; failures are recorded and do not abort the cell
// unless `fatal` is set.
async function check(cell, name, fn, { fatal = false } = {}) {
  const started = Date.now();
  try {
    const detail = (await fn()) ?? {};
    record(cell, name, "pass", { ms: Date.now() - started, ...detail });
    return true;
  } catch (error) {
    record(cell, name, "fail", {
      ms: Date.now() - started,
      error: String(error?.message ?? error).slice(0, 4000),
    });
    if (fatal) throw error;
    return false;
  }
}

async function deploy(cell, dir, server, deployer) {
  const fixture = join(deployer.project, `mixed-version-fixture-${randomUUID()}`);
  mkdirSync(fixture);
  writeFileSync(join(fixture, "schema.ts"), FIXTURE_SCHEMA);
  writeFileSync(join(fixture, "permissions.ts"), FIXTURE_PERMISSIONS);
  const r = await run(
    dir,
    process.execPath,
    [
      deployer.cliJs,
      "deploy",
      server.ctx.appId,
      "--schema-dir",
      fixture,
      "--server-url",
      server.url,
    ],
    `deploy-${deployer.key}.log`,
    { JAZZ_ADMIN_SECRET: server.ctx.adminSecret },
    deployer.project,
  );
  if (r.code !== 0)
    throw new Error(`deploy with ${deployer.key} CLI failed: ${r.log.slice(-2000)}`);
}

function newCell(name) {
  const dir = join(output, name);
  mkdirSync(dir, { mode: 0o700 });
  const ctx = {
    appId: randomUUID(),
    adminSecret: randomUUID(),
    backendSecret: randomUUID(),
  };
  return { name, dir, ctx };
}

/**
 * Core sync matrix: server `sv`, clients A (`va`) and B (`vb`). Covers schema
 * deploy, account registration, writes at global tier, remote point reads,
 * subscriptions in both directions (insert/update/delete), a large chunked
 * value, disconnect + offline write + reconnect, and a server restart.
 */
async function syncCell(name, sv, va, vb, { deployer = sv, upgradeTo } = {}) {
  const cell = newCell(name);
  const server = new Server(name, cell.dir, cell.ctx);
  const clients = [];
  try {
    await check(
      name,
      "server-start",
      async () => ({ url: await server.start(sv), server: sv.version }),
      { fatal: true },
    );
    const deployed = await check(name, `deploy-with-${deployer.key}-cli`, () =>
      deploy(name, cell.dir, server, deployer),
    );
    if (!deployed && deployer !== sv)
      await check(
        name,
        `deploy-with-${sv.key}-cli-fallback`,
        () => deploy(name, cell.dir, server, sv),
        { fatal: true },
      );
    else if (!deployed) throw new Error("deploy failed");

    const A = new Client(name, cell.dir, va, "a");
    const B = new Client(name, cell.dir, vb, "b");
    clients.push(A, B);
    const open = (c) =>
      c.call("open", {
        name: c.name,
        appId: cell.ctx.appId,
        serverUrl: server.url,
        ms: 30000,
      });
    await check(name, `open-a-${va.key}`, async () => open(A), { fatal: true });
    await check(name, `open-b-${vb.key}`, async () => open(B), { fatal: true });
    await check(name, "subscribe-both", async () => {
      await A.call("subscribe", { sub: "all", tier: "global" });
      await B.call("subscribe", { sub: "all", tier: "global" });
    });

    const ids = {};
    await check(name, "a-insert-global->b-read+sub", async () => {
      ids.r1 = (
        await A.call("insert", {
          values: { label: "r1", body: "from-a", author: va.key },
          wait: "global",
        })
      ).id;
      const row = await B.call("one", { id: ids.r1, tier: "global" });
      assert.equal(row?.body, "from-a");
      return {
        sub: await B.call("expectSub", {
          sub: "all",
          id: ids.r1,
          body: "from-a",
        }),
      };
    });
    await check(name, "b-update-global->a-read+sub", async () => {
      await B.call("update", {
        id: ids.r1,
        values: { body: "edited-by-b" },
        wait: "global",
      });
      const row = await A.call("one", { id: ids.r1, tier: "global" });
      assert.equal(row?.body, "edited-by-b");
      return {
        sub: await A.call("expectSub", {
          sub: "all",
          id: ids.r1,
          body: "edited-by-b",
        }),
      };
    });
    await check(name, "b-insert->a-delete->b-sub-sees-removal", async () => {
      ids.r2 = (
        await B.call("insert", {
          values: { label: "r2", body: "to-delete", author: vb.key },
          wait: "global",
        })
      ).id;
      await A.call("expectSub", { sub: "all", id: ids.r2, body: "to-delete" });
      await A.call("delete", { id: ids.r2, wait: "global" });
      await B.call("expectSub", { sub: "all", id: ids.r2, absent: true });
      assert.equal(await B.call("one", { id: ids.r2, tier: "global" }), null);
      assert.equal((await B.call("one", { id: ids.r1, tier: "global" }))?.body, "edited-by-b");
    });
    if (!input.skipLarge)
      await check(name, "a-large-800KB-value->b", async () => {
        const body = "L".repeat(800_000);
        ids.big = (
          await A.call("insert", {
            values: { label: "big", body, author: va.key },
            wait: "global",
            ms: 60000,
          })
        ).id;
        const row = await B.call("one", {
          id: ids.big,
          tier: "global",
          ms: 60000,
        });
        assert.equal(row?.body?.length, body.length);
        assert.equal(row.body, body);
      });
    if (!input.skipLarge)
      await check(name, "b-large-800KB-value->a", async () => {
        const body = "M".repeat(800_000);
        const id = (
          await B.call("insert", {
            values: { label: "big2", body, author: vb.key },
            wait: "global",
            ms: 60000,
          })
        ).id;
        const row = await A.call("one", { id, tier: "global", ms: 60000 });
        assert.equal(row?.body, body);
      });
    for (const [c, v] of [
      [A, va],
      [B, vb],
    ]) {
      if (v.key !== "old") continue;
      // alpha.56 apps may still use the retired "edge" durability name.
      await check(name, `${c.name}-legacy-edge-tier-write+read`, async () => {
        const id = (
          await c.call("insert", {
            values: { label: "edge", body: "edge-tier", author: v.key },
            wait: "edge",
          })
        ).id;
        const other = c === A ? B : A;
        await other.call("expectSub", { sub: "all", id, body: "edge-tier" });
        assert.equal((await c.call("one", { id, tier: "edge" }))?.body, "edge-tier");
      });
    }
    await check(name, "a-offline-write->reconnect->b-sees", async () => {
      await A.call("disconnect");
      ids.r3 = (
        await A.call("insert", {
          values: { label: "r3", body: "offline", author: va.key },
          wait: "local",
        })
      ).id;
      await delay(500);
      assert.equal(await B.call("one", { id: ids.r3, tier: "global" }), null);
      await A.call("reconnect");
      await B.call("expectSub", { sub: "all", id: ids.r3, body: "offline" });
      assert.equal((await B.call("one", { id: ids.r3, tier: "global" }))?.body, "offline");
    });
    await check(name, "b-offline-write->reconnect->a-sees", async () => {
      await B.call("disconnect");
      const id = (
        await B.call("insert", {
          values: { label: "r3b", body: "offline-b", author: vb.key },
          wait: "local",
        })
      ).id;
      await B.call("reconnect");
      await A.call("expectSub", { sub: "all", id, body: "offline-b" });
    });

    const freshClient = async (cname, v, expectId) => {
      const C = new Client(name, cell.dir, v, cname);
      clients.push(C);
      await C.call("open", {
        name: cname,
        appId: cell.ctx.appId,
        serverUrl: server.url,
        ms: 30000,
      });
      const point = await C.call("one", {
        id: ids.r1,
        tier: "global",
        ms: 30000,
      });
      assert.equal(point?.body, "edited-by-b", "fresh client point read");
      await C.call("subscribe", { sub: "all", tier: "global" });
      await C.call("expectSub", { sub: "all", id: expectId, ms: 45000 });
      await C.call("expectSub", { sub: "all", id: ids.r2, absent: true });
      await C.close();
      return { client: v.version };
    };
    await check(name, "fresh-client-before-restart", () => freshClient("c0", va, ids.r3));

    // Server bounce (same version), or an in-place upgrade of the server binary
    // on the same data dir and port: the rolling-deploy case.
    const next = upgradeTo ?? sv;
    const label = upgradeTo ? `server-upgrade-${sv.key}-to-${upgradeTo.key}` : "server-restart";
    await check(name, label, async () => {
      await server.stop();
      await delay(300);
      // Write while the server is down: must be delivered after it returns.
      ids.r4 = (
        await A.call("insert", {
          values: { label: "r4", body: "while-down", author: va.key },
          wait: "local",
        })
      ).id;
      await server.start(next, { log: `server-${next.key}-restart.log` });
      return { server: next.version };
    });
    await check(name, `${label}:pending-write-delivered`, async () => {
      await B.call("expectSub", {
        sub: "all",
        id: ids.r4,
        body: "while-down",
        ms: 45000,
      });
      assert.equal(
        (await B.call("one", { id: ids.r4, tier: "global", ms: 30000 }))?.body,
        "while-down",
      );
    });
    await check(name, `${label}:history-readable`, async () => {
      assert.equal(
        (await A.call("one", { id: ids.r1, tier: "global", ms: 30000 }))?.body,
        "edited-by-b",
      );
      assert.equal(await A.call("one", { id: ids.r2, tier: "global" }), null);
      if (ids.big)
        assert.equal(
          (await A.call("one", { id: ids.big, tier: "global", ms: 60000 }))?.label,
          "big",
        );
    });
    await check(name, `${label}:b-write->a-sub`, async () => {
      const id = (
        await B.call("insert", {
          values: { label: "r5", body: "after-restart", author: vb.key },
          wait: "global",
          ms: 30000,
        })
      ).id;
      await A.call("expectSub", { sub: "all", id, body: "after-restart" });
    });
    await check(name, "fresh-client-after-restart", () => freshClient("c", vb, ids.r4));
  } catch (error) {
    record(name, "cell-aborted", "fail", {
      error: String(error?.message ?? error).slice(0, 4000),
    });
  } finally {
    for (const c of clients) await c.close();
    await server.stop();
  }
}

/**
 * Large values vs. fresh subscribers: a client that subscribes after a large
 * row already exists must receive its first result. Isolates the size
 * threshold and whether a server restart changes the outcome.
 */
async function largeValueCell(name, sv, writer, reader) {
  const cell = newCell(name);
  const server = new Server(name, cell.dir, cell.ctx);
  const clients = [];
  try {
    await check(
      name,
      "server-start",
      async () => ({ url: await server.start(sv), server: sv.version }),
      { fatal: true },
    );
    await check(name, "deploy", () => deploy(name, cell.dir, server, sv), {
      fatal: true,
    });
    const open = async (v, cname) => {
      const c = new Client(name, cell.dir, v, cname);
      clients.push(c);
      await c.call("open", {
        name: cname,
        appId: cell.ctx.appId,
        serverUrl: server.url,
        ms: 30000,
      });
      return c;
    };
    const W = await open(writer, "w");
    const sizes = input.largeSizes ?? [60_000, 70_000, 800_000];
    const ids = [];
    const probe = async (label, size, id) => {
      await check(name, `${label}-${size}B`, async () => {
        const R = await open(reader, `r-${label}-${size}`);
        await R.call("subscribe", { sub: "all", tier: "global" });
        await R.call("expectSub", { sub: "all", id, ms: 20000 });
        await R.close();
      });
    };
    for (const size of sizes) {
      const id = (
        await W.call("insert", {
          values: {
            label: `size-${size}`,
            body: "Z".repeat(size),
            author: writer.key,
          },
          wait: "global",
          ms: 60000,
        })
      ).id;
      ids.push([size, id]);
      await probe("fresh-subscriber", size, id);
    }
    await check(name, "server-restart", async () => {
      await server.stop();
      await server.start(sv, { log: "server-restart.log" });
    });
    for (const [size, id] of ids) await probe("fresh-subscriber-after-restart", size, id);
  } catch (error) {
    record(name, "cell-aborted", "fail", {
      error: String(error?.message ?? error).slice(0, 4000),
    });
  } finally {
    for (const c of clients) await c.close();
    await server.stop();
  }
}

/** Legacy server-edge topologies. Edges are removed on the candidate. */
async function edgeCells() {
  {
    const name = "edge-old-behind-new-core";
    const cell = newCell(name);
    const core = new Server(name, cell.dir, cell.ctx);
    const edgeDir = join(cell.dir, "edge");
    mkdirSync(edgeDir);
    const edge = new Server(name, edgeDir, cell.ctx);
    const clients = [];
    try {
      await check(name, "core-start", async () => ({ url: await core.start(V.new) }), {
        fatal: true,
      });
      await check(name, "deploy", () => deploy(name, cell.dir, core, V.new), {
        fatal: true,
      });
      let edgeUp = false;
      await check(name, "old-edge-start", async () => {
        await edge.start(V.old, { upstreamUrl: core.url, log: "edge.log" });
        edgeUp = true;
      });
      if (edgeUp) {
        await check(name, "old-client-via-old-edge-write-global", async () => {
          const A = new Client(name, cell.dir, V.old, "a");
          clients.push(A);
          await A.call("open", {
            name: "a",
            appId: cell.ctx.appId,
            serverUrl: edge.url,
            ms: 20000,
          });
          await A.call("insert", {
            values: { label: "e", body: "via-edge", author: "old" },
            wait: "global",
            ms: 15000,
          });
        });
      }
      await delay(500);
      const logs = ["edge.log"]
        .map((l) => (existsSync(join(edgeDir, l)) ? readFileSync(join(edgeDir, l), "utf8") : ""))
        .join("\n");
      const coreLog = readFileSync(join(cell.dir, "server.log"), "utf8");
      const hints = [...(logs + "\n" + coreLog).split("\n")]
        .filter((l) => /edge|role|handshake|hello|reject|unsupported/i.test(l))
        .slice(0, 20);
      record(name, "log-evidence", "info", { lines: hints });
    } finally {
      for (const c of clients) await c.close();
      await edge.stop();
      await core.stop();
    }
  }
  {
    const name = "edge-new-cli-refuses-upstream";
    const cell = newCell(name);
    const edge = new Server(name, cell.dir, cell.ctx);
    await check(name, "new-cli-with-upstream-url-fails-explicitly", async () => {
      try {
        await edge.start(V.new, { upstreamUrl: "http://127.0.0.1:9" });
      } catch (error) {
        const message = String(error.message);
        assert.match(message, /edge|upstream/i);
        return { message: message.slice(0, 400) };
      } finally {
        await edge.stop();
      }
      throw new Error("candidate server accepted --upstream-url");
    });
  }
}

const CELLS = {
  "baseline-old-server-old-clients": () =>
    syncCell("baseline-old-server-old-clients", V.old, V.old, V.old),
  "candidate-new-server-new-clients": () =>
    syncCell("candidate-new-server-new-clients", V.new, V.new, V.new),
  "new-server-old+new-clients": () =>
    syncCell("new-server-old+new-clients", V.new, V.old, V.new, {
      deployer: V.old,
    }),
  "new-server-new+old-clients": () => syncCell("new-server-new+old-clients", V.new, V.new, V.old),
  "old-server-new+old-clients": () =>
    syncCell("old-server-new+old-clients", V.old, V.new, V.old, {
      deployer: V.new,
    }),
  "old-server-old+new-clients": () => syncCell("old-server-old+new-clients", V.old, V.old, V.new),
  "rolling-upgrade-old-to-new-server": () =>
    syncCell("rolling-upgrade-old-to-new-server", V.old, V.old, V.new, {
      upgradeTo: V.new,
    }),
  "rolling-upgrade-old-clients-only": () =>
    syncCell("rolling-upgrade-old-clients-only", V.old, V.old, V.old, {
      upgradeTo: V.new,
    }),
  "large-values-new-server-new-clients": () =>
    largeValueCell("large-values-new-server-new-clients", V.new, V.new, V.new),
  "large-values-new-server-old-writer-old-reader": () =>
    largeValueCell("large-values-new-server-old-writer-old-reader", V.new, V.old, V.old),
  "large-values-old-server-old-clients": () =>
    largeValueCell("large-values-old-server-old-clients", V.old, V.old, V.old),
  edge: edgeCells,
};

const watchdog = setTimeout(() => {
  console.error("whole-run deadline exceeded");
  void processes.terminate(1);
}, 30 * 60_000);
try {
  record("run", "versions", "info", {
    old: { version: V.old.version, cli: V.old.cli },
    new: { version: V.new.version, cli: V.new.cli },
  });
  for (const [name, cell] of Object.entries(CELLS)) {
    if (input.only && !input.only.includes(name)) continue;
    await cell();
  }
} finally {
  clearTimeout(watchdog);
  writeFileSync(join(output, "results.json"), JSON.stringify(results, null, 2));
  const failed = results.filter((r) => r.status === "fail");
  console.log(
    JSON.stringify({
      summary: { checks: results.length, failed: failed.length },
    }),
  );
  await processes.cleanup();
  processes.dispose();
  process.exitCode = failed.length ? 1 : 0;
}
