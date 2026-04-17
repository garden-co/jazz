import { spawn } from "node:child_process";
import { access, mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import jazzNapi from "../../crates/jazz-napi/index.js";

const { TestingServer } = jazzNapi;

const DEFAULT_SCENARIOS = ["w4", "b2"];
const DEFAULT_OUT_DIR = "/tmp/jazz-browser-profiles";
const DEFAULT_VITE_PORT = 4173;
const DEFAULT_CDP_PORT = 9333;
const DEFAULT_SCALE = 0.03;
const DEFAULT_LARGE_MULTIPLIER = 4;

const ROOT_DIR = path.resolve(new URL("../..", import.meta.url).pathname);
const JAZZ_TOOLS_DIR = path.join(ROOT_DIR, "packages", "jazz-tools");
const PROFILE_PATH = path.join(ROOT_DIR, "benchmarks", "realistic", "profiles", "s.json");
const SCHEMA_PATH = path.join(
  ROOT_DIR,
  "benchmarks",
  "realistic",
  "schema",
  "project_board.schema.json",
);
const B5_SCENARIO_PATH = path.join(
  ROOT_DIR,
  "benchmarks",
  "realistic",
  "scenarios",
  "b5_server_permission_recursive.json",
);

function parseArgs(argv) {
  const args = {
    scenarios: [...DEFAULT_SCENARIOS],
    outDir: DEFAULT_OUT_DIR,
    vitePort: DEFAULT_VITE_PORT,
    cdpPort: DEFAULT_CDP_PORT,
    scale: DEFAULT_SCALE,
    largeMultiplier: DEFAULT_LARGE_MULTIPLIER,
    keepArtifacts: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    switch (arg) {
      case "--scenario":
      case "--scenarios":
        args.scenarios = next
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
        i += 1;
        break;
      case "--out-dir":
        args.outDir = next;
        i += 1;
        break;
      case "--vite-port":
        args.vitePort = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--cdp-port":
        args.cdpPort = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--scale":
        args.scale = Number.parseFloat(next);
        i += 1;
        break;
      case "--large-multiplier":
        args.largeMultiplier = Number.parseInt(next, 10);
        i += 1;
        break;
      case "--keep-artifacts":
        args.keepArtifacts = true;
        break;
      case "--help":
        printHelp();
        process.exit(0);
      default:
        throw new Error(`Unknown arg: ${arg}`);
    }
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node benchmarks/realistic/profile_browser.mjs [options]

Options:
  --scenario w4,b2,b3,b5   Comma-separated scenario list (default: ${DEFAULT_SCENARIOS.join(",")})
  --out-dir PATH           Where to write .cpuprofile files (default: ${DEFAULT_OUT_DIR})
  --vite-port N            Vite dev-server port (default: ${DEFAULT_VITE_PORT})
  --cdp-port N             Chromium DevTools port (default: ${DEFAULT_CDP_PORT})
  --scale FLOAT            Dataset scale for small profiles (default: ${DEFAULT_SCALE})
  --large-multiplier N     Multiplier for B3 large profile (default: ${DEFAULT_LARGE_MULTIPLIER})
  --keep-artifacts         Keep Chromium user-data dir on exit
`);
}

function jsLiteral(value) {
  return JSON.stringify(value);
}

async function resolveChromiumExecutable() {
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

function scaledProfile(input, scale) {
  const tasks = Math.max(100, Math.floor(input.tasks * scale));
  const comments = Math.max(tasks, Math.floor(input.comments * scale));
  const activity_events = Math.max(tasks, Math.floor(input.activity_events * scale));
  return { ...input, tasks, comments, activity_events };
}

function scaledLargeProfile(input, scale, multiplier) {
  const base = scaledProfile(input, scale);
  const factor = Math.max(1, Math.floor(multiplier));
  const tasks = Math.min(4000, base.tasks * factor);
  const comments = Math.min(16000, Math.max(tasks, base.comments * factor));
  const activity_events = Math.min(12000, Math.max(tasks, base.activity_events * factor));
  return { ...base, id: `${base.id}_L`, tasks, comments, activity_events };
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

function buildInitScript(schemaTables) {
  return String.raw`
    (async () => {
      const [{ createDb }, { loadWasmModule }, { generateAuthSecret }] = await Promise.all([
        import("/src/runtime/db.ts"),
        import("/src/runtime/client.js"),
        import("/src/runtime/auth-secret-store.js"),
      ]);
      const schema = ${jsLiteral(schemaTables)};
      function tableProxy(table, tableSchema = schema) {
        return { _table: table, _schema: tableSchema, _rowType: {}, _initType: {} };
      }
      function query(table, conditions = [], orderBy = [], limit, querySchema = schema) {
        return {
          _table: table,
          _schema: querySchema,
          _rowType: {},
          _build() {
            return JSON.stringify({ table, conditions, includes: {}, orderBy, limit, offset: 0 });
          },
        };
      }
      function nowMicros() { return Date.now() * 1000; }
      class Lcg {
        constructor(seed) {
          this.state = (BigInt(seed >>> 0) | 1n);
        }
        nextU64() {
          this.state =
            (this.state * 6364136223846793005n + 1442695040888963407n) & ((1n << 64n) - 1n);
          return this.state;
        }
        nextInt(upper) {
          if (upper <= 1) return 0;
          return Number(this.nextU64() % BigInt(upper));
        }
      }
      async function createServerDb(options) {
        const config = {
          appId: options.appId,
          dbName: options.dbName,
          serverUrl: options.serverUrl,
          adminSecret: options.includeAdminSecret === false ? undefined : options.adminSecret,
          logLevel: "warn",
        };
        if (options.includeJwt !== false && options.jwtToken) {
          config.jwtToken = options.jwtToken;
        }
        if (options.localFirstSecret) {
          config.auth = { localFirstSecret: options.localFirstSecret };
        }
        return createDb(config);
      }
      function permissionRecursiveSchema(recursiveDepth) {
        const folderSelectPolicy = {
          using: {
            type: "Or",
            exprs: [
              {
                type: "Cmp",
                column: "owner_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["user_id"] },
              },
              {
                type: "And",
                exprs: [
                  { type: "IsNotNull", column: "parent_id" },
                  {
                    type: "Inherits",
                    operation: "Select",
                    via_column: "parent_id",
                    max_depth: recursiveDepth,
                  },
                ],
              },
            ],
          },
        };
        const folderUpdatePolicy = {
          using: {
            type: "Or",
            exprs: [
              {
                type: "Cmp",
                column: "owner_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["user_id"] },
              },
              {
                type: "And",
                exprs: [
                  { type: "IsNotNull", column: "parent_id" },
                  {
                    type: "Inherits",
                    operation: "Update",
                    via_column: "parent_id",
                    max_depth: recursiveDepth,
                  },
                ],
              },
            ],
          },
          with_check: {
            type: "Or",
            exprs: [
              {
                type: "Cmp",
                column: "owner_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["user_id"] },
              },
              {
                type: "And",
                exprs: [
                  { type: "IsNotNull", column: "parent_id" },
                  {
                    type: "Inherits",
                    operation: "Update",
                    via_column: "parent_id",
                    max_depth: recursiveDepth,
                  },
                ],
              },
            ],
          },
        };
        const documentSelectPolicy = {
          using: {
            type: "Inherits",
            operation: "Select",
            via_column: "folder_id",
            max_depth: recursiveDepth,
          },
        };
        const documentUpdatePolicy = {
          using: {
            type: "And",
            exprs: [
              {
                type: "Cmp",
                column: "editor_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["user_id"] },
              },
              {
                type: "Inherits",
                operation: "Update",
                via_column: "folder_id",
                max_depth: recursiveDepth,
              },
            ],
          },
          with_check: {
            type: "And",
            exprs: [
              {
                type: "Cmp",
                column: "editor_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["user_id"] },
              },
              {
                type: "Inherits",
                operation: "Update",
                via_column: "folder_id",
                max_depth: recursiveDepth,
              },
            ],
          },
        };
        return {
          folders: {
            columns: [
              { name: "parent_id", column_type: { type: "Uuid" }, nullable: true, references: "folders" },
              { name: "owner_id", column_type: { type: "Text" }, nullable: false },
              { name: "title", column_type: { type: "Text" }, nullable: false },
              { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
            ],
            policies: {
              select: folderSelectPolicy,
              insert: {},
              update: folderUpdatePolicy,
              delete: {},
            },
          },
          documents: {
            columns: [
              { name: "folder_id", column_type: { type: "Uuid" }, nullable: false, references: "folders" },
              { name: "editor_id", column_type: { type: "Text" }, nullable: false },
              { name: "body", column_type: { type: "Text" }, nullable: false },
              { name: "revision", column_type: { type: "Integer" }, nullable: false },
              { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
            ],
            policies: {
              select: documentSelectPolicy,
              insert: {},
              update: documentUpdatePolicy,
              delete: {},
            },
          },
        };
      }
      async function seedPermissionDataset(db, scenario, permissionSchema, owners) {
        const folderTable = tableProxy("folders", permissionSchema);
        const documentTable = tableProxy("documents", permissionSchema);
        const rng = new Lcg(scenario.seed);
        const totalFolders = Math.max(4, scenario.folders);
        const totalDocuments = Math.max(20, scenario.documents);
        const allowedFolders = [];
        const deniedFolders = [];
        const ts = nowMicros();
        const allowedRoot = await db.insertDurable(folderTable, {
          parent_id: null,
          owner_id: owners.allowedOwnerId,
          title: "allowed-root",
          updated_at: ts,
        }, { tier: "worker" });
        const deniedRoot = await db.insertDurable(folderTable, {
          parent_id: null,
          owner_id: owners.deniedOwnerId,
          title: "denied-root",
          updated_at: ts + 1,
        }, { tier: "worker" });
        allowedFolders.push(allowedRoot.id);
        deniedFolders.push(deniedRoot.id);
        for (let i = 2; i < totalFolders; i += 1) {
          const allowedChain = i % 2 === 0;
          const parent = allowedChain
            ? allowedFolders[allowedFolders.length - 1]
            : deniedFolders[deniedFolders.length - 1];
          const row = await db.insertDurable(folderTable, {
            parent_id: parent,
            owner_id: allowedChain ? owners.allowedOwnerId : owners.deniedOwnerId,
            title: "folder-" + i,
            updated_at: ts + i,
          }, { tier: "worker" });
          if (allowedChain) allowedFolders.push(row.id);
          else deniedFolders.push(row.id);
        }
        const allowThreshold = Math.max(1, Math.min(99, Math.round(scenario.allow_fraction * 100)));
        for (let i = 0; i < totalDocuments; i += 1) {
          const useAllowed = rng.nextInt(100) < allowThreshold;
          const folderList = useAllowed ? allowedFolders : deniedFolders;
          const folderId = folderList[rng.nextInt(folderList.length)];
          const allowAllowedUserWrite = rng.nextInt(100) < allowThreshold;
          const editorId = useAllowed
            ? (allowAllowedUserWrite ? owners.allowedOwnerId : owners.intermediateOwnerId)
            : owners.deniedOwnerId;
          await db.insertDurable(documentTable, {
            folder_id: folderId,
            editor_id: editorId,
            body: "doc-" + i,
            revision: 0,
            updated_at: ts + 10000 + i,
          }, { tier: "worker" });
        }
      }
      async function seedDataset(db, config) {
        const usersTable = tableProxy("users");
        const organizationsTable = tableProxy("organizations");
        const membershipsTable = tableProxy("memberships");
        const projectsTable = tableProxy("projects");
        const tasksTable = tableProxy("tasks");
        const commentsTable = tableProxy("task_comments");
        const taskWatchersTable = tableProxy("task_watchers");
        const activityTable = tableProxy("activity_events");
        const users = [];
        const organizations = [];
        const projects = [];
        const taskIds = [];
        const taskProjectIdx = [];
        const commentsPerTask = new Array(config.tasks).fill(0);
        const ts = nowMicros();

        for (let i = 0; i < config.users; i += 1) {
          const row = await db.insert(usersTable, {
            display_name: "User " + i,
            email: "user" + i + "@bench.test",
          });
          users.push(row.id);
        }
        for (let i = 0; i < config.organizations; i += 1) {
          const row = await db.insert(organizationsTable, {
            name: "Org " + i,
            created_at: ts + i,
          });
          organizations.push(row.id);
        }
        for (let i = 0; i < config.users; i += 1) {
          await db.insert(membershipsTable, {
            organization_id: organizations[i % organizations.length],
            user_id: users[i],
            role: i % 9 === 0 ? "admin" : "member",
          });
        }
        for (let i = 0; i < config.projects; i += 1) {
          const row = await db.insert(projectsTable, {
            organization_id: organizations[i % organizations.length],
            name: "Project " + i,
            archived: false,
            updated_at: ts + i,
          });
          projects.push(row.id);
        }
        const statuses = ["todo", "in_progress", "review", "done"];
        for (let i = 0; i < config.tasks; i += 1) {
          const projectIdx = i % projects.length;
          const assigneeIdx = i % users.length;
          const row = await db.insert(tasksTable, {
            project_id: projects[projectIdx],
            title: "Task " + i,
            status: statuses[i % statuses.length],
            priority: 1 + (i % 4),
            assignee_id: users[assigneeIdx],
            updated_at: ts + i,
            due_at: ts + i * 11,
          });
          taskIds.push(row.id);
          taskProjectIdx.push(projectIdx);
        }
        for (let i = 0; i < config.comments; i += 1) {
          const taskIdx = i % taskIds.length;
          await db.insert(commentsTable, {
            task_id: taskIds[taskIdx],
            author_id: users[(i * 7) % users.length],
            body: "Comment " + i + " body",
            created_at: ts + i,
          });
          commentsPerTask[taskIdx] += 1;
        }
        for (let taskIdx = 0; taskIdx < taskIds.length; taskIdx += 1) {
          for (let w = 0; w < config.watchers_per_task; w += 1) {
            await db.insert(taskWatchersTable, {
              task_id: taskIds[taskIdx],
              user_id: users[(taskIdx + w) % users.length],
            });
          }
        }
        for (let i = 0; i < config.activity_events; i += 1) {
          const taskIdx = i % taskIds.length;
          await db.insert(activityTable, {
            project_id: projects[taskProjectIdx[taskIdx]],
            task_id: taskIds[taskIdx],
            actor_id: users[(i * 11) % users.length],
            kind: i % 3 === 0 ? "task_updated" : "comment_added",
            created_at: ts + i,
            payload: JSON.stringify({ event: i }),
          });
        }
        const hotProjectCount = Math.max(1, Math.round(config.projects * config.hot_project_fraction));
        return { users, projects, taskIds, hotProjectCount };
      }
      globalThis.__profileHarness = {
        createDb,
        createServerDb,
        query,
        seedDataset,
        schema,
        loadWasmModule,
        generateAuthSecret,
        permissionRecursiveSchema,
        seedPermissionDataset,
      };
      return true;
    })()
  `;
}

function buildW4Setup(config, dbName) {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const cfg = ${jsLiteral(config)};
      const dbName = ${jsLiteral(dbName)};
      const db = await h.createDb({ appId: "profile-w4-app", dbName, logLevel: "warn" });
      const state = await h.seedDataset(db, cfg);
      await db.all(
        h.query("tasks", [{ column: "project_id", op: "eq", value: state.projects[0] }], [["updated_at", "desc"]], 200),
        { tier: "worker" }
      );
      await db.shutdown();
      globalThis.__w4Profile = { dbName, hotProjectId: state.projects[0] };
      return true;
    })()
  `;
}

function buildW4Run() {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const { dbName, hotProjectId } = globalThis.__w4Profile;
      const t0 = performance.now();
      const db = await h.createDb({ appId: "profile-w4-app", dbName, logLevel: "warn" });
      const rows = await db.all(
        h.query("tasks", [{ column: "project_id", op: "eq", value: hotProjectId }], [["updated_at", "desc"]], 200),
        { tier: "worker" }
      );
      globalThis.__w4Profile.db = db;
      return { rows: rows.length, elapsedMs: performance.now() - t0 };
    })()
  `;
}

function buildW4Cleanup() {
  return `(async () => { const db = globalThis.__w4Profile?.db; if (db) await db.shutdown(); return true; })()`;
}

function buildB2Setup(config, serverInfo, dbName) {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const cfg = ${jsLiteral(config)};
      const server = ${jsLiteral(serverInfo)};
      const db = await h.createServerDb(server.appId, ${jsLiteral(dbName)}, server.jwtToken, server.serverUrl, server.adminSecret);
      const state = await h.seedDataset(db, cfg);
      globalThis.__b2Profile = { db, state };
      return { users: state.users.length, tasks: state.taskIds.length };
    })()
  `;
}

function buildB2Run() {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const { db, state } = globalThis.__b2Profile;
      const t0 = performance.now();
      for (let i = 0; i < 200; i += 1) {
        const assignee = state.users[i % state.users.length];
        await db.all(
          h.query("tasks", [
            { column: "assignee_id", op: "eq", value: assignee },
            { column: "status", op: "eq", value: "in_progress" },
          ], [["updated_at", "desc"]], 200)
        );
      }
      return { elapsedMs: performance.now() - t0 };
    })()
  `;
}

function buildB2Cleanup() {
  return `(async () => { const db = globalThis.__b2Profile?.db; if (db) await db.shutdown(); return true; })()`;
}

function buildB3Setup(config, serverInfo, dbName) {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const cfg = ${jsLiteral(config)};
      const server = ${jsLiteral(serverInfo)};
      const dbName = ${jsLiteral(dbName)};
      const seedDb = await h.createServerDb(server.appId, dbName, server.jwtToken, server.serverUrl, server.adminSecret);
      const state = await h.seedDataset(seedDb, cfg);
      const hotProjectId = state.projects[0];
      await seedDb.all(
        h.query("tasks", [{ column: "project_id", op: "eq", value: hotProjectId }], [["updated_at", "desc"]], 200)
      );
      await seedDb.shutdown();
      globalThis.__b3Profile = { dbName, hotProjectId, server };
      return { hotProjectId };
    })()
  `;
}

function buildB3Run() {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const { dbName, hotProjectId, server } = globalThis.__b3Profile;
      const t0 = performance.now();
      const db = await h.createServerDb(server.appId, dbName, server.jwtToken, server.serverUrl, server.adminSecret);
      const rows = await db.all(
        h.query("tasks", [{ column: "project_id", op: "eq", value: hotProjectId }], [["updated_at", "desc"]], 200)
      );
      globalThis.__b3Profile.db = db;
      return { rows: rows.length, elapsedMs: performance.now() - t0 };
    })()
  `;
}

function buildB3Cleanup() {
  return `(async () => { const db = globalThis.__b3Profile?.db; if (db) await db.shutdown(); return true; })()`;
}

function buildB5Setup(scenario, serverInfo, dbPrefix) {
  return `
    (async () => {
      const h = globalThis.__profileHarness;
      const scenario = ${jsLiteral(scenario)};
      const server = ${jsLiteral(serverInfo)};
      const dbPrefix = ${jsLiteral(dbPrefix)};
      const permissionSchema = h.permissionRecursiveSchema(Math.max(1, scenario.recursive_depth));
      const seedLocalSecret = h.generateAuthSecret();
      const allowedLocalSecret = h.generateAuthSecret();
      const intermediateLocalSecret = h.generateAuthSecret();
      const wasmModule = await h.loadWasmModule();
      const allowedPrincipalId = wasmModule.WasmRuntime.deriveUserId(allowedLocalSecret);
      const deniedPrincipalId = wasmModule.WasmRuntime.deriveUserId(h.generateAuthSecret());
      const intermediatePrincipalId = wasmModule.WasmRuntime.deriveUserId(intermediateLocalSecret);
      const seedDb = await h.createServerDb({
        appId: server.appId,
        dbName: dbPrefix + "-seed",
        serverUrl: server.serverUrl,
        adminSecret: server.adminSecret,
        includeJwt: false,
        localFirstSecret: seedLocalSecret,
      });
      await h.seedPermissionDataset(seedDb, scenario, permissionSchema, {
        allowedOwnerId: allowedPrincipalId,
        deniedOwnerId: deniedPrincipalId,
        intermediateOwnerId: intermediatePrincipalId,
      });
      const allowedDb = await h.createServerDb({
        appId: server.appId,
        dbName: dbPrefix + "-allowed",
        serverUrl: server.serverUrl,
        adminSecret: server.adminSecret,
        includeAdminSecret: false,
        includeJwt: false,
        localFirstSecret: allowedLocalSecret,
      });
      const visibleDocumentsQuery = h.query(
        "documents",
        [],
        [["updated_at", "desc"]],
        1000,
        permissionSchema
      );
      let warmAllowedVisible = 0;
      const allowedSession = {
        user_id: allowedPrincipalId,
        claims: { auth_mode: "local-first" },
      };
      const unsubscribe = allowedDb.subscribeAll(
        visibleDocumentsQuery,
        (delta) => {
          warmAllowedVisible = delta.all.length;
        },
        undefined,
        allowedSession,
      );
      const warmupDeadline = performance.now() + 30000;
      while (performance.now() < warmupDeadline) {
        if (warmAllowedVisible > 0) break;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      if (warmAllowedVisible <= 0) {
        throw new Error("Timed out warming B5 allowed documents");
      }
      globalThis.__b5Profile = {
        seedDb,
        allowedDb,
        permissionSchema,
        visibleDocumentsQuery,
        unsubscribe,
      };
      return { warmAllowedVisible };
    })()
  `;
}

function buildB5Run() {
  return `
    (async () => {
      const { allowedDb, visibleDocumentsQuery } = globalThis.__b5Profile;
      const t0 = performance.now();
      const rows = await allowedDb.all(visibleDocumentsQuery);
      return { rows: rows.length, elapsedMs: performance.now() - t0 };
    })()
  `;
}

function buildB5Cleanup() {
  return `
    (async () => {
      const state = globalThis.__b5Profile;
      if (!state) return true;
      if (state.unsubscribe) state.unsubscribe();
      if (state.seedDb) await state.seedDb.shutdown();
      if (state.allowedDb) await state.allowedDb.shutdown();
      return true;
    })()
  `;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = args.outDir;
  await mkdir(outDir, { recursive: true });

  const profile = JSON.parse(await readFile(PROFILE_PATH, "utf8"));
  const schemaTables = JSON.parse(await readFile(SCHEMA_PATH, "utf8")).tables;
  const b5Scenario = JSON.parse(await readFile(B5_SCENARIO_PATH, "utf8"));

  const smallProfile = scaledProfile(profile, args.scale);
  const largeProfile = scaledLargeProfile(profile, args.scale, args.largeMultiplier);
  const now = Date.now();

  const viteBinary = await resolveViteBinary();
  const vite = spawn(
    viteBinary,
    [
      "--config",
      "vitest.config.browser.ts",
      "--host",
      "127.0.0.1",
      "--port",
      String(args.vitePort),
      "--strictPort",
    ],
    {
      cwd: JAZZ_TOOLS_DIR,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let testingServer;
  let chrome;
  let userDataDir = null;
  const pageUrl = `http://127.0.0.1:${args.vitePort}/tests/browser/remote-db-harness.html`;

  try {
    const viteReady = waitForHttp(pageUrl, 15_000);
    const viteStderr = [];
    vite.stderr.on("data", (chunk) => {
      viteStderr.push(chunk.toString());
    });
    await viteReady;

    const chromeExecutable = await resolveChromiumExecutable();
    userDataDir = await mkdtemp(path.join(tmpdir(), "jazz-browser-profile-"));
    chrome = spawn(
      chromeExecutable,
      [
        `--remote-debugging-port=${args.cdpPort}`,
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

    const server = await TestingServer.start();
    testingServer = server;
    const serverInfo = {
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      jwtToken: await server.jwtForUser("browser-profile-user", {}),
    };

    const { webSocketDebuggerUrl } = await waitForJson(
      `http://127.0.0.1:${args.cdpPort}/json/version`,
    );
    const cdp = await new CDPClient(webSocketDebuggerUrl).connect();
    const sessions = new Map();
    const activeProfileTargets = new Map();
    let pageSessionId = null;
    let profilingEnabled = false;

    async function startProfilerForSession(sessionId, meta) {
      if (activeProfileTargets.has(sessionId)) return;
      try {
        await cdp.send("Profiler.enable", {}, sessionId);
        await cdp
          .send("Profiler.setSamplingInterval", { interval: 100 }, sessionId)
          .catch(() => {});
        await cdp.send("Profiler.start", {}, sessionId);
        activeProfileTargets.set(sessionId, meta);
      } catch (error) {
        console.warn("failed to start profiler", meta, error.message);
      }
    }

    cdp.on(async (msg) => {
      if (msg.method === "Target.attachedToTarget") {
        const { sessionId, targetInfo } = msg.params;
        sessions.set(sessionId, targetInfo);
        if (targetInfo.type === "worker") {
          await cdp.send("Runtime.enable", {}, sessionId).catch(() => {});
          if (profilingEnabled) {
            await startProfilerForSession(sessionId, { kind: "worker", url: targetInfo.url });
          }
        }
      }
      if (msg.method === "Target.detachedFromTarget") {
        sessions.delete(msg.params.sessionId);
      }
    });

    const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
    pageSessionId = sessionId;
    sessions.set(pageSessionId, { type: "page", url: pageUrl, targetId });
    await cdp.send("Page.enable", {}, pageSessionId).catch(() => {});
    await cdp.send("Runtime.enable", {}, pageSessionId).catch(() => {});
    await cdp.send("Target.setAutoAttach", {
      autoAttach: true,
      waitForDebuggerOnStart: false,
      flatten: true,
    });
    await cdp.send(
      "Target.setAutoAttach",
      {
        autoAttach: true,
        waitForDebuggerOnStart: false,
        flatten: true,
      },
      pageSessionId,
    );
    await cdp.send("Page.navigate", { url: pageUrl }, pageSessionId);
    await cdp.waitFor(
      (msg) =>
        msg.sessionId === pageSessionId && msg.method === "Page.loadEventFired" ? true : null,
      10_000,
    );

    async function evalPage(expression) {
      const result = await cdp.send(
        "Runtime.evaluate",
        {
          expression,
          awaitPromise: true,
          returnByValue: true,
        },
        pageSessionId,
      );
      if (result.exceptionDetails) {
        throw new Error(result.exceptionDetails.text || "page evaluate failed");
      }
      return result.result?.value;
    }

    await evalPage(buildInitScript(schemaTables));

    async function withScenarioProfile(name, setupExpr, runExpr, cleanupExpr) {
      activeProfileTargets.clear();
      profilingEnabled = false;
      await evalPage(setupExpr);
      profilingEnabled = true;
      await startProfilerForSession(pageSessionId, { kind: "page", url: pageUrl });
      for (const [attachedSessionId, info] of sessions) {
        if (attachedSessionId === pageSessionId) continue;
        if (info.type === "worker") {
          await startProfilerForSession(attachedSessionId, { kind: "worker", url: info.url });
        }
      }

      const result = await evalPage(runExpr);
      const stopped = [];
      for (const [attachedSessionId, meta] of [...activeProfileTargets.entries()]) {
        try {
          const profileResult = await cdp.send("Profiler.stop", {}, attachedSessionId);
          const file = path.join(outDir, `${name}-${meta.kind}-${stopped.length}.cpuprofile`);
          await writeFile(file, JSON.stringify(profileResult.profile));
          stopped.push({ meta, file, summary: summarizeProfile(profileResult.profile) });
        } catch (error) {
          stopped.push({ meta, error: error.message });
        }
      }
      profilingEnabled = false;
      if (cleanupExpr) await evalPage(cleanupExpr);

      console.log(`\nSCENARIO ${name}:`, result);
      for (const item of stopped) {
        if (item.summary) printSummary(`${name} ${item.meta.kind}`, item.summary);
        else console.log(`failed to stop ${name} ${item.meta.kind}: ${item.error}`);
      }
    }

    for (const scenario of args.scenarios) {
      if (scenario === "w4") {
        await withScenarioProfile(
          "w4",
          buildW4Setup(smallProfile, `profile-w4-${now}`),
          buildW4Run(),
          buildW4Cleanup(),
        );
      } else if (scenario === "b2") {
        await withScenarioProfile(
          "b2",
          buildB2Setup(smallProfile, serverInfo, `profile-b2-${now}`),
          buildB2Run(),
          buildB2Cleanup(),
        );
      } else if (scenario === "b3") {
        await withScenarioProfile(
          "b3",
          buildB3Setup(largeProfile, serverInfo, `profile-b3-${now}`),
          buildB3Run(),
          buildB3Cleanup(),
        );
      } else if (scenario === "b5") {
        await withScenarioProfile(
          "b5",
          buildB5Setup(b5Scenario, serverInfo, `profile-b5-${now}`),
          buildB5Run(),
          buildB5Cleanup(),
        );
      } else {
        throw new Error(`Unsupported scenario: ${scenario}`);
      }
    }

    await cdp.close();
  } finally {
    if (testingServer) {
      try {
        await testingServer.stop();
      } catch {}
    }
    if (chrome) {
      chrome.kill("SIGKILL");
    }
    if (vite) {
      vite.kill("SIGKILL");
    }
    if (userDataDir && !args.keepArtifacts) {
      await rm(userDataDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
