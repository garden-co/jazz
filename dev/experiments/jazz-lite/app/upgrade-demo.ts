/**
 * Browser harness for the lite → full upgrade. Starts on Jazz Lite, renders a
 * subscription, and exposes hooks for a driver to trigger and observe the swap.
 * Full Jazz is reached only through the dynamic import below.
 */
import { createLiteDb } from "../src/client.js";
import { createUpgradableDb } from "../src/upgrade.js";
import { app, type Todo } from "./schema.js";

const params = new URLSearchParams(location.search);
const token = params.get("token") ?? "";
const appId = params.get("appId") ?? "";
const serverUrl = params.get("server") ?? "";
const t0 = performance.now();
const marks: Record<string, number> = {};
const mark = (name: string) => (marks[name] ??= Math.round(performance.now() - t0));
const results: Array<{ at: number; rows: number; mode: string }> = [];

const lite = createLiteDb({ url: params.get("proxy") ?? "", getToken: () => token });
let owner = "";
void lite.ready().then(() => (owner = lite.accountId ?? ""));
const db = createUpgradableDb({
  lite,
  loadFull: async () => {
    mark("fullImportStart");
    const jazz = await import("jazz-tools");
    mark("fullImported");
    const store = {
      async read() {
        return localStorage.getItem("jazz-lite-account");
      },
      async update(transform: (value: string | null) => string | null) {
        const next = transform(localStorage.getItem("jazz-lite-account"));
        if (next === null) localStorage.removeItem("jazz-lite-account");
        else localStorage.setItem("jazz-lite-account", next);
      },
    };
    const accounts = await jazz.createAccountManager({ appId, serverUrl, env: "lite-demo", store });
    const account = accounts.getSnapshot?.().account ?? (await accounts.loginJWT(token));
    mark("fullAccount");
    const full = await jazz.createDb({ appId, serverUrl, env: "lite-demo", account });
    mark("fullDbReady");
    return full;
  },
});

const list = document.getElementById("list")!;
db.subscribe(app.todos, (rows: Todo[]) => {
  mark("firstResult");
  results.push({ at: Math.round(performance.now() - t0), rows: rows.length, mode: db.mode });
  list.textContent = rows.map((t) => t.title).join("\n");
});

Object.assign(window, {
  __jazzLite: {
    db,
    marks,
    results,
    async upgrade() {
      const timings = await db.upgrade();
      mark("handedOver");
      return { timings, handovers: db.handovers };
    },
    insert: (title: string) => db.insert(app.todos, { title, done: false, owner_id: owner }),
  },
});
