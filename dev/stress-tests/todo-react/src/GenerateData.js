import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState, useCallback } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";
const TOTAL_PROJECTS = 10000;
const TOTAL_TODOS = 50000;
const BATCH_SIZE = 500;
const YIELD_EVERY_BATCHES = 10;
const ADJECTIVES = [
  "quick",
  "lazy",
  "sleepy",
  "noisy",
  "hungry",
  "brave",
  "calm",
  "eager",
  "fierce",
  "gentle",
  "happy",
  "jolly",
  "kind",
  "lively",
  "mighty",
  "neat",
  "proud",
  "quiet",
  "rapid",
  "sharp",
  "tough",
  "vast",
  "warm",
  "zany",
  "ancient",
  "bold",
  "clever",
  "dark",
  "elegant",
  "frozen",
  "golden",
  "hidden",
  "iron",
  "jade",
  "keen",
  "lunar",
  "molten",
  "noble",
  "obsidian",
  "parallel",
  "radiant",
  "silent",
  "twisted",
  "urgent",
  "violet",
  "wicked",
  "xenial",
  "young",
];
const NOUNS = [
  "fox",
  "dog",
  "cat",
  "owl",
  "bee",
  "ant",
  "elk",
  "emu",
  "yak",
  "bat",
  "hawk",
  "wolf",
  "bear",
  "deer",
  "frog",
  "goat",
  "hare",
  "ibis",
  "jay",
  "kite",
  "lynx",
  "mole",
  "newt",
  "orca",
  "puma",
  "quail",
  "rook",
  "seal",
  "toad",
  "vole",
  "wren",
  "zebu",
  "crab",
  "dove",
  "finch",
  "gecko",
  "heron",
  "iguana",
  "jackal",
  "koala",
  "lemur",
  "moose",
  "narwhal",
  "osprey",
  "parrot",
  "raven",
  "squid",
  "tapir",
  "urchin",
  "viper",
];
const VERBS = [
  "build",
  "craft",
  "debug",
  "edit",
  "fix",
  "grow",
  "hack",
  "index",
  "join",
  "keep",
  "link",
  "merge",
  "nest",
  "open",
  "parse",
  "query",
  "render",
  "sync",
  "test",
  "undo",
  "view",
  "wire",
  "xerox",
  "yield",
  "zip",
  "align",
  "batch",
  "cache",
  "drain",
  "emit",
  "flush",
  "grep",
  "hash",
  "init",
  "jump",
  "kick",
  "load",
  "map",
  "nuke",
  "optimize",
  "patch",
  "queue",
  "rebase",
  "ship",
  "trace",
  "unpack",
  "verify",
  "watch",
  "xor",
  "yank",
];
const TOPICS = [
  "dashboard",
  "migration",
  "pipeline",
  "widget",
  "module",
  "service",
  "endpoint",
  "schema",
  "layout",
  "component",
  "database",
  "cluster",
  "gateway",
  "monitor",
  "router",
  "cache",
  "proxy",
  "worker",
  "scheduler",
  "registry",
  "sandbox",
  "template",
  "config",
  "plugin",
  "bridge",
  "tunnel",
  "beacon",
  "console",
  "kernel",
  "runtime",
  "compiler",
  "debugger",
  "profiler",
  "tracker",
  "scanner",
  "emitter",
  "handler",
  "adapter",
  "factory",
  "builder",
];
function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}
function randomProjectName() {
  return `${pick(ADJECTIVES)}-${pick(NOUNS)}-${pick(TOPICS)}`;
}
function randomTodoTitle() {
  return `${pick(VERBS)} the ${pick(ADJECTIVES)} ${pick(NOUNS)} ${pick(TOPICS)}`;
}
export function GenerateData() {
  // App.tsx opts into the sync in-process client (asyncSubscriptionsOnly:
  // false), so the db here is the full Db with transaction support, not the
  // async channel facade useDb is typed as.
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [status, setStatus] = useState("idle");
  const [projectsCreated, setProjectsCreated] = useState(0);
  const [todosCreated, setTodosCreated] = useState(0);
  const [error, setError] = useState(null);
  const generate = useCallback(async () => {
    if (!sessionUserId) return;
    setStatus("generating");
    setProjectsCreated(0);
    setTodosCreated(0);
    setError(null);
    try {
      // Generate projects in mergeable transactions
      const projectIds = [];
      let batchesSinceYield = 0;
      for (let i = 0; i < TOTAL_PROJECTS; i += BATCH_SIZE) {
        const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_PROJECTS);
        db.transaction((tx) => {
          for (let j = i; j < batchEnd; j++) {
            const row = tx.insert(app.projects, { name: randomProjectName() });
            projectIds.push(row.id);
          }
        });
        setProjectsCreated(batchEnd);
        batchesSinceYield++;
        if (batchesSinceYield >= YIELD_EVERY_BATCHES) {
          batchesSinceYield = 0;
          await new Promise((r) => setTimeout(r, 0));
        }
      }
      // Generate todos in mergeable transactions, round-robin across projects
      batchesSinceYield = 0;
      for (let i = 0; i < TOTAL_TODOS; i += BATCH_SIZE) {
        const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_TODOS);
        db.transaction((tx) => {
          for (let j = i; j < batchEnd; j++) {
            tx.insert(app.todos, {
              title: randomTodoTitle(),
              done: j % 5 === 0,
              owner_id: sessionUserId,
              projectId: projectIds[j % projectIds.length],
            });
          }
        });
        setTodosCreated(batchEnd);
        batchesSinceYield++;
        if (batchesSinceYield >= YIELD_EVERY_BATCHES) {
          batchesSinceYield = 0;
          await new Promise((r) => setTimeout(r, 0));
        }
      }
      setStatus("done");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setStatus("error");
    }
  }, [db, sessionUserId]);
  return _jsxs(_Fragment, {
    children: [
      _jsx("h1", { children: "Generate Stress Test Data" }),
      _jsxs("p", {
        children: [
          "This will create ",
          _jsx("strong", { children: TOTAL_PROJECTS.toLocaleString() }),
          " projects and",
          " ",
          _jsx("strong", { children: TOTAL_TODOS.toLocaleString() }),
          " todos linked to them.",
        ],
      }),
      _jsx("button", {
        onClick: generate,
        disabled: status === "generating" || !sessionUserId,
        children: status === "generating" ? "Generating..." : "Generate",
      }),
      status !== "idle" &&
        _jsxs("div", {
          style: { marginTop: "1rem" },
          children: [
            _jsxs("p", {
              children: [
                "Projects: ",
                projectsCreated.toLocaleString(),
                " / ",
                TOTAL_PROJECTS.toLocaleString(),
              ],
            }),
            _jsx("progress", {
              value: projectsCreated,
              max: TOTAL_PROJECTS,
              style: { width: "100%" },
            }),
            _jsxs("p", {
              children: [
                "Todos: ",
                todosCreated.toLocaleString(),
                " / ",
                TOTAL_TODOS.toLocaleString(),
              ],
            }),
            _jsx("progress", { value: todosCreated, max: TOTAL_TODOS, style: { width: "100%" } }),
          ],
        }),
      status === "done" && _jsx("p", { style: { color: "green" }, children: "Done!" }),
      status === "error" && _jsxs("p", { style: { color: "red" }, children: ["Error: ", error] }),
      _jsx("p", { children: _jsx("a", { href: "#list", children: "View Todos" }) }),
    ],
  });
}
