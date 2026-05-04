import { useState, useCallback } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";

const TOTAL_PROJECTS = 10_000;
const TOTAL_TODOS = 50_000;
const BATCH_SIZE = 500;

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

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function randomProjectName(): string {
  return `${pick(ADJECTIVES)}-${pick(NOUNS)}-${pick(TOPICS)}`;
}

function randomTodoTitle(): string {
  return `${pick(VERBS)} the ${pick(ADJECTIVES)} ${pick(NOUNS)} ${pick(TOPICS)}`;
}

type Status = "idle" | "generating" | "done" | "error";

export function GenerateData() {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  const [status, setStatus] = useState<Status>("idle");
  const [projectsCreated, setProjectsCreated] = useState(0);
  const [todosCreated, setTodosCreated] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const generate = useCallback(async () => {
    if (!sessionUserId) return;
    setStatus("generating");
    setProjectsCreated(0);
    setTodosCreated(0);
    setError(null);

    try {
      // Generate projects in batches
      const projectIds: string[] = [];
      for (let i = 0; i < TOTAL_PROJECTS; i += BATCH_SIZE) {
        const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_PROJECTS);
        for (let j = i; j < batchEnd; j++) {
          const row = db.insert(app.projects, { name: randomProjectName() });
          projectIds.push(row.id);
        }
        setProjectsCreated(batchEnd);
        // Yield to the event loop so the UI can update
        await new Promise((r) => setTimeout(r, 0));
      }

      // Generate todos in batches, round-robin across projects
      for (let i = 0; i < TOTAL_TODOS; i += BATCH_SIZE) {
        const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_TODOS);
        for (let j = i; j < batchEnd; j++) {
          db.insert(app.todos, {
            title: randomTodoTitle(),
            done: j % 5 === 0,
            owner_id: sessionUserId,
            projectId: projectIds[j % projectIds.length],
          });
        }
        setTodosCreated(batchEnd);
        await new Promise((r) => setTimeout(r, 0));
      }

      setStatus("done");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setStatus("error");
    }
  }, [db, sessionUserId]);

  return (
    <>
      <h1>Generate Stress Test Data</h1>
      <p>
        This will create <strong>{TOTAL_PROJECTS.toLocaleString()}</strong> projects and{" "}
        <strong>{TOTAL_TODOS.toLocaleString()}</strong> todos linked to them.
      </p>

      <button onClick={generate} disabled={status === "generating" || !sessionUserId}>
        {status === "generating" ? "Generating..." : "Generate"}
      </button>

      {status !== "idle" && (
        <div style={{ marginTop: "1rem" }}>
          <p>
            Projects: {projectsCreated.toLocaleString()} / {TOTAL_PROJECTS.toLocaleString()}
          </p>
          <progress value={projectsCreated} max={TOTAL_PROJECTS} style={{ width: "100%" }} />

          <p>
            Todos: {todosCreated.toLocaleString()} / {TOTAL_TODOS.toLocaleString()}
          </p>
          <progress value={todosCreated} max={TOTAL_TODOS} style={{ width: "100%" }} />
        </div>
      )}

      {status === "done" && <p style={{ color: "green" }}>Done!</p>}
      {status === "error" && <p style={{ color: "red" }}>Error: {error}</p>}

      <p>
        <a href="#list">View Todos</a>
      </p>
    </>
  );
}
