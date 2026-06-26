import {
  runBrowserBatchDurabilitySmoke,
  runBrowserStorageConcurrencySmoke,
  runDbAllByteaOrderSmoke,
  runReloadPersistenceSmoke,
  runWebSocketBoundarySmoke,
  runWebSocketRustSmoke,
  runWorkerBackedTour,
  todoSchemaHex,
  type ReloadPersistenceSmokeMode,
  type ScenarioProgress,
} from "./scenario.js";
import type { TodoView } from "jazz-tools";
import "./styles.css";

const summary = requireElement("#summary");
const log = requireElement("#log");
const runtimeStatus = requireElement("#runtimeStatus");
const durabilityStatus = requireElement("#durabilityStatus");
const readStatus = requireElement("#readStatus");
const watchStatus = requireElement("#watchStatus");
const rowCount = requireElement("#rowCount");
const todosBody = requireElement("#todosBody");
const transitionsBody = requireElement("#transitionsBody");
const lines: string[] = [];
let durableReached = false;

declare global {
  interface Window {
    __jazzBrowserTodoSchemaHex?: string;
  }
}

window.__jazzBrowserTodoSchemaHex = todoSchemaHex();

void run().catch((error: unknown) => {
  summary.textContent = "Failed";
  summary.classList.add("failed");
  runtimeStatus.textContent = "Failed";
  writeLog(`error: ${formatError(error)}`);
});

async function run(): Promise<void> {
  writeLog("browser storage port: openBrowserDb(namespace)");
  const search = new URLSearchParams(window.location.search);
  const smoke = search.get("smoke");
  const result = smoke === "websocket-boundary"
    ? await runWebSocketBoundarySmoke(writeLog)
    : smoke === "websocket-rust"
      ? await runWebSocketRustSmoke(requireQueryParam(search, "ws"), writeLog)
    : smoke === "db-all-bytea-order"
      ? await runDbAllByteaOrderSmoke(writeLog)
    : smoke === "browser-concurrency"
      ? await runBrowserStorageConcurrencySmoke(search.get("ns") ?? "", writeLog)
    : smoke === "browser-batch-durability"
      ? await runBrowserBatchDurabilitySmoke(search.get("ns") ?? "", writeLog)
    : isReloadPersistenceSmokeMode(smoke)
      ? await runReloadPersistenceSmoke(smoke, search.get("ns") ?? "", writeLog)
      : await runWorkerBackedTour(writeLog, updateProgress);
  summary.textContent = result.message;
  summary.classList.add("ready");
}

function requireQueryParam(search: URLSearchParams, name: string): string {
  const value = search.get(name);
  if (!value) throw new Error(`missing required query parameter ${name}`);
  return value;
}

function isReloadPersistenceSmokeMode(value: string | null): value is ReloadPersistenceSmokeMode {
  return value === "reload-write" || value === "reload-verify";
}

function writeLog(line: string): void {
  lines.push(line);
  log.textContent = lines.join("\n");
}

function updateProgress(event: ScenarioProgress): void {
  switch (event.type) {
    case "worker-starting":
      runtimeStatus.textContent = "Worker starting";
      break;
    case "worker-ready":
      runtimeStatus.textContent = "Worker ready";
      break;
    case "db-opened":
      readStatus.textContent = `DB ${handleLabel(event.db)}`;
      break;
    case "query-prepared":
      readStatus.textContent = `Query ${handleLabel(event.query)}`;
      break;
    case "insert-permission":
      if (!durableReached) {
        durabilityStatus.textContent = event.allowed ? "Insert allowed" : "Insert blocked";
      }
      break;
    case "write-state":
      durabilityStatus.textContent = `${event.fate} / ${event.durability}`;
      break;
    case "write-durable":
      durableReached = true;
      durabilityStatus.textContent = event.durability;
      break;
    case "watch-opened":
      watchStatus.textContent = `${handleLabel(event.watch)} / ${event.current.length}`;
      readStatus.textContent = `${event.current.length} decoded`;
      renderTodos(event.current);
      break;
    case "todo-transition":
      watchStatus.textContent = `${event.label}: ${formatTodosForDom(event.todos)}`;
      readStatus.textContent = `${event.todos.length} decoded`;
      renderTodos(event.todos);
      appendTransition(event.label, event.todos);
      break;
    case "worker-shutdown":
      runtimeStatus.textContent = "Shutdown";
      break;
  }
}

function renderTodos(todos: TodoView[]): void {
  todosBody.replaceChildren(...todos.map((todo) => {
    const row = document.createElement("div");
    row.className = "todo-row";
    row.role = "row";
    row.dataset.title = todo.title;
    row.dataset.done = String(todo.done);

    row.append(
      cell(todo.title, "cell strong"),
      cell(todo.done ? "done" : "open", `cell badge ${todo.done ? "done" : "open"}`),
      cell(rowIdLabel(todo.rowId), "cell muted code"),
    );
    row.title = `encoded todo ${rowIdLabel(todo.rowId)} decoded on the main thread`;
    return row;
  }));
  rowCount.textContent = `${todos.length} ${todos.length === 1 ? "todo" : "todos"}`;
}

function appendTransition(label: string, todos: TodoView[]): void {
  const row = document.createElement("div");
  row.className = "transition-row";
  row.dataset.state = label;
  row.textContent = `${label}: ${formatTodosForDom(todos)}`;
  transitionsBody.append(row);
}

function cell(text: string, className: string): HTMLSpanElement {
  const element = document.createElement("span");
  element.className = className;
  element.role = "cell";
  element.textContent = text;
  return element;
}

function handleLabel(handle: { kind: string; id: number }): string {
  return `${handle.kind}#${handle.id}`;
}

function rowIdLabel(rowId: Uint8Array): string {
  return [...rowId].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function formatTodosForDom(todos: TodoView[]): string {
  return todos.map((todo) => `${todo.title}:${todo.done ? "done" : "open"}`).join(", ") || "none";
}

function requireElement(selector: string): HTMLElement {
  const element = document.querySelector<HTMLElement>(selector);
  if (!element) {
    throw new Error(`missing element ${selector}`);
  }
  return element;
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  return String(error);
}
