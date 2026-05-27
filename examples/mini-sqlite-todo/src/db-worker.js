import init, { MiniJazzRuntime } from "./generated/mini-jazz-sqlite-wasm/mini_jazz_sqlite_wasm.js";

const PROJECT_ID = "todo-list";
const PAGE_SIZE = 10;
let db;

self.onmessage = async ({ data }) => {
  try {
    if (data.type === "init") {
      await init();
      db = await MiniJazzRuntime.openOpfs(data.dbName, data.nodeId, data.user);
      if (!db.readRows("projects").some((row) => row.id === PROJECT_ID)) {
        db.insertRow("projects", PROJECT_ID, { title: "Todo list" });
      }
    } else if (data.type === "add") {
      db.insertRow("todos", `todo-${crypto.randomUUID()}`, {
        title: data.title,
        done: false,
        project: PROJECT_ID,
      });
    } else if (data.type === "generate") {
      const startedAt = performance.now();
      for (let i = 0; i < data.count; i++) {
        db.insertRow("todos", `todo-${crypto.randomUUID()}`, {
          title: `Todo ${i + 1}`,
          done: false,
          project: PROJECT_ID,
        });
        if ((i + 1) % 1000 === 0) {
          postMessage({ type: "progress", generated: i + 1, total: data.count });
          await new Promise((resolve) => setTimeout(resolve));
        }
      }
      postState(performance.now() - startedAt);
      return;
    } else if (data.type === "toggle") {
      db.updateRow("todos", data.id, { done: data.done });
    } else if (data.type === "delete") {
      db.deleteRow("todos", data.id);
    }
    postState();
  } catch (error) {
    postMessage({ type: "error", message: error.message ?? String(error) });
  }
};

function postState(generateMs) {
  const startedAt = performance.now();
  const todos = db
    .readRowsWhereEqTopCreatedAtDesc("todos", "done", false, PAGE_SIZE)
    .map((row) => ({
      id: row.id,
      title: row.values.title,
      done: row.values.done,
      txId: row.tx_id,
    }));
  postMessage({
    type: "state",
    todos,
    queryMs: performance.now() - startedAt,
    currentRows: db.storageStats().current_rows,
    generateMs,
  });
}
