import init, { MiniJazzRuntime } from "./generated/mini-jazz-sqlite-wasm/mini_jazz_sqlite_wasm.js";

const PROJECT_ID = "todo-list";
const PAGE_SIZE = 10;
const GENERATED_LABELS = ["work", "home", "urgent", "later", "bug", "idea", "docs", "release"];
const SORT_COLUMNS = {
  date: "$createdAt",
  name: "title",
};
let db;
let labelsById = new Map();
let filters = {
  search: "",
  labelIds: [],
  sortField: "date",
  sortDir: "desc",
};

self.onmessage = async ({ data }) => {
  try {
    if (data.type === "init") {
      await init();
      db = await MiniJazzRuntime.openOpfs(data.dbName, data.nodeId, data.user);
      if (!db.readRows("projects").some((row) => row.id === PROJECT_ID)) {
        db.insertRow("projects", PROJECT_ID, { title: "Todo list" });
      }
      refreshLabelCache();
    } else if (data.type === "add") {
      const id = `todo-${crypto.randomUUID()}`;
      db.insertRow("todos", id, {
        title: data.title,
        done: false,
        project: PROJECT_ID,
      });
      addTodoLabels(id, data.labels);
    } else if (data.type === "setFilters") {
      filters = sanitizeFilters(data.filters);
    } else if (data.type === "generate") {
      const startedAt = performance.now();
      ensureLabels(GENERATED_LABELS);
      for (let i = 0; i < data.count; i++) {
        const todoId = `todo-${crypto.randomUUID()}`;
        const todoLabels = labelsForGeneratedTodo(i);
        db.insertRow("todos", todoId, {
          title: generatedTitle(i, todoLabels),
          done: false,
          project: PROJECT_ID,
        });
        addTodoLabels(todoId, todoLabels);
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
      deleteTodoLabels(data.id);
      db.deleteRow("todos", data.id);
    }
    postState();
  } catch (error) {
    postMessage({ type: "error", message: error.message ?? String(error) });
  }
};

function postState(generateMs) {
  const startedAt = performance.now();
  const todoIds = todoIdsForSelectedLabels(filters.labelIds);
  let rows = [];

  if (!todoIds || todoIds.length > 0) {
    rows = db.query(buildTodoQuery(todoIds));
  }
  const queryMs = performance.now() - startedAt;

  const todos = rows.map((row) => ({
    id: row.id,
    title: row.values.title,
    done: row.values.done,
    txId: row.tx_id,
  }));

  postMessage({
    type: "state",
    filters,
    labels: sortedLabels(),
    todos,
    queryMs,
    currentRows: db.storageStats().current_rows,
    generateMs,
  });
}

function buildTodoQuery(todoIds) {
  const conditions = [];
  const search = filters.search.trim();

  if (search) {
    conditions.push({ column: "title", op: "contains", value: search });
  }
  if (todoIds) {
    conditions.push({ column: "id", op: "in", value: todoIds });
  }

  return {
    table: "todos",
    conditions,
    includes: {},
    orderBy: [[SORT_COLUMNS[filters.sortField], filters.sortDir]],
    limit: PAGE_SIZE,
  };
}

function addTodoLabels(todoId, labelNames) {
  const labels = ensureLabels(labelNames);
  for (const label of labels) {
    db.insertRow("todo_labels", `${todoId}-${label.id}`, {
      todo: todoId,
      label: label.id,
    });
  }
}

function ensureLabels(labelNames) {
  const labels = [];
  const seen = new Set();
  for (const rawName of labelNames ?? []) {
    const name = normalizeLabelName(rawName);
    if (!name || seen.has(name)) continue;
    seen.add(name);
    const id = labelIdForName(name);
    if (!labelsById.has(id)) {
      db.insertRow("labels", id, { name });
      labelsById.set(id, { id, name });
    }
    labels.push(labelsById.get(id));
  }
  return labels;
}

function labelRow(row) {
  return [row.id, { id: row.id, name: row.values.name }];
}

function refreshLabelCache() {
  labelsById = new Map(db.readRows("labels").map(labelRow));
}

function sortedLabels() {
  return Array.from(labelsById.values()).sort((left, right) => left.name.localeCompare(right.name));
}

function deleteTodoLabels(todoId) {
  for (const row of db.query({
    table: "todo_labels",
    conditions: [{ column: "todo", op: "eq", value: todoId }],
    includes: {},
  })) {
    db.deleteRow("todo_labels", row.id);
  }
}

function todoIdsForSelectedLabels(labelIds) {
  if (!labelIds.length) return null;

  let intersection;
  for (const labelId of labelIds) {
    const ids = new Set(
      db
        .query({
          table: "todo_labels",
          conditions: [{ column: "label", op: "eq", value: labelId }],
          includes: {},
        })
        .map((row) => row.values.todo),
    );

    if (!intersection) {
      intersection = ids;
    } else {
      intersection = new Set([...intersection].filter((id) => ids.has(id)));
    }

    if (intersection.size === 0) return [];
  }

  return [...intersection];
}

function sanitizeFilters(nextFilters = {}) {
  const labelIds = Array.isArray(nextFilters.labelIds)
    ? nextFilters.labelIds.filter((id) => labelsById.has(id))
    : [];
  const sortField = nextFilters.sortField === "name" ? "name" : "date";
  const sortDir = nextFilters.sortDir === "asc" ? "asc" : "desc";
  return {
    search: String(nextFilters.search ?? "").slice(0, 80),
    labelIds,
    sortField,
    sortDir,
  };
}

function labelsForGeneratedTodo(index) {
  const labels = [GENERATED_LABELS[index % GENERATED_LABELS.length]];
  if (index % 3 === 0) labels.push(GENERATED_LABELS[(index + 3) % GENERATED_LABELS.length]);
  return labels;
}

function generatedTitle(index, labels) {
  const topic = index % 5 === 0 ? "ship" : index % 5 === 1 ? "review" : "note";
  return `Todo ${String(index + 1).padStart(6, "0")} ${topic} ${labels.join(" ")}`;
}

function normalizeLabelName(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replaceAll(/\s+/g, "-")
    .replaceAll(/[^a-z0-9_-]/g, "")
    .slice(0, 32);
}

function labelIdForName(name) {
  return `label-${name}`;
}
