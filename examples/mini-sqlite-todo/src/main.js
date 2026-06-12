const app = document.getElementById("app");
const worker = new Worker(new URL("./db-worker.js", import.meta.url), { type: "module" });
const state = {
  ready: false,
  generating: false,
  todos: [],
  labels: [],
  filters: {
    search: "",
    labelIds: [],
    sortField: "date",
    sortDir: "desc",
  },
  error: "",
  generated: 0,
  totalToGenerate: 100000,
  generateMs: 0,
  queryMs: 0,
  currentRows: 0,
};
let searchTimer = 0;

app.innerHTML = `
  <section class="todo-app">
    <header class="app-header">
      <div>
        <p class="eyebrow">mini-jazz-sqlite WASM</p>
        <h1>Todos</h1>
      </div>
      <p id="status" class="status" role="status"></p>
    </header>
    <form id="todo-form" class="todo-form">
      <input id="todo-title" name="title" type="text" autocomplete="off" placeholder="Add a task" required />
      <input id="todo-labels" name="labels" type="text" autocomplete="off" placeholder="labels: work, urgent" />
      <button type="submit">Add</button>
    </form>
    <div class="controls">
      <input id="search" type="search" autocomplete="off" placeholder="Search titles" />
      <label>
        <span>Sort</span>
        <select id="sort-field">
          <option value="date">Date</option>
          <option value="name">Name</option>
        </select>
      </label>
      <label>
        <span>Order</span>
        <select id="sort-dir">
          <option value="desc">Desc</option>
          <option value="asc">Asc</option>
        </select>
      </label>
    </div>
    <div id="label-filters" class="label-filters" aria-label="Label filters"></div>
    <button id="generate" class="generate" type="button">Generate 100k todos</button>
    <p id="error-message" class="error-message" role="alert" hidden></p>
    <ul id="todo-list" class="todo-list"></ul>
    <p id="empty-state" class="empty-state">No matching todos.</p>
    <p id="summary" class="summary"></p>
  </section>
`;

const form = app.querySelector("#todo-form");
const titleInput = app.querySelector("#todo-title");
const labelsInput = app.querySelector("#todo-labels");
const searchInput = app.querySelector("#search");
const sortField = app.querySelector("#sort-field");
const sortDir = app.querySelector("#sort-dir");
const labelFilters = app.querySelector("#label-filters");
const list = app.querySelector("#todo-list");
const generate = app.querySelector("#generate");

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const title = titleInput.value.trim();
  if (!title || !state.ready) return;
  worker.postMessage({ type: "add", title, labels: parseLabels(labelsInput.value) });
  titleInput.value = "";
  labelsInput.value = "";
});

generate.addEventListener("click", () => {
  if (!state.ready || state.generating) return;
  state.generating = true;
  state.generated = 0;
  state.generateMs = 0;
  worker.postMessage({ type: "generate", count: state.totalToGenerate });
  render();
});

searchInput.addEventListener("input", () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    setFilters({ search: searchInput.value });
  }, 120);
});

sortField.addEventListener("change", () => {
  setFilters({ sortField: sortField.value });
});

sortDir.addEventListener("change", () => {
  setFilters({ sortDir: sortDir.value });
});

labelFilters.addEventListener("click", (event) => {
  const target = event.target;
  if (target.dataset.role !== "label-filter") return;
  const labelIds = new Set(state.filters.labelIds);
  if (labelIds.has(target.dataset.id)) {
    labelIds.delete(target.dataset.id);
  } else {
    labelIds.add(target.dataset.id);
  }
  setFilters({ labelIds: [...labelIds] });
});

list.addEventListener("change", (event) => {
  const target = event.target;
  if (target.dataset.role !== "toggle") return;
  worker.postMessage({ type: "toggle", id: target.dataset.id, done: target.checked });
});

list.addEventListener("click", (event) => {
  const target = event.target;
  if (target.dataset.role !== "delete") return;
  worker.postMessage({ type: "delete", id: target.dataset.id });
});

worker.onmessage = ({ data }) => {
  if (data.type === "state") {
    state.ready = true;
    state.todos = data.todos;
    state.labels = data.labels;
    state.filters = data.filters;
    state.queryMs = data.queryMs;
    state.currentRows = data.currentRows;
    state.generateMs = data.generateMs ?? state.generateMs;
    state.generating = false;
    state.error = "";
  } else if (data.type === "progress") {
    state.generating = true;
    state.generated = data.generated;
    state.totalToGenerate = data.total;
  } else {
    state.generating = false;
    state.error = data.message;
  }
  render();
};

addEventListener("pagehide", () => worker.terminate());

worker.postMessage({
  type: "init",
  dbName: "mini-jazz-sqlite-labeled-todos.sqlite3",
  nodeId: "browser-worker",
  user: "alice",
});
render();

function setFilters(patch) {
  if (!state.ready || state.generating) return;
  state.filters = { ...state.filters, ...patch };
  worker.postMessage({ type: "setFilters", filters: state.filters });
  render();
}

function render() {
  app.querySelector("#status").textContent = state.ready ? statusText() : "Opening OPFS worker...";

  for (const control of [titleInput, labelsInput, searchInput, sortField, sortDir]) {
    control.disabled = !state.ready || state.generating;
  }
  form.querySelector("button").disabled = !state.ready || state.generating;
  generate.disabled = !state.ready || state.generating;
  searchInput.value = state.filters.search;
  sortField.value = state.filters.sortField;
  sortDir.value = state.filters.sortDir;

  const error = app.querySelector("#error-message");
  error.hidden = !state.error;
  error.textContent = state.error;

  labelFilters.innerHTML = state.labels.map(labelFilterHtml).join("");
  list.innerHTML = state.todos.map(todoHtml).join("");
  app.querySelector("#empty-state").hidden = state.todos.length > 0;

  const done = state.todos.filter((todo) => todo.done).length;
  const filters = filterSummary();
  app.querySelector("#summary").textContent =
    `${state.todos.length - done} open / ${done} done shown. ` +
    `${state.currentRows.toLocaleString()} current rows. ` +
    `Top-10 query: ${state.queryMs.toFixed(2)} ms. ` +
    `${filters}` +
    (state.generateMs ? ` Generate: ${(state.generateMs / 1000).toFixed(2)} s.` : "");
}

function statusText() {
  if (!state.generating) return "OPFS worker ready";
  return `Generating ${state.generated.toLocaleString()} / ${state.totalToGenerate.toLocaleString()} todos...`;
}

function filterSummary() {
  const parts = [];
  if (state.filters.search) parts.push(`search "${state.filters.search}"`);
  if (state.filters.labelIds.length) parts.push(`${state.filters.labelIds.length} label filter`);
  const sortName = state.filters.sortField === "name" ? "name" : "date";
  parts.push(`${sortName} ${state.filters.sortDir}`);
  return `Filters: ${parts.join(", ")}.`;
}

function labelFilterHtml(label) {
  const selected = state.filters.labelIds.includes(label.id);
  return `
    <button type="button" class="chip ${selected ? "selected" : ""}" data-role="label-filter" data-id="${escapeAttr(label.id)}">
      ${escapeHtml(label.name)}
    </button>
  `;
}

function todoHtml(todo) {
  return `
    <li class="todo-item ${todo.done ? "done" : ""}">
      <label class="todo-label">
        <input type="checkbox" data-role="toggle" data-id="${escapeAttr(todo.id)}" ${todo.done ? "checked" : ""}>
        <span>${escapeHtml(todo.title)}</span>
      </label>
      <button type="button" data-role="delete" data-id="${escapeAttr(todo.id)}">Delete</button>
    </li>
  `;
}

function parseLabels(value) {
  return value
    .split(",")
    .map((label) => label.trim())
    .filter(Boolean);
}

function escapeHtml(value) {
  return String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll('"', "&quot;");
}
