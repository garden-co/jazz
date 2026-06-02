const app = document.getElementById("app");
const worker = new Worker(new URL("./db-worker.js", import.meta.url), { type: "module" });
const state = {
  ready: false,
  generating: false,
  users: [],
  groups: [],
  projects: [],
  activeUserId: "user-alice",
  selectedProjectId: null,
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
  showTableStats: false,
  tableStats: null,
};
let searchTimer = 0;
let editingTodoId = null;

app.innerHTML = `
  <section class="todo-app">
    <header class="app-header">
      <div>
        <p class="eyebrow">mini-jazz-sqlite WASM</p>
        <h1>Todos</h1>
      </div>
      <p id="status" class="status" role="status"></p>
    </header>
    <div class="scope-controls">
      <label class="field">
        <span>User</span>
        <select id="user-select"></select>
      </label>
      <label class="field">
        <span>Project</span>
        <select id="project-select"></select>
      </label>
      <div id="group-list" class="group-list" aria-label="Groups"></div>
    </div>
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
      <label class="table-stats-toggle">
        <input id="table-stats" type="checkbox" />
        <span>Table stats</span>
      </label>
    </div>
    <div id="label-filters" class="label-filters" aria-label="Label filters"></div>
    <button id="generate" class="generate" type="button">Generate 100k todos</button>
    <p id="error-message" class="error-message" role="alert" hidden></p>
    <ul id="todo-list" class="todo-list"></ul>
    <p id="empty-state" class="empty-state">No visible open todos.</p>
    <p id="summary" class="summary"></p>
  </section>
`;

const form = app.querySelector("#todo-form");
const titleInput = app.querySelector("#todo-title");
const labelsInput = app.querySelector("#todo-labels");
const searchInput = app.querySelector("#search");
const sortField = app.querySelector("#sort-field");
const sortDir = app.querySelector("#sort-dir");
const tableStats = app.querySelector("#table-stats");
const labelFilters = app.querySelector("#label-filters");
const list = app.querySelector("#todo-list");
const generate = app.querySelector("#generate");
const userSelect = app.querySelector("#user-select");
const projectSelect = app.querySelector("#project-select");

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const title = titleInput.value.trim();
  if (!title || !state.ready || !state.selectedProjectId) return;
  worker.postMessage({
    type: "add",
    title,
    labels: parseLabels(labelsInput.value),
    projectId: state.selectedProjectId,
  });
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

tableStats.addEventListener("change", () => {
  state.showTableStats = tableStats.checked;
  state.tableStats = null;
  worker.postMessage({ type: "setTableStats", enabled: state.showTableStats });
  render();
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

userSelect.addEventListener("change", () => {
  if (!state.ready || state.generating) return;
  worker.postMessage({ type: "setUser", user: userSelect.value });
});

projectSelect.addEventListener("change", () => {
  if (!state.ready || state.generating) return;
  worker.postMessage({ type: "setProject", projectId: projectSelect.value });
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

list.addEventListener("dblclick", (event) => {
  const target = event.target.closest?.("[data-role='title']");
  if (!target || !state.ready || state.generating) return;
  const todo = state.todos.find((candidate) => candidate.id === target.dataset.id);
  if (!todo?.canRename) return;
  editingTodoId = todo.id;
  render();
});

list.addEventListener("keydown", (event) => {
  const target = event.target;
  if (target.dataset.role !== "title-editor") return;
  if (event.key === "Escape") {
    editingTodoId = null;
    render();
    return;
  }
  if (event.key !== "Enter") return;
  event.preventDefault();
  const title = target.value.trim();
  editingTodoId = null;
  if (title) {
    worker.postMessage({ type: "rename", id: target.dataset.id, title });
  }
  render();
});

worker.onmessage = ({ data }) => {
  if (data.type === "state") {
    state.ready = true;
    state.users = data.users;
    state.groups = data.groups;
    state.projects = data.projects;
    state.activeUserId = data.activeUserId;
    state.selectedProjectId = data.selectedProjectId;
    state.todos = data.todos;
    state.labels = data.labels;
    state.filters = data.filters;
    state.showTableStats = data.showTableStats;
    state.tableStats = data.tableStats;
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
  dbName: "mini-jazz-sqlite-nested-group-label-todos.sqlite3",
  nodeId: "browser-worker",
  user: "user-alice",
});
render();

function setFilters(patch) {
  if (!state.ready || state.generating) return;
  state.filters = { ...state.filters, ...patch };
  worker.postMessage({ type: "setFilters", filters: state.filters });
  render();
}

function render() {
  app.querySelector("#status").textContent =
    state.generating || state.ready ? statusText() : "Opening OPFS worker...";

  for (const control of [titleInput, labelsInput, searchInput, sortField, sortDir, tableStats]) {
    control.disabled =
      !state.ready || state.generating || (control === titleInput && !state.selectedProjectId);
  }
  labelsInput.disabled = !state.ready || state.generating || !state.selectedProjectId;
  form.querySelector("button").disabled =
    !state.ready || state.generating || !state.selectedProjectId;
  generate.disabled = !state.ready || state.generating;
  userSelect.disabled = !state.ready || state.generating;
  projectSelect.disabled = !state.ready || state.generating || state.projects.length === 0;

  userSelect.innerHTML = state.users.map(userOptionHtml).join("");
  userSelect.value = state.activeUserId;

  projectSelect.innerHTML = state.projects.map(projectOptionHtml).join("");
  projectSelect.value = state.selectedProjectId ?? "";

  app.querySelector("#group-list").innerHTML = groupListHtml();
  searchInput.value = state.filters.search;
  sortField.value = state.filters.sortField;
  sortDir.value = state.filters.sortDir;
  tableStats.checked = state.showTableStats;

  const error = app.querySelector("#error-message");
  error.hidden = !state.error;
  error.textContent = state.error;

  labelFilters.innerHTML = state.labels.map(labelFilterHtml).join("");
  if (editingTodoId && !state.todos.some((todo) => todo.id === editingTodoId && todo.canRename)) {
    editingTodoId = null;
  }
  list.innerHTML = state.todos.map(todoHtml).join("");
  const editor = list.querySelector("[data-role='title-editor']");
  if (editor) {
    editor.focus();
    editor.select();
  }
  app.querySelector("#empty-state").hidden = state.todos.length > 0;

  app.querySelector("#summary").textContent = [
    `${state.todos.length} open todos shown.`,
    `${state.projects.length.toLocaleString()} visible projects.`,
    filterSummary(),
    tableStatsSummary(),
  ]
    .filter(Boolean)
    .join(" ");
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

function tableStatsSummary() {
  if (!state.showTableStats) return "";
  if (!state.tableStats) return "Table stats: loading...";
  const parts = [
    `${state.tableStats.currentRows.toLocaleString()} current rows`,
    `access ${state.tableStats.visibilityMs.toFixed(2)} ms`,
    `top-10 ${state.tableStats.queryMs.toFixed(2)} ms`,
  ];
  if (state.generateMs) {
    parts.push(`generate ${(state.generateMs / 1000).toFixed(2)} s`);
  }
  return `Table stats: ${parts.join(", ")}.`;
}

function userOptionHtml(user) {
  return `<option value="${escapeAttr(user.id)}">${escapeHtml(user.name)}</option>`;
}

function projectOptionHtml(project) {
  return `<option value="${escapeAttr(project.id)}">${escapeHtml(project.title)}</option>`;
}

function groupListHtml() {
  if (!state.groups.length) return `<span class="group-chip muted">No groups</span>`;
  return state.groups
    .map((group) => `<span class="group-chip">${escapeHtml(group.name)}</span>`)
    .join("");
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
  const labelHtml = todo.labels.length
    ? `<span class="todo-tags">${todo.labels.map((label) => `<span>${escapeHtml(label.name)}</span>`).join("")}</span>`
    : "";
  const titleHtml =
    editingTodoId === todo.id && todo.canRename
      ? `<input class="todo-title-editor" data-role="title-editor" data-id="${escapeAttr(todo.id)}" type="text" autocomplete="off" value="${escapeAttr(todo.title)}">`
      : `<strong class="todo-title" data-role="title" data-id="${escapeAttr(todo.id)}">${escapeHtml(todo.title)}</strong>`;
  return `
    <li class="todo-item ${todo.done ? "done" : ""}">
      <div class="todo-label">
        <input type="checkbox" data-role="toggle" data-id="${escapeAttr(todo.id)}" aria-label="${escapeAttr(todo.title)}" ${todo.done ? "checked" : ""}>
        <span>
          ${titleHtml}
          <small>${escapeHtml(todo.projectTitle)} - by ${escapeHtml(todo.createdByName)}</small>
          ${labelHtml}
        </span>
      </div>
      <button type="button" data-role="delete" data-id="${escapeAttr(todo.id)}" ${todo.canDelete ? "" : "disabled"}>Delete</button>
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
