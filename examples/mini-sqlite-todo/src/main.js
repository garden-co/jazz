const app = document.getElementById("app");
const worker = new Worker(new URL("./db-worker.js", import.meta.url), { type: "module" });
const state = {
  ready: false,
  generating: false,
  todos: [],
  error: "",
  generated: 0,
  totalToGenerate: 100000,
  generateMs: 0,
  queryMs: 0,
  currentRows: 0,
};

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
      <button type="submit">Add</button>
    </form>
    <button id="generate" class="generate" type="button">Generate 100k todos</button>
    <p id="error-message" class="error-message" role="alert" hidden></p>
    <ul id="todo-list" class="todo-list"></ul>
    <p id="empty-state" class="empty-state">No todos yet.</p>
    <p id="summary" class="summary"></p>
  </section>
`;

const form = app.querySelector("#todo-form");
const input = app.querySelector("#todo-title");
const list = app.querySelector("#todo-list");
const generate = app.querySelector("#generate");

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const title = input.value.trim();
  if (!title || !state.ready) return;
  worker.postMessage({ type: "add", title });
  input.value = "";
});

generate.addEventListener("click", () => {
  if (!state.ready || state.generating) return;
  state.generating = true;
  state.generated = 0;
  state.generateMs = 0;
  worker.postMessage({ type: "generate", count: state.totalToGenerate });
  render();
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
  dbName: "mini-jazz-sqlite-lean-todos.sqlite3",
  nodeId: "browser-worker",
  user: "alice",
});
render();

function render() {
  app.querySelector("#status").textContent = state.ready ? statusText() : "Opening OPFS worker...";
  input.disabled = !state.ready || state.generating;
  form.querySelector("button").disabled = !state.ready || state.generating;
  generate.disabled = !state.ready || state.generating;

  const error = app.querySelector("#error-message");
  error.hidden = !state.error;
  error.textContent = state.error;

  list.innerHTML = state.todos.map(todoHtml).join("");
  app.querySelector("#empty-state").hidden = state.todos.length > 0;

  const done = state.todos.filter((todo) => todo.done).length;
  app.querySelector("#summary").textContent =
    `${state.todos.length - done} open / ${done} done shown. ` +
    `${state.currentRows.toLocaleString()} current rows. ` +
    `Top-10 query: ${state.queryMs.toFixed(2)} ms. ` +
    (state.generateMs ? `Generate: ${(state.generateMs / 1000).toFixed(2)} s.` : "");
}

function statusText() {
  if (!state.generating) return "OPFS worker ready";
  return `Generating ${state.generated.toLocaleString()} / ${state.totalToGenerate.toLocaleString()} todos...`;
}

function todoHtml(todo) {
  return `
    <li class="todo-item ${todo.done ? "done" : ""}">
      <label class="todo-label">
        <input type="checkbox" data-role="toggle" data-id="${todo.id}" ${todo.done ? "checked" : ""}>
        <span>${escapeHtml(todo.title)}</span>
      </label>
      <button type="button" data-role="delete" data-id="${todo.id}">Delete</button>
    </li>
  `;
}

function escapeHtml(value) {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
