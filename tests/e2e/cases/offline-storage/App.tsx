import { createRoot } from "react-dom/client";
import * as React from "react";
import { createJazzClient, JazzProvider, useAll, useDb } from "jazz-tools/react";
import { app } from "./schema/app.js";

type ClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;

function readConfigFromSearch(): ClientConfig {
  const params = new URLSearchParams(window.location.search);
  const appId = params.get("appId") ?? "offline-e2e-app";
  const serverUrl = params.get("serverUrl") ?? "http://127.0.0.1:1625";
  const localAuthToken = params.get("token") ?? "offline-user";
  const adminSecret = params.get("adminSecret") ?? undefined;

  return {
    appId,
    serverUrl,
    adminSecret,
    env: "dev",
    userBranch: "main",
    localAuthMode: "demo",
    localAuthToken,
  };
}

function TodoView() {
  const db = useDb();
  const todos = useAll(app.todos);
  const [title, setTitle] = React.useState("");

  const onSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) {
      return;
    }

    db.insert(app.todos, {
      title: trimmed,
      done: false,
    });
    setTitle("");
  };

  return (
    <>
      <form onSubmit={onSubmit}>
        <label>
          Todo title
          <input
            aria-label="Todo title"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
          />
        </label>
        <button type="submit">Add todo</button>
      </form>

      <ul id="todo-list">
        {todos?.map((todo) => (
          <li key={todo.id} className={todo.done ? "done" : ""}>
            <input
              className="toggle"
              type="checkbox"
              checked={todo.done}
              onChange={() => db.update(app.todos, todo.id, { done: !todo.done })}
            />
            <span>{todo.title}</span>
          </li>
        ))}
      </ul>
    </>
  );
}

const client = createJazzClient(readConfigFromSearch());

function App() {
  return (
    <JazzProvider client={client}>
      <h1>Offline App</h1>
      <TodoView />
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
