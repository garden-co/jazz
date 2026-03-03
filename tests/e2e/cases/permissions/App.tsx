import { createRoot } from "react-dom/client";
import * as React from "react";
import { createJazzClient, JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "./schema/app.js";

type ClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;

function readConfigFromSearch(): ClientConfig {
  const params = new URLSearchParams(window.location.search);
  const appId = params.get("appId") ?? "offline-e2e-app";
  const serverUrl = params.get("serverUrl") ?? "http://127.0.0.1:1625";
  const localAuthToken = params.get("token") ?? "offline-user";
  const adminSecret = params.get("adminSecret") ?? undefined;
  const dbName = params.get("dbName") ?? `Db-${localAuthToken}`;

  return {
    appId,
    serverUrl,
    adminSecret,
    env: "dev",
    userBranch: "main",
    localAuthMode: "demo",
    localAuthToken,
    dbName: dbName,
  };
}

function TodoView() {
  const db = useDb();
  const todos = useAll(app.todos);
  const session = useSession();
  const [title, setTitle] = React.useState("");
  const [ownerId, setOwnerId] = React.useState(session?.user_id);

  const onSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) {
      return;
    }

    db.insert(app.todos, {
      title: trimmed,
      done: false,
      owner_id: ownerId,
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
        <label>
          Owner
          <input
            aria-label="Owner"
            value={ownerId}
            onChange={(event) => setOwnerId(event.target.value)}
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
            <span>
              {todo.title} {todo.done ? "(done)" : ""}
            </span>
            <button onClick={() => db.deleteFrom(app.todos, todo.id)}>Delete todo</button>
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
      <h1>Permissions App</h1>
      <TodoView />
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
